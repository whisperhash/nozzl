//! `RelayUrl`: validated newtype around a `wss://` or `ws://` URL.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RelayUrl(url::Url);

impl RelayUrl {
    pub fn parse(s: &str) -> Result<Self, RelayUrlError> {
        let parsed = url::Url::parse(s)?;
        match parsed.scheme() {
            "ws" | "wss" => {}
            other => return Err(RelayUrlError::BadScheme(other.to_string())),
        }
        // Reject embedded userinfo (`wss://user:pass@host`). We never do relay
        // auth via the URL (NIP-42 is challenge-response), and the URL is logged
        // verbatim and used as a Prometheus `relay` label — both surfaces a
        // read-only observer can read — so credentials must never live in it.
        // See "Logs are an observability surface" in CLAUDE.md.
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(RelayUrlError::HasUserinfo);
        }
        // rust-nostr's RelayUrl, which the relay pool takes, also refuses a
        // second "://" anywhere in the URL, and the acquirer converts every
        // RelayUrl into one with `expect`. Refusing it here keeps that
        // conversion total, so such a URL is a config error, never a panic.
        if parsed.as_str().matches("://").count() > 1 {
            return Err(RelayUrlError::MultipleSchemeSeparators);
        }
        Ok(Self(parsed))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_url(&self) -> &url::Url {
        &self.0
    }
}

impl fmt::Display for RelayUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl FromStr for RelayUrl {
    type Err = RelayUrlError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for RelayUrl {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(self.0.as_str())
    }
}

impl<'de> Deserialize<'de> for RelayUrl {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RelayUrlError {
    #[error("invalid URL: {0}")]
    Invalid(#[from] url::ParseError),
    #[error("scheme must be ws or wss, got {0}")]
    BadScheme(String),
    #[error("relay URL must not contain embedded credentials (user:pass@)")]
    HasUserinfo,
    #[error("relay URL must not contain \"://\" more than once")]
    MultipleSchemeSeparators,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_wss() {
        let url = RelayUrl::parse("wss://relay.damus.io").unwrap();
        assert_eq!(url.as_str(), "wss://relay.damus.io/");
    }

    #[test]
    fn parse_ws() {
        let url = RelayUrl::parse("ws://localhost:7000").unwrap();
        assert_eq!(url.as_str(), "ws://localhost:7000/");
    }

    #[test]
    fn rejects_https() {
        let err = RelayUrl::parse("https://example.com").unwrap_err();
        assert!(matches!(err, RelayUrlError::BadScheme(s) if s == "https"));
    }

    #[test]
    fn rejects_empty() {
        assert!(RelayUrl::parse("").is_err());
    }

    #[test]
    fn rejects_embedded_credentials() {
        // Credentials in the URL would leak into logs + the `relay` metric label.
        assert!(matches!(
            RelayUrl::parse("wss://user:pass@relay.example").unwrap_err(),
            RelayUrlError::HasUserinfo
        ));
        assert!(matches!(
            RelayUrl::parse("wss://user@relay.example").unwrap_err(),
            RelayUrlError::HasUserinfo
        ));
    }

    #[test]
    fn rejects_a_second_scheme_separator() {
        // The relay pool's own URL type refuses these, and the acquirer
        // converts every RelayUrl into it.
        for s in [
            "wss://relay.example/?via=wss://other.example",
            "wss://relay.example/#ws://x",
        ] {
            assert!(
                matches!(
                    RelayUrl::parse(s).unwrap_err(),
                    RelayUrlError::MultipleSchemeSeparators
                ),
                "{s}"
            );
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(RelayUrl::parse("not a url").is_err());
    }

    #[test]
    fn json_roundtrip() {
        let url = RelayUrl::parse("wss://relay.damus.io").unwrap();
        let json = serde_json::to_string(&url).unwrap();
        let back: RelayUrl = serde_json::from_str(&json).unwrap();
        assert_eq!(url, back);
    }

    #[test]
    fn json_rejects_https() {
        let err = serde_json::from_str::<RelayUrl>("\"https://example.com\"").unwrap_err();
        assert!(err.to_string().contains("scheme"));
    }

    #[test]
    fn toml_roundtrip() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Holder {
            url: RelayUrl,
        }
        let h = Holder {
            url: RelayUrl::parse("wss://nos.lol").unwrap(),
        };
        let s = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&s).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn from_str_works() {
        let url: RelayUrl = "wss://relay.primal.net".parse().unwrap();
        assert_eq!(url.as_str(), "wss://relay.primal.net/");
    }

    #[test]
    fn hash_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RelayUrl::parse("wss://a").unwrap());
        set.insert(RelayUrl::parse("wss://a/").unwrap());
        // url::Url normalizes trailing slash, so these collapse.
        assert_eq!(set.len(), 1);
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrips_through_json(host in "[a-z]{1,20}\\.(com|io|lol|dev)") {
            let url = format!("wss://{host}");
            let parsed = RelayUrl::parse(&url).unwrap();
            let json = serde_json::to_string(&parsed).unwrap();
            let back: RelayUrl = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(parsed, back);
        }
    }
}
