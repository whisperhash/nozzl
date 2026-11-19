//! NIP-11 relay information document fetcher.
//!
//! Issues a single `GET /` with `Accept: application/nostr+json` against the
//! relay's HTTP endpoint (the `wss://`/`ws://` URL with the scheme swapped).
//! Parses just the fields we care about — `name`, `software`, `version`, and
//! `limitations.{max_filters, max_subscriptions}`.
//!
//! KNOWN WRONG, #49 (NIP-11 (relay information) is never read): NIP-11 names
//! the object `limitation`, singular, in every revision since 2023-04-08, and
//! dropped `max_filters` on 2025-04-13, so a real document never matches the
//! plural key below. Nothing reads the result either, and wss:// relays are
//! never fetched (below), so the bug has had no effect. Fix it there, with the
//! fetch over TLS and the use of `max_limit`, not here alone.
//!
//! Implemented over `tokio::net::TcpStream` to keep the dependency surface
//! small (no `reqwest`/`hyper` for one cold-path GET on each connect).
//! TLS is **not** supported in v1 — `wss://` relays will simply have their
//! NIP-11 fetch fail and the acquirer will fall back to the no-NIP-11
//! defaults. This is consistent with the test relay (which is `ws://` only)
//! and acceptable because NIP-11 is advisory.

use std::time::Duration;

use nozzl_types::RelayUrl;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Soft connect/read timeout for the NIP-11 fetch. Failures are non-fatal —
/// we just log and continue without a parsed doc.
const NIP11_TIMEOUT: Duration = Duration::from_secs(5);

/// Parsed subset of a NIP-11 relay info document.
#[allow(dead_code, reason = "version is captured for log surfaces but not yet read")]
#[derive(Clone, Debug, Default)]
pub struct Nip11Info {
    pub name: Option<String>,
    pub software: Option<String>,
    pub version: Option<String>,
    pub limitations: Nip11Limitations,
}

/// `limitations` sub-object from NIP-11.
#[derive(Clone, Copy, Debug, Default)]
pub struct Nip11Limitations {
    pub max_filters: Option<u32>,
    pub max_subscriptions: Option<u32>,
}

#[derive(Deserialize)]
struct RawDoc {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    software: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    limitations: Option<RawLimitations>,
}

#[derive(Deserialize)]
struct RawLimitations {
    #[serde(default)]
    max_filters: Option<u32>,
    #[serde(default)]
    max_subscriptions: Option<u32>,
}

/// Fetch the NIP-11 doc for `relay`. Returns `None` for any failure
/// (transport, TLS-not-supported, parse error, non-2xx status, etc.) — the
/// caller is expected to fall back to "assume the relay is permissive".
pub async fn fetch(relay: &RelayUrl) -> Option<Nip11Info> {
    let url = relay.as_url();
    // v1: skip wss; we'd need rustls/native-tls and the cost isn't justified
    // for an advisory health hint. Fall back silently.
    if url.scheme() == "wss" {
        tracing::debug!(relay = %relay, "skipping NIP-11 fetch on wss (no TLS in v1)");
        return None;
    }
    let host = url.host_str()?;
    let port = url.port().unwrap_or(80);
    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };

    let bytes = match tokio::time::timeout(NIP11_TIMEOUT, fetch_raw(host, port, path)).await {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            tracing::debug!(relay = %relay, error = %e, "NIP-11 fetch failed");
            return None;
        }
        Err(_) => {
            tracing::debug!(relay = %relay, "NIP-11 fetch timed out");
            return None;
        }
    };

    let body = split_body(&bytes)?;
    let raw: RawDoc = match serde_json::from_slice(body) {
        Ok(d) => d,
        Err(e) => {
            tracing::debug!(relay = %relay, error = %e, "NIP-11 parse failed");
            return None;
        }
    };

    let info = Nip11Info {
        name: raw.name,
        software: raw.software,
        version: raw.version,
        limitations: raw
            .limitations
            .map(|l| Nip11Limitations {
                max_filters: l.max_filters,
                max_subscriptions: l.max_subscriptions,
            })
            .unwrap_or_default(),
    };
    tracing::info!(
        relay = %relay,
        name = info.name.as_deref().unwrap_or(""),
        software = info.software.as_deref().unwrap_or(""),
        max_filters = info.limitations.max_filters.unwrap_or(0),
        max_subscriptions = info.limitations.max_subscriptions.unwrap_or(0),
        "NIP-11 doc fetched",
    );
    Some(info)
}

async fn fetch_raw(host: &str, port: u16, path: &str) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect((host, port)).await?;
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Accept: application/nostr+json\r\n\
         Connection: close\r\n\
         User-Agent: {ua}\r\n\
         \r\n",
        ua = crate::transport::USER_AGENT,
    );
    stream.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(1024);
    stream.read_to_end(&mut buf).await?;
    Ok(buf)
}

/// Split an HTTP/1.1 response into header/body. Returns `None` for malformed
/// responses or non-2xx status codes.
fn split_body(buf: &[u8]) -> Option<&[u8]> {
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let header = std::str::from_utf8(&buf[..split]).ok()?;
    let mut lines = header.lines();
    let status_line = lines.next()?;
    let status: u16 = status_line.split_whitespace().nth(1)?.parse().ok()?;
    if !(200..300).contains(&status) {
        return None;
    }
    Some(&buf[split + 4..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_response(status: u16, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: application/nostr+json\r\n\r\n{body}",
        )
        .into_bytes()
    }

    #[test]
    fn split_body_extracts_after_double_crlf() {
        let resp = make_response(200, r#"{"name":"x"}"#);
        let body = split_body(&resp).expect("body");
        assert_eq!(body, br#"{"name":"x"}"#);
    }

    #[test]
    fn split_body_rejects_non_2xx() {
        let resp = make_response(404, "");
        assert!(split_body(&resp).is_none());
    }

    #[test]
    fn split_body_rejects_malformed() {
        let resp = b"NOT AN HTTP RESPONSE";
        assert!(split_body(resp).is_none());
    }

    #[test]
    fn parse_limitations() {
        let doc = r#"{
            "name": "test",
            "software": "nostr-rs-relay",
            "version": "0.9",
            "limitations": {
                "max_filters": 5,
                "max_subscriptions": 20
            }
        }"#;
        let raw: RawDoc = serde_json::from_str(doc).unwrap();
        assert_eq!(raw.name.as_deref(), Some("test"));
        assert_eq!(raw.limitations.unwrap().max_filters, Some(5));
    }

    #[test]
    fn parse_doc_without_limitations() {
        let doc = r#"{"name":"x"}"#;
        let raw: RawDoc = serde_json::from_str(doc).unwrap();
        assert_eq!(raw.name.as_deref(), Some("x"));
        assert!(raw.limitations.is_none());
    }

    #[test]
    fn parse_unknown_fields_are_ignored() {
        // NIP-11 has many optional fields; serde::Deserialize w/ default skips them.
        let doc = r#"{"name":"x","contact":"a@b","supported_nips":[1,11]}"#;
        let raw: RawDoc = serde_json::from_str(doc).unwrap();
        assert_eq!(raw.name.as_deref(), Some("x"));
    }
}
