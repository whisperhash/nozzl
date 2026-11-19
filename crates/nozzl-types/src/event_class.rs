//! NIP-01 event classification and address coordinates.
//!
//! Classification follows NIP-01 strictly (kind 0 / 3, ranges 10000-19999,
//! 20000-29999, 30000-39999). The `nostr` crate's `Kind::is_replaceable`
//! includes additional kinds (e.g. 41 ChannelMetadata) that NIP-01 does not
//! formally cover; we deliberately use our own classification so the archive's
//! layout decisions stay aligned with the spec.

use nostr::{Event, PublicKey};

/// NIP-01 event-class taxonomy. Pure function of `kind`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EventClass {
    Regular,
    Replaceable,
    Addressable,
    Ephemeral,
}

impl EventClass {
    pub fn classify(kind: u16) -> Self {
        match kind {
            0 | 3 => Self::Replaceable,
            k if (10_000..20_000).contains(&k) => Self::Replaceable,
            k if (20_000..30_000).contains(&k) => Self::Ephemeral,
            k if (30_000..40_000).contains(&k) => Self::Addressable,
            _ => Self::Regular,
        }
    }

    /// Single source of truth for "does this class produce archived chunks?".
    /// Returns `Some(metric_label)` for classes the writer pool produces
    /// chunks for, or `None` for classes it skips.
    ///
    /// Used by:
    /// - `nozzl_archive::chunk_writer` to produce the `class=...` label on
    ///   `chunks_published_total` and related metrics.
    /// - The binary's `Metrics::preallocate_known_series` call to filter
    ///   the (class, kind, slug, shard) preallocation set down to classes
    ///   that will actually emit chunks.
    ///
    /// Archived classes are Replaceable, Addressable, and Regular (e.g.
    /// kind-9735 zap receipts). Ephemeral is the only non-archived class —
    /// relays don't persist it. This method and `class_segment`/`wal_path` in
    /// `nozzl-archive::path_enc` must agree on *which* classes archive (the
    /// latter gates the writer-task spawn); when changing the archived set,
    /// change both.
    ///
    /// NOTE: this returns the **bare** metric label (`addressable`). The on-disk
    /// directory segment is deliberately different — `path_enc::class_segment`
    /// returns the Hive-prefixed `c=addressable` so the archive is a standard
    /// partitioned dataset. Metric label value ≠ on-disk segment; do not collapse
    /// the two (the storage scan lists by the `c=` segment but labels with this).
    #[must_use]
    pub fn chunk_metric_label(self) -> Option<&'static str> {
        match self {
            Self::Replaceable => Some("replaceable"),
            Self::Addressable => Some("addressable"),
            Self::Regular => Some("regular"),
            Self::Ephemeral => None,
        }
    }
}

/// The address coordinate that uniquely identifies a replaceable or addressable
/// event slot. `None` for regular/ephemeral events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddressCoord {
    Replaceable {
        kind: u16,
        pubkey: PublicKey,
    },
    Addressable {
        kind: u16,
        pubkey: PublicKey,
        d: String,
    },
}

impl AddressCoord {
    /// Returns `Some(...)` for replaceable or addressable kinds, `None` otherwise.
    /// For addressable kinds without a `d` tag, treats `d` as empty string.
    pub fn from_event(e: &Event) -> Option<Self> {
        let kind = e.kind.as_u16();
        match EventClass::classify(kind) {
            EventClass::Replaceable => Some(Self::Replaceable {
                kind,
                pubkey: e.pubkey,
            }),
            EventClass::Addressable => Some(Self::Addressable {
                kind,
                pubkey: e.pubkey,
                d: e.tags.identifier().unwrap_or("").to_owned(),
            }),
            EventClass::Regular | EventClass::Ephemeral => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Tag};

    fn build_event(kind: u16, tags: Vec<Tag>) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(nostr::Kind::from_u16(kind), "test")
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap()
    }

    // ----- EventClass::classify boundary table -----

    #[test]
    fn classify_kind_0_is_replaceable() {
        assert_eq!(EventClass::classify(0), EventClass::Replaceable);
    }

    #[test]
    fn classify_kind_3_is_replaceable() {
        assert_eq!(EventClass::classify(3), EventClass::Replaceable);
    }

    #[test]
    fn classify_kind_1_is_regular() {
        assert_eq!(EventClass::classify(1), EventClass::Regular);
    }

    #[test]
    fn classify_kind_4_is_regular() {
        // kind 4 is encrypted DM, regular per NIP-01
        assert_eq!(EventClass::classify(4), EventClass::Regular);
    }

    #[test]
    fn classify_kind_999_is_regular() {
        assert_eq!(EventClass::classify(999), EventClass::Regular);
    }

    #[test]
    fn classify_kind_1000_is_regular() {
        assert_eq!(EventClass::classify(1000), EventClass::Regular);
    }

    #[test]
    fn classify_kind_9999_is_regular() {
        assert_eq!(EventClass::classify(9999), EventClass::Regular);
    }

    #[test]
    fn classify_kind_10000_is_replaceable() {
        assert_eq!(EventClass::classify(10000), EventClass::Replaceable);
    }

    #[test]
    fn classify_kind_10002_is_replaceable() {
        assert_eq!(EventClass::classify(10002), EventClass::Replaceable);
    }

    #[test]
    fn classify_kind_19999_is_replaceable() {
        assert_eq!(EventClass::classify(19999), EventClass::Replaceable);
    }

    #[test]
    fn classify_kind_20000_is_ephemeral() {
        assert_eq!(EventClass::classify(20000), EventClass::Ephemeral);
    }

    #[test]
    fn classify_kind_25000_is_ephemeral() {
        assert_eq!(EventClass::classify(25000), EventClass::Ephemeral);
    }

    #[test]
    fn classify_kind_29999_is_ephemeral() {
        assert_eq!(EventClass::classify(29999), EventClass::Ephemeral);
    }

    #[test]
    fn classify_kind_30000_is_addressable() {
        assert_eq!(EventClass::classify(30000), EventClass::Addressable);
    }

    #[test]
    fn classify_kind_30023_is_addressable() {
        assert_eq!(EventClass::classify(30023), EventClass::Addressable);
    }

    #[test]
    fn classify_kind_30063_is_addressable() {
        assert_eq!(EventClass::classify(30063), EventClass::Addressable);
    }

    #[test]
    fn classify_kind_32267_is_addressable() {
        assert_eq!(EventClass::classify(32267), EventClass::Addressable);
    }

    #[test]
    fn classify_kind_39999_is_addressable() {
        assert_eq!(EventClass::classify(39999), EventClass::Addressable);
    }

    #[test]
    fn classify_kind_40000_is_regular() {
        assert_eq!(EventClass::classify(40000), EventClass::Regular);
    }

    #[test]
    fn classify_kind_max_is_regular() {
        assert_eq!(EventClass::classify(u16::MAX), EventClass::Regular);
    }

    // ----- EventClass::chunk_metric_label (archived-class predicate) -----

    #[test]
    fn chunk_metric_label_covers_archived_classes() {
        assert_eq!(
            EventClass::Replaceable.chunk_metric_label(),
            Some("replaceable")
        );
        assert_eq!(
            EventClass::Addressable.chunk_metric_label(),
            Some("addressable")
        );
        // Regular is archived too (e.g. kind-9735 zap receipts).
        assert_eq!(EventClass::Regular.chunk_metric_label(), Some("regular"));
    }

    #[test]
    fn chunk_metric_label_skips_ephemeral() {
        // Ephemeral is the only non-archived class — relays don't persist it.
        // class_segment()/wal_path() in nozzl-archive::path_enc must agree.
        assert_eq!(EventClass::Ephemeral.chunk_metric_label(), None);
    }

    // ----- AddressCoord::from_event -----

    #[test]
    fn coord_replaceable_kind_0() {
        let e = build_event(0, vec![]);
        let coord = AddressCoord::from_event(&e).expect("kind 0 → Some");
        match coord {
            AddressCoord::Replaceable { kind, .. } => assert_eq!(kind, 0),
            _ => panic!("expected Replaceable"),
        }
    }

    #[test]
    fn coord_replaceable_kind_10002() {
        let e = build_event(10002, vec![]);
        let coord = AddressCoord::from_event(&e).expect("kind 10002 → Some");
        assert!(matches!(
            coord,
            AddressCoord::Replaceable { kind: 10002, .. }
        ));
    }

    #[test]
    fn coord_addressable_with_d_tag() {
        let e = build_event(30023, vec![Tag::identifier("my-article")]);
        let coord = AddressCoord::from_event(&e).expect("kind 30023 → Some");
        match coord {
            AddressCoord::Addressable { kind, d, .. } => {
                assert_eq!(kind, 30023);
                assert_eq!(d, "my-article");
            }
            _ => panic!("expected Addressable"),
        }
    }

    #[test]
    fn coord_addressable_no_d_tag_is_empty() {
        let e = build_event(30063, vec![]);
        let coord = AddressCoord::from_event(&e).expect("kind 30063 → Some");
        match coord {
            AddressCoord::Addressable { kind, d, .. } => {
                assert_eq!(kind, 30063);
                assert_eq!(d, "");
            }
            _ => panic!("expected Addressable"),
        }
    }

    #[test]
    fn coord_addressable_takes_first_d_tag() {
        let e = build_event(
            30023,
            vec![Tag::identifier("first"), Tag::identifier("second")],
        );
        let coord = AddressCoord::from_event(&e).unwrap();
        match coord {
            AddressCoord::Addressable { d, .. } => assert_eq!(d, "first"),
            _ => panic!("expected Addressable"),
        }
    }

    #[test]
    fn coord_regular_returns_none() {
        let e = build_event(1, vec![]);
        assert!(AddressCoord::from_event(&e).is_none());
    }

    #[test]
    fn coord_ephemeral_returns_none() {
        let e = build_event(22500, vec![]);
        assert!(AddressCoord::from_event(&e).is_none());
    }
}
