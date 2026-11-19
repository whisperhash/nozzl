//! Pure path derivation for archived events. No I/O, no global state.
//!
//! Mirrors `docs/design.md` §3.D3 (storage layout) and §6 (d-tag encoding).
//! All output is suitable for both `LocalFileSystem` (POSIX) and `AmazonS3`
//! object keys: lowercase hex identifiers, decimal kind, ISO 8601 basic
//! `YYYYMMDDTHHMMSSZ` `created_at`, and a percent-encoded d-tag whose
//! unreserved set is `[A-Za-z0-9._~-]`.
//!
//! The layout is fully Hive-partitioned (`key=value` at every level):
//! `c={class}/k={kind}/p={shard}/dt={YYYY-MM-DD}/events-{ulid}.parquet`. The
//! per-kind slug is deliberately **not** on disk — it's functionally determined
//! by `kind` (1:1 in config) and is surfaced only in metrics/logs, sourced from
//! the config `kind→slug` map, never parsed back out of a path.
//!
//! Pubkeys are sharded across two intermediate directories before the
//! full-pubkey segment: `p={hex[0..2]}/p={hex[2..4]}/p={hex}/...`. 256×256
//! buckets keep any one directory bounded even at billions of pubkeys, which
//! matters for filesystem tooling on the local backend (file managers, `ls`,
//! `tar`, `rsync` all degrade past ~10k entries per dir). Pubkeys are
//! uniformly distributed by construction (32-byte random scalars), so a
//! direct hex prefix suffices — no hashing. Full rationale, scale math, and
//! shell helper for navigating to a known coordinate are in
//! `docs/design.md` §3.D3 under "Pubkey sharding".
//!
//! NOTE: we deliberately construct the final `object_store::path::Path` via
//! `Path::parse` rather than `Path::from(&str)` / `Path::from_iter`. The
//! latter applies `object_store`'s own percent-encoding pass, which mangles
//! literal `~` (RFC 3986 unreserved) into `%7E`. `Path::parse` validates
//! segments without re-encoding, preserving the spec-mandated form.

use std::path::PathBuf;

use chrono::{DateTime, NaiveDate, Utc};
use nostr::Event;
use nozzl_types::{AddressCoord, EventClass};
use object_store::path::Path;
use percent_encoding::{AsciiSet, CONTROLS, percent_encode};
use sha2::{Digest, Sha256};
use ulid::Ulid;

/// The maximum length of an unhashed `d_encoded` body (the part after `d=`).
/// Beyond this, we fall back to the `sha256:<first-16-hex>` form.
const D_ENCODED_MAX_LEN: usize = 200;

/// `[A-Za-z0-9._~-]` is the RFC 3986 "unreserved" set. We must percent-encode
/// every other byte. `percent-encoding`'s `AsciiSet` is defined by exclusion
/// from `CONTROLS`, so we add every printable byte that is NOT unreserved.
const UNRESERVED_EXCEPTIONS: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(0x7f); // DEL

/// Derive the (legacy per-event JSON) archive path for an event.
///
/// NOTE: this coordinate-based per-event JSON layout is legacy — production
/// writes Parquet chunks via [`chunk_path`]. It is retained for the d-tag
/// encoding tests and any out-of-band per-event tooling.
///
/// Returns `None` if the event's kind is neither replaceable nor addressable
/// per NIP-01. The returned `Path` is rooted at the archive root (no leading
/// slash); callers prepend any backend-level prefix via the `object_store`
/// configuration, not by string concatenation.
///
/// # Panics
///
/// Panics only if the encoded path violates `object_store::path::Path`'s
/// segment rules — which by construction it cannot. The percent-encoding
/// rules in this module guarantee no `/`, no NUL, no control characters,
/// and no `.` / `..` segments. The expectation here is a load-bearing
/// invariant assertion, not a runtime check.
#[must_use]
pub fn event_path(event: &Event) -> Option<Path> {
    let coord = AddressCoord::from_event(event)?;
    let raw = build_raw_path(event, &coord);
    Some(Path::parse(raw).expect("encoded archive path must be Path-valid"))
}

/// Build the raw path string. Split out from `event_path` so tests can
/// assert exact strings without round-tripping through `Path`.
pub(crate) fn build_raw_path(event: &Event, coord: &AddressCoord) -> String {
    let ts = format_created_at(event.created_at.as_secs());
    let event_id_hex = event.id.to_hex();
    match coord {
        AddressCoord::Replaceable { kind, pubkey } => {
            let pk = pubkey.to_hex();
            let (s1, s2) = pubkey_shards(&pk);
            format!("c=replaceable/k={kind}/p={s1}/p={s2}/p={pk}/{ts}-{event_id_hex}.json")
        }
        AddressCoord::Addressable { kind, pubkey, d } => {
            let pk = pubkey.to_hex();
            let (s1, s2) = pubkey_shards(&pk);
            let d_enc = encode_d_tag(d);
            format!(
                "c=addressable/k={kind}/p={s1}/p={s2}/p={pk}/d={d_enc}/{ts}-{event_id_hex}.json"
            )
        }
    }
}

/// Split a 64-char lowercase pubkey hex into two 2-char shard prefixes
/// (`hex[0..2]`, `hex[2..4]`). See module docs for the rationale.
fn pubkey_shards(pk_hex: &str) -> (&str, &str) {
    debug_assert_eq!(pk_hex.len(), 64, "pubkey hex must be 64 ASCII chars");
    (&pk_hex[0..2], &pk_hex[2..4])
}

/// Format a Unix-seconds timestamp as ISO 8601 basic UTC: `YYYYMMDDTHHMMSSZ`.
///
/// Designed to give a fixed-width, lex-sortable, human-readable prefix.
/// Years are forced to 4 digits, so the prefix is always exactly 16 chars
/// for any timestamp in `[1, year-9999]`. Pre-epoch (negative seconds) and
/// year-> 9999 timestamps are clamped to a sentinel that sorts to one
/// extreme — these never appear in real Nostr traffic.
fn format_created_at(secs: u64) -> String {
    use chrono::Datelike;

    let s = i64::try_from(secs).unwrap_or(i64::MAX);
    match chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0) {
        Some(dt) if (1..=9999).contains(&dt.year()) => dt.format("%Y%m%dT%H%M%SZ").to_string(),
        // Anything outside [year 1, year 9999] would either truncate to a
        // wrong year or produce a 5+-digit year that breaks fixed-width
        // lex sort. Use a sentinel that sorts to the very start.
        _ => "00000000T000000Z".to_string(),
    }
}

/// Encode a d-tag value per design.md §6.
///
/// 1. Percent-encode bytes outside `[A-Za-z0-9._~-]`.
/// 2. If the result is ≤ `D_ENCODED_MAX_LEN` bytes, return it verbatim.
/// 3. Otherwise, return `sha256:<first-16-hex-of-sha256(raw_bytes)>`.
///
/// The `:` in the hashed-fallback form is illegal in step-2 output (it would
/// have been percent-encoded), so the two forms can never collide.
#[must_use]
pub fn encode_d_tag(d: &str) -> String {
    let encoded: String = percent_encode(d.as_bytes(), UNRESERVED_EXCEPTIONS).collect();
    if encoded.len() <= D_ENCODED_MAX_LEN {
        encoded
    } else {
        let mut hasher = Sha256::new();
        hasher.update(d.as_bytes());
        let digest = hasher.finalize();
        let hex = hex::encode(digest);
        // First 16 hex chars = first 8 bytes. Spec text reads "first-16-hex".
        format!("sha256:{}", &hex[..16])
    }
}

// ============================================================================
// Chunk paths (Parquet) and WAL paths.
// ============================================================================

/// The pubkey shard component of a chunk path. Wraps a single ASCII hex char
/// (`0-9a-f`), giving 16 buckets when used as the only shard level.
///
/// Construction validates that the byte is one of the 16 lowercase hex digits;
/// callers cannot construct an invalid shard.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct PubkeyShard(char);

impl PubkeyShard {
    /// Derive the shard for a hex-encoded pubkey. Returns `None` if the input
    /// is empty or its first char is not a valid lowercase hex digit.
    #[must_use]
    pub fn from_pubkey_hex(pubkey_hex: &str) -> Option<Self> {
        let c = pubkey_hex.chars().next()?;
        if c.is_ascii_digit() || ('a'..='f').contains(&c) {
            Some(Self(c))
        } else {
            None
        }
    }

    #[must_use]
    pub fn as_char(self) -> char {
        self.0
    }

    /// All 16 valid shard chars in order. Used by the writer-task registry to
    /// pre-spawn one task per shard for each configured kind.
    #[must_use]
    pub fn all() -> [Self; 16] {
        [
            Self('0'),
            Self('1'),
            Self('2'),
            Self('3'),
            Self('4'),
            Self('5'),
            Self('6'),
            Self('7'),
            Self('8'),
            Self('9'),
            Self('a'),
            Self('b'),
            Self('c'),
            Self('d'),
            Self('e'),
            Self('f'),
        ]
    }
}

/// Map an `EventClass` to the Hive-style top-level directory segment used in
/// chunk paths (`c=replaceable` / `c=addressable` / `c=regular`); only Ephemeral
/// (which relays don't persist) returns `None`.
///
/// NOTE: this is **deliberately distinct** from `EventClass::chunk_metric_label`.
/// The on-disk segment carries the Hive `c=` prefix (`c=addressable`) so the path
/// is a standard partitioned dataset; the *metric label value* stays bare
/// (`addressable`). On-disk representation ≠ metric label — do not collapse them.
fn class_segment(class: EventClass) -> Option<&'static str> {
    match class {
        EventClass::Replaceable => Some("c=replaceable"),
        EventClass::Addressable => Some("c=addressable"),
        EventClass::Regular => Some("c=regular"),
        EventClass::Ephemeral => None,
    }
}

/// Public accessor for the Hive-style top-level chunk-path directory segment of a
/// class (`"c=replaceable"` / `"c=addressable"` / `"c=regular"`; `None` for
/// Ephemeral, which isn't archived).
///
/// This is the **single source of truth** for those directory names: `nozzl-export`
/// builds its `read_parquet` glob from this rather than duplicating the strings,
/// so the read path can never drift from what [`chunk_path`] writes.
#[must_use]
pub fn class_dir_segment(class: EventClass) -> Option<&'static str> {
    class_segment(class)
}

/// Decode a Hive class segment (`c=replaceable` / `c=addressable` / `c=regular`)
/// back into its `EventClass` — the inverse of [`class_segment`]. The single
/// source of truth for reading the class back off disk (storage scan + WAL
/// recovery), so those callers never hardcode the strings.
///
/// Returns `None` for any segment that isn't one of the three archived classes.
#[must_use]
pub fn parse_class_segment(seg: &str) -> Option<EventClass> {
    match seg {
        "c=replaceable" => Some(EventClass::Replaceable),
        "c=addressable" => Some(EventClass::Addressable),
        "c=regular" => Some(EventClass::Regular),
        _ => None,
    }
}

/// Decode a `k={kind}` kind-directory segment back into its `kind` — the inverse
/// of the `k={kind}` fragment that [`chunk_path`] emits.
///
/// `{kind}` is a decimal `u16`. The match is **exact**: the whole remainder after
/// `k=` must parse as a `u16`, so a legacy slugged segment (`k=3-follow-lists`)
/// returns `None` rather than matching `k=3`. This exactness also prevents
/// `k=3` from being confused with `k=30` at the glob/scan layer.
///
/// Returns `None` if the segment lacks the `k=` prefix or the remainder isn't a
/// valid `u16`.
#[must_use]
pub fn parse_kind_segment(seg: &str) -> Option<u16> {
    seg.strip_prefix("k=")?.parse::<u16>().ok()
}

/// Build the object-store path for a Parquet chunk:
///
/// ```text
/// c={class}/k={kind}/p={shard}/dt={YYYY-MM-DD}/events-{ulid}.parquet
/// ```
///
/// Returns `None` only for the non-archived Ephemeral class (Replaceable,
/// Addressable, and Regular all archive).
///
/// # Panics
/// Panics only if the encoded path violates `object_store::path::Path`'s
/// segment rules. By construction it cannot — shard is `[0-9a-f]`, dates are
/// `YYYY-MM-DD`, and ULIDs are Crockford-base32. The expectation here is a
/// load-bearing invariant assertion.
#[must_use]
pub fn chunk_path(
    class: EventClass,
    kind: u16,
    shard: PubkeyShard,
    date: NaiveDate,
    ulid: Ulid,
) -> Option<Path> {
    let class_seg = class_segment(class)?;
    let date_str = date.format("%Y-%m-%d").to_string();
    let raw = format!(
        "{class_seg}/k={kind}/p={c}/dt={date_str}/events-{ulid}.parquet",
        c = shard.as_char(),
    );
    Some(Path::parse(raw).expect("encoded chunk path must be Path-valid"))
}

/// Build the local-disk path for a WAL file. WAL files live under `_wal/`
/// under the storage root; they are local-only (S3 has no append) and use a
/// `PathBuf` rather than an `object_store::Path`.
///
/// Format: `{root}/_wal/c={class}-k={kind}-p={shard}.wal`
///
/// Returns `None` for kinds that aren't archived.
#[must_use]
pub fn wal_path(
    root: &std::path::Path,
    class: EventClass,
    kind: u16,
    shard: PubkeyShard,
) -> Option<PathBuf> {
    let class_seg = class_segment(class)?;
    let mut p = PathBuf::from(root);
    p.push("_wal");
    p.push(format!("{class_seg}-k={kind}-p={c}.wal", c = shard.as_char()));
    Some(p)
}

/// Convert a unix-seconds timestamp to a `NaiveDate` in UTC. Used by the
/// writer task to pick `dt=` when constructing chunk paths.
#[must_use]
pub fn date_from_unix_secs(secs: u64) -> NaiveDate {
    let s = i64::try_from(secs).unwrap_or(i64::MAX);
    DateTime::<Utc>::from_timestamp(s, 0).map_or_else(
        || NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is valid"),
        |dt| dt.date_naive(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};

    /// Build a signed event of `kind` with the given tags and a fixed
    /// `created_at`. We sign with random keys (we only need a structurally
    /// valid event for path derivation; pubkey/event-id values are random
    /// per call but stable for the duration of a single test).
    fn build(kind: u16, tags: Vec<Tag>, created_at: u64) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(kind), "")
            .tags(tags)
            .custom_created_at(Timestamp::from_secs(created_at))
            .sign_with_keys(&keys)
            .unwrap()
    }

    // ---- d-tag encoding boundary cases ----

    #[test]
    fn encode_empty_is_empty() {
        assert_eq!(encode_d_tag(""), "");
    }

    #[test]
    fn encode_unreserved_passthrough() {
        assert_eq!(encode_d_tag("abc"), "abc");
        assert_eq!(encode_d_tag("ABC"), "ABC");
        assert_eq!(encode_d_tag("0-9"), "0-9");
        assert_eq!(encode_d_tag("a.b_c~d"), "a.b_c~d");
    }

    #[test]
    fn encode_slash() {
        assert_eq!(encode_d_tag("my/draft"), "my%2Fdraft");
    }

    #[test]
    fn encode_space() {
        assert_eq!(encode_d_tag("hello world"), "hello%20world");
    }

    #[test]
    fn encode_nul() {
        assert_eq!(encode_d_tag("\u{0}"), "%00");
    }

    #[test]
    fn encode_emoji() {
        // U+1F680 → F0 9F 9A 80
        assert_eq!(encode_d_tag("\u{1F680}"), "%F0%9F%9A%80");
    }

    #[test]
    fn encode_uppercase_hex() {
        // Percent-encoding must use uppercase hex (per design and RFC 3986
        // recommendation). `percent-encoding` already does this.
        let out = encode_d_tag("\x0a"); // LF
        assert_eq!(out, "%0A");
    }

    #[test]
    fn encode_at_200_byte_boundary_unhashed() {
        // 200 unreserved bytes encode to 200 bytes — no fallback.
        let d: String = "a".repeat(200);
        let out = encode_d_tag(&d);
        assert_eq!(out.len(), 200);
        assert_eq!(out, d);
    }

    #[test]
    fn encode_at_199_byte_boundary_unhashed() {
        let d: String = "a".repeat(199);
        let out = encode_d_tag(&d);
        assert_eq!(out, d);
    }

    #[test]
    fn encode_201_unreserved_falls_back_to_hash() {
        // 201 unreserved bytes encode to 201 bytes → exceeds 200 → hash.
        let d: String = "a".repeat(201);
        let out = encode_d_tag(&d);
        assert!(out.starts_with("sha256:"));
        assert_eq!(out.len(), "sha256:".len() + 16);
    }

    #[test]
    fn encode_short_but_expanded_falls_back_to_hash() {
        // 70 spaces → 210 bytes encoded → hashed.
        let d: String = " ".repeat(70);
        let out = encode_d_tag(&d);
        assert!(out.starts_with("sha256:"));
    }

    #[test]
    fn hashed_form_is_unambiguous_with_unreserved_form() {
        // No unreserved-set output ever contains ':' — the hashed prefix
        // `sha256:` is therefore unambiguous.
        assert!(!encode_d_tag(":").contains(':'));
        assert_eq!(encode_d_tag(":"), "%3A");
    }

    #[test]
    fn hashed_form_is_deterministic() {
        let d: String = "z".repeat(500);
        assert_eq!(encode_d_tag(&d), encode_d_tag(&d));
    }

    // ---- event_path: replaceable ----

    #[allow(clippy::case_sensitive_file_extension_comparisons)] // .json is ASCII; case is not a concern
    #[test]
    fn replaceable_kind_0_path_shape() {
        // 1_700_000_000 = 2023-11-14T22:13:20Z
        let e = build(0, vec![], 1_700_000_000);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(
            s.starts_with("c=replaceable/k=0/p="),
            "unexpected prefix: {s}"
        );
        assert!(s.ends_with(".json"), "{s}");
        // ISO 8601 basic timestamp appears immediately before the id.
        assert!(s.contains("/20231114T221320Z-"), "ts not iso: {s}");
    }

    #[test]
    fn replaceable_kind_3_path_shape() {
        // created_at = 1 → 1970-01-01T00:00:01Z
        let e = build(3, vec![], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.starts_with("c=replaceable/k=3/p="));
        assert!(s.contains("/19700101T000001Z-"), "ts: {s}");
    }

    #[test]
    fn replaceable_kind_10002_path_shape() {
        // created_at = 0 → 1970-01-01T00:00:00Z
        let e = build(10002, vec![], 0);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.starts_with("c=replaceable/k=10002/p="));
        assert!(s.contains("/19700101T000000Z-"));
    }

    #[test]
    fn pubkey_and_event_id_are_64_lowercase_hex() {
        let e = build(0, vec![], 0);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        let rest = s
            .strip_prefix("c=replaceable/k=0/p=")
            .unwrap();
        // Layout after `p=`: `{shard1}/p={shard2}/p={pk}/{file}`.
        let (s1, after1) = rest.split_once('/').unwrap();
        let after1 = after1.strip_prefix("p=").unwrap();
        let (s2, after2) = after1.split_once('/').unwrap();
        let after2 = after2.strip_prefix("p=").unwrap();
        let (pk, tail) = after2.split_once('/').unwrap();

        assert_eq!(s1.len(), 2, "shard1 len {}", s1.len());
        assert_eq!(s2.len(), 2, "shard2 len {}", s2.len());
        assert_eq!(pk.len(), 64, "pubkey hex len {}", pk.len());
        assert_eq!(&pk[0..2], s1, "shard1 must be pubkey[0..2]");
        assert_eq!(&pk[2..4], s2, "shard2 must be pubkey[2..4]");
        for seg in [s1, s2, pk] {
            assert!(
                seg.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "non-lowercase-hex seg: {seg}"
            );
        }

        let eid = tail
            .strip_prefix("19700101T000000Z-")
            .and_then(|x| x.strip_suffix(".json"))
            .unwrap();
        assert_eq!(eid.len(), 64);
        assert!(
            eid.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn replaceable_path_has_two_shard_levels() {
        let e = build(0, vec![], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        // Tree shape: `replaceable/k=…/p=AB/p=CD/p={64hex}/…`.
        // 4 slashes before the file name (kind, shard1, shard2, full pk).
        let segs: Vec<&str> = s.split('/').collect();
        assert_eq!(segs[0], "c=replaceable");
        assert_eq!(segs[1], "k=0");
        assert!(segs[2].starts_with("p=") && segs[2].len() == "p=".len() + 2);
        assert!(segs[3].starts_with("p=") && segs[3].len() == "p=".len() + 2);
        assert!(segs[4].starts_with("p=") && segs[4].len() == "p=".len() + 64);
    }

    #[test]
    fn addressable_path_has_two_shard_levels_before_d() {
        let e = build(30023, vec![Tag::identifier("hello")], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        let segs: Vec<&str> = s.split('/').collect();
        assert_eq!(segs[0], "c=addressable");
        assert_eq!(segs[1], "k=30023");
        assert!(segs[2].starts_with("p=") && segs[2].len() == "p=".len() + 2);
        assert!(segs[3].starts_with("p=") && segs[3].len() == "p=".len() + 2);
        assert!(segs[4].starts_with("p=") && segs[4].len() == "p=".len() + 64);
        assert_eq!(segs[5], "d=hello");
    }

    #[test]
    fn shards_are_pubkey_prefix_not_hash() {
        // Two events from the same author share both shard segments — that
        // is the whole point of placing the version history of one address
        // coordinate under a single leaf directory.
        let keys = nostr::Keys::generate();
        let pk_hex = keys.public_key().to_hex();
        let e1 = EventBuilder::new(Kind::from_u16(0), "a")
            .custom_created_at(Timestamp::from_secs(1))
            .sign_with_keys(&keys)
            .unwrap();
        let e2 = EventBuilder::new(Kind::from_u16(0), "b")
            .custom_created_at(Timestamp::from_secs(2))
            .sign_with_keys(&keys)
            .unwrap();
        let p1 = event_path(&e1).unwrap();
        let p2 = event_path(&e2).unwrap();
        let expected_dir = format!(
            "c=replaceable/k=0/p={s1}/p={s2}/p={pk_hex}",
            s1 = &pk_hex[0..2],
            s2 = &pk_hex[2..4],
        );
        assert!(
            p1.as_ref().starts_with(&expected_dir),
            "p1: {} expected dir: {expected_dir}",
            p1.as_ref()
        );
        assert!(
            p2.as_ref().starts_with(&expected_dir),
            "p2: {} expected dir: {expected_dir}",
            p2.as_ref()
        );
    }

    #[test]
    fn iso_timestamp_is_lex_chronological() {
        // Two events one second apart should produce filenames that
        // sort with the older one first.
        let a = build(0, vec![], 1_700_000_000);
        let b = build(0, vec![], 1_700_000_001);
        let pa = event_path(&a).unwrap();
        let pb = event_path(&b).unwrap();
        let fname_a = pa.as_ref().rsplit_once('/').unwrap().1;
        let fname_b = pb.as_ref().rsplit_once('/').unwrap().1;
        assert!(
            fname_a < fname_b,
            "expected lex order to match chrono: {fname_a} < {fname_b}"
        );
    }

    #[test]
    fn iso_timestamp_clamps_pre_epoch_via_sentinel() {
        // Cast i64::MAX as u64 — represents an absurd far-future event;
        // chrono will reject it. We use the sentinel.
        let raw = build_raw_path(
            &build(0, vec![], 0),
            &nozzl_types::AddressCoord::Replaceable {
                kind: 0,
                pubkey: nostr::Keys::generate().public_key(),
            },
        );
        // sanity: real timestamp produced 1970-... — the sentinel tests
        // are stress-style on `format_created_at` directly.
        assert!(raw.contains("19700101T000000Z-"));
        assert_eq!(super::format_created_at(0), "19700101T000000Z");
        assert_eq!(super::format_created_at(u64::MAX), "00000000T000000Z");
    }

    // ---- event_path: addressable ----

    #[test]
    fn addressable_with_simple_d_tag() {
        let e = build(30023, vec![Tag::identifier("abc")], 1_700_000_000);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.starts_with("c=addressable/k=30023/p="));
        assert!(s.contains("/d=abc/"), "{s}");
    }

    #[test]
    fn addressable_with_empty_d_tag_literal_d_equals() {
        let e = build(30023, vec![Tag::identifier("")], 1_700_000_000);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.contains("/d=/"), "expected literal `d=` segment: {s}");
    }

    #[test]
    fn addressable_missing_d_tag_treated_as_empty() {
        let e = build(30063, vec![], 1_700_000_000);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.contains("/d=/"), "{s}");
    }

    #[test]
    fn addressable_with_slash_in_d_tag() {
        let e = build(30023, vec![Tag::identifier("my/draft")], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.contains("/d=my%2Fdraft/"), "{s}");
    }

    #[test]
    fn addressable_with_space_in_d_tag() {
        let e = build(30023, vec![Tag::identifier("hello world")], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.contains("/d=hello%20world/"), "{s}");
    }

    #[test]
    fn addressable_with_emoji_in_d_tag() {
        let e = build(30023, vec![Tag::identifier("\u{1F680}")], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.contains("/d=%F0%9F%9A%80/"), "{s}");
    }

    #[test]
    fn addressable_with_nul_in_d_tag() {
        let e = build(30023, vec![Tag::identifier("\u{0}")], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        assert!(s.contains("/d=%00/"), "{s}");
    }

    #[test]
    fn addressable_with_199_byte_d_tag() {
        let d: String = "a".repeat(199);
        let e = build(30023, vec![Tag::identifier(&d)], 1);
        let p = event_path(&e).unwrap();
        let expected = format!("/d={d}/");
        assert!(p.as_ref().contains(&expected));
    }

    #[test]
    fn addressable_with_200_byte_d_tag() {
        let d: String = "a".repeat(200);
        let e = build(30023, vec![Tag::identifier(&d)], 1);
        let p = event_path(&e).unwrap();
        let expected = format!("/d={d}/");
        assert!(p.as_ref().contains(&expected));
    }

    #[test]
    fn addressable_with_201_byte_d_tag_falls_back_to_hash() {
        let d: String = "a".repeat(201);
        let e = build(30023, vec![Tag::identifier(&d)], 1);
        let p = event_path(&e).unwrap();
        let s = p.as_ref();
        let needle = "/d=sha256:";
        let idx = s
            .find(needle)
            .unwrap_or_else(|| panic!("missing hash form: {s}"));
        let after = &s[idx + needle.len()..];
        let (hex_part, _rest) = after.split_once('/').unwrap();
        assert_eq!(hex_part.len(), 16, "hex prefix len {}", hex_part.len());
        assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn addressable_takes_first_d_tag_only() {
        let e = build(
            30023,
            vec![Tag::identifier("first"), Tag::identifier("second")],
            1,
        );
        let p = event_path(&e).unwrap();
        assert!(p.as_ref().contains("/d=first/"));
        assert!(!p.as_ref().contains("/d=second/"));
    }

    // ---- classes without an address coordinate ----
    // `event_path` is the legacy coordinate-based (per-event JSON) path; it
    // keys on `AddressCoord`, which only exists for replaceable/addressable.
    // Regular events ARE archived, but via `chunk_path` (class segment
    // "regular"), not `event_path` — so `event_path` is `None` for them.

    #[test]
    fn kind_1_regular_event_path_is_none() {
        let e = build(1, vec![], 1);
        assert!(event_path(&e).is_none());
    }

    #[test]
    fn kind_999_regular_event_path_is_none() {
        let e = build(999, vec![], 1);
        assert!(event_path(&e).is_none());
    }

    #[test]
    fn kind_22500_ephemeral_returns_none() {
        let e = build(22500, vec![], 1);
        assert!(event_path(&e).is_none());
    }

    // ---- determinism ----

    #[test]
    fn same_event_yields_same_path() {
        let e = build(0, vec![], 1_700_000_000);
        assert_eq!(
            event_path(&e).unwrap(),
            event_path(&e).unwrap()
        );
    }

    // ---- parse_kind_segment / parse_class_segment: inverse of the on-disk path ----

    #[test]
    fn parse_kind_segment_simple() {
        assert_eq!(parse_kind_segment("k=0"), Some(0));
        assert_eq!(parse_kind_segment("k=10002"), Some(10002));
        assert_eq!(parse_kind_segment("k=30063"), Some(30063));
    }

    #[test]
    fn parse_kind_segment_is_exact_not_prefix() {
        // `k=3` must not be confused with `k=30`; a legacy slugged segment
        // (`k=3-follow-lists`) no longer parses (slug is off-disk now).
        assert_eq!(parse_kind_segment("k=3"), Some(3));
        assert_eq!(parse_kind_segment("k=3-follow-lists"), None);
    }

    #[test]
    fn parse_kind_and_class_round_trip_chunk_path() {
        // The fragments emitted by `chunk_path` must decode back to their inputs.
        for kind in [0u16, 10050, 31990] {
            let p = chunk_path(
                EventClass::Replaceable,
                kind,
                PubkeyShard::from_pubkey_hex("0").unwrap(),
                NaiveDate::from_ymd_opt(2026, 5, 2).unwrap(),
                Ulid::nil(),
            )
            .unwrap();
            // segments: c={class}/k={kind}/p=.../dt=.../events-….parquet
            let segs: Vec<&str> = p.as_ref().split('/').collect();
            assert_eq!(parse_class_segment(segs[0]), Some(EventClass::Replaceable));
            assert_eq!(parse_kind_segment(segs[1]), Some(kind));
        }
    }

    #[test]
    fn parse_kind_segment_rejects_malformed() {
        assert_eq!(parse_kind_segment(""), None);
        assert_eq!(parse_kind_segment("profile-metadata"), None); // no k=
        assert_eq!(parse_kind_segment("k="), None); // empty kind
        assert_eq!(parse_kind_segment("k=abc"), None); // non-numeric kind
        assert_eq!(parse_kind_segment("k=99999"), None); // 99999 > u16::MAX
        assert_eq!(parse_kind_segment("k=0-profile-metadata"), None); // legacy slugged
        assert_eq!(parse_kind_segment("p=0"), None); // wrong prefix
    }

    #[test]
    fn parse_class_segment_round_trip_and_rejects() {
        for class in [
            EventClass::Replaceable,
            EventClass::Addressable,
            EventClass::Regular,
        ] {
            let seg = class_dir_segment(class).unwrap();
            assert_eq!(parse_class_segment(seg), Some(class));
        }
        // Bare (pre-`c=`) names must NOT parse — that's how WAL recovery / the
        // storage scan detect an un-migrated legacy layout.
        assert_eq!(parse_class_segment("replaceable"), None);
        assert_eq!(parse_class_segment("addressable"), None);
        assert_eq!(parse_class_segment("c=ephemeral"), None);
        assert_eq!(parse_class_segment(""), None);
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 1024, .. ProptestConfig::default() })]

        /// For any UTF-8 d-tag, `encode_d_tag` returns a string that:
        ///  - is ≤ 200 bytes OR is exactly `sha256:` + 16 hex chars,
        ///  - contains no `/` (path-segment-safe),
        ///  - contains no NUL or other ASCII control characters,
        ///  - is deterministic.
        #[test]
        fn d_tag_encoding_is_path_safe_and_bounded(d in ".*") {
            let enc = encode_d_tag(&d);
            let enc2 = encode_d_tag(&d);
            prop_assert_eq!(&enc, &enc2);

            if let Some(hex) = enc.strip_prefix("sha256:") {
                prop_assert_eq!(hex.len(), 16);
                prop_assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
            } else {
                prop_assert!(enc.len() <= 200);
            }

            prop_assert!(!enc.contains('/'), "encoded form must not contain '/': {enc}");
            prop_assert!(!enc.chars().any(|c| c.is_ascii_control()),
                "encoded form must not contain control chars: {enc:?}");
        }

        /// The full segment `d={enc}` must round-trip through
        /// `object_store::path::Path::parse` without error — i.e. it is a
        /// valid object_store path component.
        #[test]
        fn d_tag_segment_is_valid_object_store_path(d in ".*") {
            let enc = encode_d_tag(&d);
            let raw = format!(
                "c=addressable/k=30023/p=00/p=11/p=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff/d={enc}/19700101T000001Z-aa.json"
            );
            let path = object_store::path::Path::parse(&raw);
            prop_assert!(path.is_ok(), "Path::parse failed for {raw}: {:?}", path.err());
        }
    }

    // ---- chunk_path / wal_path / PubkeyShard ----

    #[test]
    fn pubkey_shard_accepts_valid_hex() {
        for c in ['0', '5', '9', 'a', 'c', 'f'] {
            let s = PubkeyShard::from_pubkey_hex(&c.to_string());
            assert!(s.is_some(), "char {c} should be valid");
            assert_eq!(s.unwrap().as_char(), c);
        }
    }

    #[test]
    fn pubkey_shard_rejects_uppercase_and_other() {
        assert!(PubkeyShard::from_pubkey_hex("A").is_none());
        assert!(PubkeyShard::from_pubkey_hex("g").is_none());
        assert!(PubkeyShard::from_pubkey_hex("Z").is_none());
        assert!(PubkeyShard::from_pubkey_hex("").is_none());
        assert!(PubkeyShard::from_pubkey_hex("/").is_none());
    }

    #[test]
    fn pubkey_shard_uses_first_char_only() {
        let s = PubkeyShard::from_pubkey_hex(
            "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
        )
        .unwrap();
        assert_eq!(s.as_char(), '8');
    }

    #[test]
    fn pubkey_shard_all_returns_16_distinct() {
        let all = PubkeyShard::all();
        assert_eq!(all.len(), 16);
        let mut chars: Vec<char> = all.iter().map(|s| s.as_char()).collect();
        chars.sort_unstable();
        assert_eq!(
            chars,
            vec![
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
            ]
        );
    }

    #[test]
    fn chunk_path_replaceable_v1() {
        let date = NaiveDate::from_ymd_opt(2026, 5, 2).unwrap();
        // Use Ulid::nil() so the expected string is deterministic (26 zeros).
        let ulid = Ulid::nil();
        let p = chunk_path(
            EventClass::Replaceable,
            0,
            PubkeyShard::from_pubkey_hex("8").unwrap(),
            date,
            ulid,
        )
        .expect("kind 0 is replaceable");
        assert_eq!(
            p.as_ref(),
            "c=replaceable/k=0/p=8/dt=2026-05-02/events-00000000000000000000000000.parquet"
        );
    }

    #[test]
    fn chunk_path_addressable() {
        let date = NaiveDate::from_ymd_opt(2026, 5, 2).unwrap();
        let ulid = Ulid::nil();
        let p = chunk_path(
            EventClass::Addressable,
            30063,
            PubkeyShard::from_pubkey_hex("a").unwrap(),
            date,
            ulid,
        )
        .expect("kind 30063 is addressable");
        assert_eq!(
            p.as_ref(),
            "c=addressable/k=30063/p=a/dt=2026-05-02/events-00000000000000000000000000.parquet"
        );
    }

    #[test]
    fn chunk_path_regular_resolves() {
        let date = NaiveDate::from_ymd_opt(2026, 5, 2).unwrap();
        let ulid = Ulid::nil();
        let p = chunk_path(
            EventClass::Regular,
            9735,
            PubkeyShard::from_pubkey_hex("0").unwrap(),
            date,
            ulid,
        )
        .expect("kind 9735 is regular and archived");
        assert_eq!(
            p.as_ref(),
            "c=regular/k=9735/p=0/dt=2026-05-02/events-00000000000000000000000000.parquet"
        );
    }

    #[test]
    fn chunk_path_ephemeral_is_none() {
        let date = NaiveDate::from_ymd_opt(2026, 5, 2).unwrap();
        let ulid = Ulid::new();
        assert!(
            chunk_path(
                EventClass::Ephemeral,
                22000,
                PubkeyShard::from_pubkey_hex("0").unwrap(),
                date,
                ulid
            )
            .is_none()
        );
    }

    #[test]
    fn wal_path_replaceable() {
        let p = wal_path(
            std::path::Path::new("/var/data"),
            EventClass::Replaceable,
            0,
            PubkeyShard::from_pubkey_hex("8").unwrap(),
        )
        .expect("kind 0 is replaceable");
        assert_eq!(
            p.to_str().unwrap(),
            "/var/data/_wal/c=replaceable-k=0-p=8.wal"
        );
    }

    #[test]
    fn wal_path_addressable() {
        let p = wal_path(
            std::path::Path::new("data/events"),
            EventClass::Addressable,
            30063,
            PubkeyShard::from_pubkey_hex("c").unwrap(),
        )
        .expect("kind 30063 is addressable");
        assert!(
            p.to_str()
                .unwrap()
                .ends_with("_wal/c=addressable-k=30063-p=c.wal")
        );
    }

    #[test]
    fn wal_path_regular() {
        let p = wal_path(
            std::path::Path::new("/var/data"),
            EventClass::Regular,
            9735,
            PubkeyShard::from_pubkey_hex("3").unwrap(),
        )
        .expect("kind 9735 is regular and archived");
        assert_eq!(
            p.to_str().unwrap(),
            "/var/data/_wal/c=regular-k=9735-p=3.wal"
        );
    }

    #[test]
    fn wal_path_ephemeral_is_none() {
        let p = wal_path(
            std::path::Path::new("/var/data"),
            EventClass::Ephemeral,
            22500,
            PubkeyShard::from_pubkey_hex("3").unwrap(),
        );
        assert!(p.is_none());
    }

    #[test]
    fn date_from_unix_secs_handles_epoch_and_recent() {
        let epoch = date_from_unix_secs(0);
        assert_eq!(epoch, NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        // 2026-05-02 00:00:00 UTC = 1_777_680_000
        let d = date_from_unix_secs(1_777_680_000);
        assert_eq!(d, NaiveDate::from_ymd_opt(2026, 5, 2).unwrap());
    }

    #[test]
    fn ulid_filenames_sort_lexicographically_by_creation() {
        // Two ULIDs created in succession must compare such that earlier <= later.
        let a = Ulid::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = Ulid::new();
        assert!(a.to_string() < b.to_string(), "ULIDs must lex-sort by time");
    }
}
