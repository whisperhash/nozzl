//! REQ filter construction.
//!
//! Helpers for the two distinct filter shapes the acquirer issues:
//!  - **forward** filters: `kinds + since`, no `until`, no `limit`.
//!  - **backfill** page filters: `kinds + since=0 + until + limit`. The
//!    `since=0` is set explicitly because some relays (notably
//!    `relay.zapstore.dev`, which runs zapstore's own server on top of
//!    the `rely` framework) reject paginated-history REQs that have an
//!    `until` but no `since` as "filters are too vague" — the lower
//!    bound of the time window is required even when it's epoch.
//!
//! These functions are pure and unit-tested independently of the network.

use nostr::{Filter, Kind, Timestamp};

/// Compute the `since` value for a forward subscription.
///
/// `min_per_kind` is the smallest known checkpoint across the kinds covered by
/// the REQ — `None` if no kind in the REQ has any prior checkpoint. The safety
/// margin is subtracted with saturate semantics so a checkpoint smaller than
/// the margin (e.g. 0 with margin 60) yields 0 instead of underflowing.
///
/// Returns `None` when `min_per_kind` is `None` (i.e. no `since` should be set
/// at all — relay should send everything, then EOSE).
#[must_use]
pub fn since_with_safety_margin(
    min_per_kind: Option<i64>,
    safety_margin_seconds: u64,
) -> Option<i64> {
    let m = min_per_kind?;
    // Convert the margin to i64 saturating at i64::MAX. In practice we
    // configure margin as a small Duration, so this is just defensive.
    let margin_i64 = i64::try_from(safety_margin_seconds).unwrap_or(i64::MAX);
    Some(m.saturating_sub(margin_i64).max(0))
}

/// Build a forward-subscription [`Filter`].
///
/// `since` is the `since_with_safety_margin` output; pass `None` to omit the
/// `since` field entirely (i.e. let the relay send everything until EOSE).
#[must_use]
pub fn forward_filter(kinds: &[u16], since: Option<i64>) -> Filter {
    let f = Filter::new().kinds(kinds.iter().copied().map(Kind::from_u16));
    apply_since(f, since)
}

/// Build a single-page backfill [`Filter`].
///
/// `until` is the upper bound of this page (`created_at ≤ until`, inclusive
/// per NIP-01). `limit` is the per-page cap; the relay's NIP-11
/// advertised cap or [`AcquirerConfig::backfill_default_limit`] is used by
/// callers, never modified here.
///
/// Always sets `since = 0` — see module docs. The cost is one extra field
/// on every backfill filter; the benefit is acceptance by relays that
/// require an explicit lower time bound on paginated-history REQs.
#[must_use]
pub fn backfill_filter(kinds: &[u16], until: Option<i64>, limit: u32) -> Filter {
    let mut f = Filter::new()
        .kinds(kinds.iter().copied().map(Kind::from_u16))
        .since(Timestamp::from_secs(0));
    if let Some(t) = until {
        // INVARIANT: callers pass a strictly-positive `until`. `handle_eose`
        // detects the epoch floor (a step to `until <= 0`) via
        // `next_backfill_advance` and marks the backfill Complete instead of
        // paginating further — there is nothing older than `created_at = 0` to
        // fetch.
        //
        // A non-positive `until` reaching here is therefore a bug, AND a hazard:
        // `until:0` is interpreted as "no upper bound" by some relays
        // (relay.fountain.fm), which then return their NEWEST events — resetting
        // the backfill walk to the forward edge and looping forever (the
        // 2026-06-15 fountain saw-tooth). As a last-ditch guard against a
        // corrupted/hand-edited checkpoint that slips past the caller, clamp
        // UP to 1 (a real, tiny upper bound) — never DOWN to 0. A stray value
        // then fetches only `created_at <= 1` (epoch) events, EOSEs, and
        // terminates at the floor on the next step instead of saw-toothing.
        // Do NOT "simplify" this back to `unwrap_or(0)`: that reintroduces #23.
        if t <= 0 {
            tracing::warn!(
                until = t,
                "backfill_filter: non-positive until clamped to 1 \
                 (caller should have completed at the epoch floor)"
            );
        }
        let secs = u64::try_from(t.max(1)).unwrap_or(u64::MAX);
        f = f.until(Timestamp::from_secs(secs));
    }
    f.limit(usize::try_from(limit).unwrap_or(usize::MAX))
}

/// Kind used by the dispatch-liveness probe. NDK uses 99999, but `nostr 0.44`'s
/// `Kind` is `u16`-bounded so we use `u16::MAX` (65535) — equally outside any
/// configured / indexed range and guaranteed to produce zero stored events on
/// every public relay we know of.
///
/// Note: `EventClass::classify(65535)` is `Regular`, which is now an archived
/// class. This is benign — the probe uses `limit:0` (no stored events) and no
/// operator configures kind 65535 — but if that ever changes, probe traffic
/// would fall in the archivable Regular band.
pub const PROBE_KIND: u16 = u16::MAX;

/// Build the dispatch-liveness probe [`Filter`].
///
/// NDK-style dispatch-liveness probe: zero-result REQ that should
/// EOSE almost instantly. Kind [`PROBE_KIND`] is in an unindexed range;
/// `limit:0` tells strfry-class relays to skip stored events and EOSE
/// immediately. Relays that reject `limit:0` as malformed CLOSED our
/// probe `sub_id`, which is routed as "alive" by
/// `relay_task::on_probe_settled` — so this shape is robust across
/// implementations.
#[must_use]
pub fn probe_filter() -> Filter {
    Filter::new().kinds([Kind::from_u16(PROBE_KIND)]).limit(0)
}

fn apply_since(f: Filter, since: Option<i64>) -> Filter {
    match since {
        Some(t) => {
            let secs = u64::try_from(t).unwrap_or(0);
            f.since(Timestamp::from_secs(secs))
        }
        None => f,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn since_none_is_none() {
        assert_eq!(since_with_safety_margin(None, 60), None);
    }

    #[test]
    fn since_subtracts_margin() {
        assert_eq!(since_with_safety_margin(Some(1_000), 60), Some(940));
    }

    #[test]
    fn since_saturates_at_zero() {
        assert_eq!(since_with_safety_margin(Some(30), 60), Some(0));
        assert_eq!(since_with_safety_margin(Some(0), 60), Some(0));
    }

    #[test]
    fn since_handles_huge_margin() {
        // Very large but representable u64 → still saturates to 0 cleanly.
        assert_eq!(since_with_safety_margin(Some(100), u64::MAX), Some(0));
    }

    #[test]
    fn since_no_underflow_with_max_margin_at_i64_max_input() {
        // The input itself is the max representable; subtracting any finite
        // margin must still yield a valid non-negative number.
        let r = since_with_safety_margin(Some(i64::MAX), 1).unwrap();
        assert_eq!(r, i64::MAX - 1);
    }

    #[test]
    fn forward_filter_sets_kinds_and_since() {
        let f = forward_filter(&[0, 3, 10002], Some(123_456));
        let kinds = f.kinds.as_ref().expect("kinds set");
        assert_eq!(kinds.len(), 3);
        assert!(kinds.contains(&Kind::from_u16(0)));
        assert!(kinds.contains(&Kind::from_u16(3)));
        assert!(kinds.contains(&Kind::from_u16(10002)));
        assert_eq!(f.since, Some(Timestamp::from_secs(123_456)));
        assert!(f.until.is_none());
        assert!(f.limit.is_none());
    }

    #[test]
    fn forward_filter_omits_since_when_none() {
        let f = forward_filter(&[0], None);
        assert!(f.since.is_none());
    }

    #[test]
    fn backfill_filter_sets_until_and_limit() {
        let f = backfill_filter(&[30023], Some(2_000_000_000), 500);
        let kinds = f.kinds.as_ref().expect("kinds set");
        assert!(kinds.contains(&Kind::from_u16(30023)));
        assert_eq!(f.until, Some(Timestamp::from_secs(2_000_000_000)));
        assert_eq!(f.limit, Some(500));
        assert_eq!(
            f.since,
            Some(Timestamp::from_secs(0)),
            "backfill MUST always set since=0 — relays may reject otherwise (see module docs)"
        );
    }

    #[test]
    fn backfill_filter_no_until_at_start() {
        let f = backfill_filter(&[0], None, 500);
        assert!(f.until.is_none());
        assert_eq!(f.limit, Some(500));
        // since=0 is set even on the first page (no `until` yet).
        assert_eq!(f.since, Some(Timestamp::from_secs(0)));
    }

    #[test]
    fn backfill_filter_clamps_non_positive_until_to_one() {
        // A non-positive `until` is a caller-contract violation
        // (the caller must complete at the epoch floor instead). As a last-ditch
        // guard against a corrupted checkpoint, `backfill_filter` clamps UP to 1
        // — NEVER to 0 — so the stray value can't be read as "no upper bound" by
        // relays that treat `until:0` as unset (fountain.fm) and saw-tooth.
        // until:1 fetches only created_at<=1 (epoch) events and terminates.
        for bad in [-5, -1, 0] {
            let f = backfill_filter(&[0], Some(bad), 500);
            assert_eq!(
                f.until,
                Some(Timestamp::from_secs(1)),
                "non-positive until={bad} must clamp UP to 1, not down to 0"
            );
        }
        // A legitimate positive `until` is passed through untouched.
        let f = backfill_filter(&[0], Some(2_000_000_000), 500);
        assert_eq!(f.until, Some(Timestamp::from_secs(2_000_000_000)));
    }

    #[test]
    fn probe_filter_has_expected_shape() {
        let f = probe_filter();
        let kinds = f.kinds.as_ref().expect("probe filter sets kinds");
        assert_eq!(kinds.len(), 1, "probe filter has exactly one kind");
        assert!(
            kinds.contains(&Kind::from_u16(PROBE_KIND)),
            "probe filter targets PROBE_KIND (u16::MAX; NDK-style unindexed kind)",
        );
        assert_eq!(f.limit, Some(0), "probe filter sets limit:0");
        // Every other field stays unset — the probe is intentionally minimal.
        assert!(f.since.is_none(), "probe filter must not set since");
        assert!(f.until.is_none(), "probe filter must not set until");
        assert!(f.ids.is_none(), "probe filter must not set ids");
        assert!(f.authors.is_none(), "probe filter must not set authors");
        assert!(f.search.is_none(), "probe filter must not set search");
        assert!(
            f.generic_tags.is_empty(),
            "probe filter must not set any generic_tags",
        );
    }

    /// Regression: `relay.zapstore.dev` (and likely other relays running
    /// strict filter-policy layers — zapstore's own server is built on
    /// `rely` with a custom `defender` policy) reject backfill REQs
    /// without `since` as `["CLOSED", sub_id, "filters are too vague"]`.
    /// Forcing `since=0` on every backfill page makes the time range
    /// explicitly bounded and is accepted. This test pins that
    /// invariant — if `backfill_filter` ever stops setting `since`, every
    /// run against zapstore.dev silently degrades to forward-only.
    #[test]
    fn backfill_filter_always_sets_since_zero_for_strict_relays() {
        for (until, limit) in [
            (None, 500),
            (Some(0), 500),
            (Some(1_700_000_000), 1),
            (Some(i64::MAX), u32::MAX),
        ] {
            let f = backfill_filter(&[0, 3, 10002, 30023], until, limit);
            assert_eq!(
                f.since,
                Some(Timestamp::from_secs(0)),
                "missing since=0 for backfill_filter(until={until:?}, limit={limit})"
            );
        }
    }
}
