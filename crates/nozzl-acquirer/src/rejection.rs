//! Heuristics for detecting "REQ rejected, please simplify" relay responses
//! and the related "too many concurrent subscriptions / rate limit" pressure
//! responses.
//!
//! NIP-01 leaves NOTICE / CLOSED message text completely unspecified, so the
//! cleanest signal we have is substring matching on the message body. The
//! design.md §5.D6 contract specifies filter-shape rejections as the trigger
//! for REQ-list halving; concurrency-pressure responses are tracked separately
//! because halving makes the situation strictly worse for those.

/// Concurrency / rate-limit pressure phrasings. These indicate the relay's
/// concurrent-REQ or per-window quota is exhausted, NOT that any individual
/// filter is too broad. Halving a filter on these doubles the open-sub count
/// and amplifies the pressure, so they need their own classification.
const CONCURRENT_PRESSURE_SUBSTRINGS: &[&str] = &[
    "concurrent",
    "subscriptions",
    "rate",
    "too many requests",
    // relay.damus.io's per-IP read budget: "ERROR: read bandwidth budget
    // exhausted (1048570 bytes/min per IP) (retry in 30ms)", seen on dev since
    // 2026-09-13. It matched neither list, so a backfill page it cut was
    // abandoned instead of retried (#40 (Forward-sub CLOSED path re-issues with
    // no backoff)).
    "bandwidth",
    "budget",
];

/// Substrings (case-insensitive) that indicate the relay believes our REQ was
/// too broad and is asking us to simplify.
const TRIGGER_SUBSTRINGS: &[&str] = &[
    "too many",
    "limit",
    "filter",
    "vague",
    "broad",
    "unsupported",
    "invalid",
];

/// Returns `true` if the given NOTICE or CLOSED message indicates that the
/// REQ was rejected for being too broad and should be split.
///
/// Empty messages return `false` — relays sometimes CLOSE with no reason
/// string, and we shouldn't loop-split on every disconnect.
///
/// Concurrency-pressure responses (see [`is_concurrent_pressure`]) are
/// excluded — the caller should detect those first and back off rather than
/// halving the kind list.
#[must_use]
pub fn is_rejection(message: &str) -> bool {
    if message.is_empty() {
        return false;
    }
    if is_concurrent_pressure(message) {
        return false;
    }
    let lower = message.to_ascii_lowercase();
    TRIGGER_SUBSTRINGS.iter().any(|s| lower.contains(s))
}

/// Returns `true` if the given NOTICE or CLOSED message indicates that the
/// relay is rejecting our REQ because we have too many concurrent
/// subscriptions or are hitting a rate limit — i.e. a back-off-and-retry
/// situation, not a filter-shape problem. Splitting kinds in response makes
/// concurrency pressure worse, so callers should distinguish.
#[must_use]
pub fn is_concurrent_pressure(message: &str) -> bool {
    if message.is_empty() {
        return false;
    }
    let lower = message.to_ascii_lowercase();
    CONCURRENT_PRESSURE_SUBSTRINGS
        .iter()
        .any(|s| lower.contains(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture table: each row is `(message, expected_rejection,
    /// expected_pressure)`. Mix real-relay quotes and synthetic edge cases so
    /// future maintenance can extend the table.
    const FIXTURES: &[(&str, bool, bool)] = &[
        // Filter-shape rejections — split-the-kinds case.
        ("too many filters", true, false),
        ("Too Many filters", true, false),
        ("filter rejected by policy", true, false),
        ("filters are too vague", true, false),
        ("filters too broad", true, false),
        ("invalid filter shape", true, false),
        ("unsupported filter combination", true, false),
        ("max-limit exceeded", true, false),
        ("kind filter has too many kinds", true, false),
        ("CLOSED: invalid", true, false),
        // Concurrency / rate-limit pressure — back-off-and-retry case.
        // These must NOT be classified as filter-shape rejections, because
        // halving the kind list amplifies the concurrent-REQ count.
        ("ERROR: too many concurrent REQs", false, true),
        ("ERROR: too many subscriptions", false, true),
        ("rate-limited: slow down", false, true),
        ("RATE limited", false, true),
        ("CLOSED: rate-limit applied", false, true),
        ("too many requests", false, true),
        // relay.damus.io's per-IP read budget, verbatim from dev's log.
        (
            "ERROR: read bandwidth budget exhausted (1048570 bytes/min per IP) (retry in 30ms)",
            false,
            true,
        ),
        // hashtree-cli's own relay limits, verbatim (ws_relay.rs).
        ("rate limited", false, true),
        ("too many subscriptions", false, true),
        // Negatives — unrelated NOTICE strings or empties.
        ("connection accepted", false, false),
        ("hello", false, false),
        ("subscription closed normally", false, false),
        ("", false, false),
    ];

    #[test]
    fn classification_table() {
        for (msg, rej, pressure) in FIXTURES {
            assert_eq!(
                is_rejection(msg),
                *rej,
                "is_rejection fixture failed for {msg:?}"
            );
            assert_eq!(
                is_concurrent_pressure(msg),
                *pressure,
                "is_concurrent_pressure fixture failed for {msg:?}"
            );
        }
    }

    #[test]
    fn case_insensitive() {
        assert!(is_rejection("TOO MANY filters"));
        assert!(is_rejection("Filter Bad"));
        assert!(is_rejection("FILTER bad"));
        assert!(is_concurrent_pressure("CONCURRENT subs exceeded"));
    }

    #[test]
    fn empty_is_neither() {
        assert!(!is_rejection(""));
        assert!(!is_concurrent_pressure(""));
    }

    #[test]
    fn pressure_does_not_trigger_rejection() {
        // Critical invariant: a "too many concurrent REQs" NOTICE must not
        // cascade into a kind-list halving — that would double the open subs
        // and amplify the pressure.
        assert!(!is_rejection("ERROR: too many concurrent REQs"));
        assert!(is_concurrent_pressure("ERROR: too many concurrent REQs"));
    }
}
