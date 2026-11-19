//! [`AcquirerConfig`] and the pure-function diff used by `apply_config`.
//!
//! The diff is split out so it can be unit-tested independently of the
//! relay-pool I/O. See `docs/crates/nozzl-acquirer.md` for the semantics.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use nozzl_types::RelayUrl;

/// Top-level configuration for the [`crate::Acquirer`].
///
/// `relays` and `kinds` are the two hot-reloadable lists. `backfill_safety_margin`,
/// `backfill_default_limit`, and `backfill_empty_pages_to_complete` are not
/// currently surfaced to `apply_config` — they're read once at spawn and are
/// not meaningful to change at runtime (changing the safety margin doesn't
/// retroactively reopen subs; changing the empty-pages threshold mid-backfill
/// would race with in-flight pages).
///
/// `kind_slugs` carries the current `kind → slug` mapping. Every entry in
/// `kinds` MUST have a corresponding entry in `kind_slugs`; the binary's
/// config-mapping enforces this invariant. The slug is attached to each
/// outbound `EventEnvelope` so that hot-reloads of the slug map cannot
/// retroactively rewrite paths for events already in flight.
#[derive(Clone, Debug)]
pub struct AcquirerConfig {
    pub relays: Vec<RelayUrl>,
    pub kinds: Vec<u16>,
    pub kind_slugs: HashMap<u16, String>,
    /// Optional per-relay kind restriction. When a relay has an entry here,
    /// only those kinds are requested from it (intersected with `kinds`).
    /// When absent for a relay, all configured `kinds` are requested.
    ///
    /// Applied at `Acquirer::spawn`, on the relay-add path, and on reload:
    /// `diff` recomputes every surviving relay's effective kind set from this
    /// map, so an edit here reaches running relay tasks via `UpdateKinds`.
    pub relay_only_kinds: HashMap<RelayUrl, Vec<u16>>,
    /// Optional per-kind relay restriction - the inverse of
    /// `relay_only_kinds`. When a kind has an entry here, it is requested only
    /// from those relays. When absent for a kind, it is requested from every
    /// configured relay. Composed with `relay_only_kinds` as an intersection by
    /// [`AcquirerConfig::effective_kinds_for`].
    ///
    /// Same lifecycle as `relay_only_kinds`: applied at spawn, on relay-add,
    /// and on reload through the per-relay effective-set diff.
    pub kind_only_relays: HashMap<u16, HashSet<RelayUrl>>,
    pub backfill_safety_margin: Duration,
    pub backfill_default_limit: u32,
    /// Number of consecutive zero-event backfill pages required before a
    /// relay's history is declared complete. See the field of the same
    /// name on `nozzl_config::AcquirerConfig`.
    pub backfill_empty_pages_to_complete: u32,
    /// LRU capacity for the pool's in-memory event-id dedup tracker. Only
    /// read at `Acquirer::spawn`; runtime changes are ignored.
    pub pool_dedup_max_events: usize,
    /// How long a forward subscription may go without delivering any event
    /// before the acquirer probes the relay's subscription-dispatch path.
    /// Triggers a subscription-dispatch liveness probe (not the asymmetric
    /// re-issue that the previous watchdog did). Setting to zero disables
    /// the probe entirely. See the design discussion in
    /// `docs/crates/nozzl-acquirer.md` and the dispatch-liveness
    /// probe plan.
    pub forward_silence_threshold: Duration,
    /// How long to wait for a subscription-dispatch probe to settle (EOSE
    /// / EVENT / CLOSED-for-probe) before declaring it timed-out and
    /// forcing a relay reconnect. Default: 5 s (matches NDK).
    pub probe_timeout: Duration,
    /// Floor between successive probe attempts against the same relay,
    /// anchored on the issue time (not settlement). Prevents a tight
    /// probe-timeout → reconnect → re-probe loop and caps probe rate at
    /// `~ 3600 / probe_min_interval.as_secs()` per hour per relay.
    /// Default: 60 s.
    pub probe_min_interval: Duration,
}

impl AcquirerConfig {
    /// Compute the effective kind list for a given relay: the global `kinds`
    /// set narrowed by **both** scoping directions.
    ///
    /// A kind reaches this relay only if
    ///   1. the relay admits the kind - `relay_only_kinds[url]` contains it, or
    ///      the relay has no `only_kinds` restriction at all; **and**
    ///   2. the kind admits the relay - `kind_only_relays[k]` contains `url`,
    ///      or the kind has no `only_relays` restriction at all.
    ///
    /// With neither restriction present this returns `kinds` verbatim, which is
    /// the historical default for every unscoped relay.
    ///
    /// This is the single choke point for kind scoping: relay-task spawn,
    /// `UpdateKinds` on hot-reload, relay-add, and the `kind_subscribed` metric
    /// all route through it, so both allowlists take effect everywhere at once.
    ///
    /// The returned vector is in the same order as `self.kinds` so callers
    /// don't need to re-sort.
    #[must_use]
    pub fn effective_kinds_for(&self, url: &RelayUrl) -> Vec<u16> {
        let relay_allowed: Option<HashSet<u16>> = self
            .relay_only_kinds
            .get(url)
            .map(|only| only.iter().copied().collect());
        self.kinds
            .iter()
            .copied()
            .filter(|k| relay_allowed.as_ref().is_none_or(|a| a.contains(k)))
            .filter(|k| {
                self.kind_only_relays
                    .get(k)
                    .is_none_or(|relays| relays.contains(url))
            })
            .collect()
    }
}

/// Difference between a previous and a new [`AcquirerConfig`].
///
/// Produced by [`AcquirerConfig::diff`] and consumed by the acquirer's
/// hot-reload code path.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ActionPlan {
    /// Relays that should be added (and start with a fresh full-kind backfill).
    pub relays_added: Vec<RelayUrl>,
    /// Relays that should be torn down.
    pub relays_removed: Vec<RelayUrl>,
    /// Kinds that should be added — backfill scoped to these is triggered for
    /// every relay (including the existing ones); forward sub re-issued.
    pub kinds_added: Vec<u16>,
    /// Kinds that were removed; forward sub re-issued without them.
    pub kinds_removed: Vec<u16>,
    /// Set of kinds in the new config (so callers don't re-derive it).
    pub new_kinds: Vec<u16>,
    /// `true` if the kind set changed at all (added or removed).
    pub kind_set_changed: bool,
    /// Per-surviving-relay kind-set transitions, computed through
    /// [`AcquirerConfig::effective_kinds_for`] so both scoping directions are
    /// already applied. Only relays whose effective set actually **changed**
    /// appear here.
    ///
    /// This replaces "the global kind set changed, so re-issue on every relay"
    /// as the driver for `UpdateKinds`. That global flag answered a different
    /// question than the one that matters: a kind pinned to one relay changes
    /// the kind set globally while leaving every *other* relay's effective set
    /// identical, and re-issuing there is pure harm -- it tears down and
    /// replays a working forward sub and discards NOTICE-learned REQ-split
    /// state, for no change in what is requested.
    pub relay_kind_updates: Vec<RelayKindUpdate>,
}

/// One surviving relay's effective-kind-set transition across a reload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayKindUpdate {
    pub url: RelayUrl,
    /// The relay's new effective kind set (both allowlists applied).
    pub effective: Vec<u16>,
    /// Kinds this relay did NOT have before and does now.
    ///
    /// These drive the scoped backfill. Deriving it from the effective-set
    /// delta rather than from the global `kinds_added` is what guarantees a
    /// newly-gained kind always gets a backfill: a kind can reach a relay for
    /// the first time either by being added globally *or* by an allowlist
    /// edit, and only the former is in `kinds_added`. Without a backfill the
    /// kind has no checkpoint, which makes the relay's forward-sub `min(since)`
    /// `None` -- an unbounded REQ across every kind on that relay.
    pub gained: Vec<u16>,
}

impl AcquirerConfig {
    /// Compute the changes needed to transition from `old` to `new`.
    ///
    /// Pure function: no I/O, no allocations beyond the result. Output is
    /// deterministic — vectors are sorted (relay URLs lexicographically by
    /// string form, kinds by u16 ascending) so unit tests can assert exact
    /// equality.
    #[must_use]
    pub fn diff(old: &Self, new: &Self) -> ActionPlan {
        // RelayUrl is Hash + Eq but not Ord, so use HashSet for membership and
        // sort the output Vec by string form for determinism.
        let old_relays: HashSet<&RelayUrl> = old.relays.iter().collect();
        let new_relays: HashSet<&RelayUrl> = new.relays.iter().collect();

        let mut relays_added: Vec<RelayUrl> = new_relays
            .difference(&old_relays)
            .map(|r| (*r).clone())
            .collect();
        relays_added.sort_by(|a: &RelayUrl, b: &RelayUrl| a.as_str().cmp(b.as_str()));

        let mut relays_removed: Vec<RelayUrl> = old_relays
            .difference(&new_relays)
            .map(|r| (*r).clone())
            .collect();
        relays_removed.sort_by(|a: &RelayUrl, b: &RelayUrl| a.as_str().cmp(b.as_str()));

        let old_kinds: BTreeSet<u16> = old.kinds.iter().copied().collect();
        let new_kinds_set: BTreeSet<u16> = new.kinds.iter().copied().collect();

        let kinds_added: Vec<u16> = new_kinds_set.difference(&old_kinds).copied().collect();
        let kinds_removed: Vec<u16> = old_kinds.difference(&new_kinds_set).copied().collect();
        let kind_set_changed = !kinds_added.is_empty() || !kinds_removed.is_empty();
        let new_kinds: Vec<u16> = new_kinds_set.into_iter().collect();

        // Per-surviving-relay effective-set transitions. Computed for every
        // relay present in BOTH configs; relays being added take the
        // relay-add path instead, and removed ones are torn down.
        let mut relay_kind_updates: Vec<RelayKindUpdate> = new
            .relays
            .iter()
            .filter(|r| old_relays.contains(r))
            .filter_map(|url| {
                let before = old.effective_kinds_for(url);
                let after = new.effective_kinds_for(url);
                if before == after {
                    return None;
                }
                let had: HashSet<u16> = before.iter().copied().collect();
                let gained: Vec<u16> = after.iter().copied().filter(|k| !had.contains(k)).collect();
                Some(RelayKindUpdate {
                    url: url.clone(),
                    effective: after,
                    gained,
                })
            })
            .collect();
        relay_kind_updates.sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));

        ActionPlan {
            relays_added,
            relays_removed,
            kinds_added,
            kinds_removed,
            new_kinds,
            kind_set_changed,
            relay_kind_updates,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay(s: &str) -> RelayUrl {
        RelayUrl::parse(s).unwrap()
    }

    fn cfg(relays: &[&str], kinds: &[u16]) -> AcquirerConfig {
        let kind_slugs: HashMap<u16, String> =
            kinds.iter().map(|k| (*k, format!("k{k}-slug"))).collect();
        AcquirerConfig {
            relays: relays.iter().map(|s| relay(s)).collect(),
            kinds: kinds.to_vec(),
            kind_slugs,
            relay_only_kinds: HashMap::new(),
            kind_only_relays: HashMap::new(),
            backfill_safety_margin: Duration::from_secs(60),
            backfill_default_limit: 500,
            backfill_empty_pages_to_complete: 8,
            pool_dedup_max_events: 5_000_000,
            forward_silence_threshold: Duration::from_secs(60),
            probe_timeout: Duration::from_secs(5),
            probe_min_interval: Duration::from_secs(60),
        }
    }

    #[test]
    fn diff_empty_to_empty_is_noop() {
        let a = cfg(&[], &[]);
        let b = cfg(&[], &[]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert!(plan.relays_added.is_empty());
        assert!(plan.relays_removed.is_empty());
        assert!(plan.kinds_added.is_empty());
        assert!(plan.kinds_removed.is_empty());
        assert!(!plan.kind_set_changed);
        assert!(plan.new_kinds.is_empty());
    }

    #[test]
    fn diff_add_relay() {
        let a = cfg(&["wss://a"], &[0, 3]);
        let b = cfg(&["wss://a", "wss://b"], &[0, 3]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.relays_added, vec![relay("wss://b")]);
        assert!(plan.relays_removed.is_empty());
        assert!(plan.kinds_added.is_empty());
        assert!(plan.kinds_removed.is_empty());
        assert!(!plan.kind_set_changed);
    }

    #[test]
    fn diff_remove_relay() {
        let a = cfg(&["wss://a", "wss://b"], &[0]);
        let b = cfg(&["wss://a"], &[0]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.relays_removed, vec![relay("wss://b")]);
        assert!(plan.relays_added.is_empty());
    }

    #[test]
    fn diff_add_kind() {
        let a = cfg(&["wss://a"], &[0, 3]);
        let b = cfg(&["wss://a"], &[0, 3, 10002]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.kinds_added, vec![10002]);
        assert!(plan.kinds_removed.is_empty());
        assert!(plan.kind_set_changed);
        assert_eq!(plan.new_kinds, vec![0, 3, 10002]);
    }

    #[test]
    fn diff_remove_kind() {
        let a = cfg(&["wss://a"], &[0, 3, 10002]);
        let b = cfg(&["wss://a"], &[0, 3]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.kinds_removed, vec![10002]);
        assert!(plan.kinds_added.is_empty());
        assert!(plan.kind_set_changed);
    }

    #[test]
    fn diff_simultaneous_add_and_remove_relay() {
        let a = cfg(&["wss://a", "wss://b"], &[0]);
        let b = cfg(&["wss://b", "wss://c"], &[0]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.relays_added, vec![relay("wss://c")]);
        assert_eq!(plan.relays_removed, vec![relay("wss://a")]);
    }

    #[test]
    fn diff_add_remove_kind_simultaneously() {
        let a = cfg(&["wss://a"], &[0, 3]);
        let b = cfg(&["wss://a"], &[3, 10002]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.kinds_added, vec![10002]);
        assert_eq!(plan.kinds_removed, vec![0]);
        assert!(plan.kind_set_changed);
    }

    #[test]
    fn diff_relay_change_with_no_kind_change_is_kind_set_unchanged() {
        let a = cfg(&["wss://a"], &[0]);
        let b = cfg(&["wss://b"], &[0]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert!(!plan.kind_set_changed);
        assert_eq!(plan.kinds_added.len(), 0);
        assert_eq!(plan.kinds_removed.len(), 0);
    }

    #[test]
    fn diff_dedupes_repeated_input_kinds_and_relays() {
        let a = cfg(&[], &[]);
        let mut kind_slugs = HashMap::new();
        kind_slugs.insert(0u16, "zero".to_string());
        kind_slugs.insert(3u16, "three".to_string());
        let b = AcquirerConfig {
            relays: vec![relay("wss://a"), relay("wss://a")],
            kinds: vec![0, 0, 3],
            kind_slugs,
            relay_only_kinds: HashMap::new(),
            kind_only_relays: HashMap::new(),
            backfill_safety_margin: Duration::from_secs(60),
            backfill_default_limit: 500,
            backfill_empty_pages_to_complete: 8,
            pool_dedup_max_events: 5_000_000,
            forward_silence_threshold: Duration::from_secs(60),
            probe_timeout: Duration::from_secs(5),
            probe_min_interval: Duration::from_secs(60),
        };
        let plan = AcquirerConfig::diff(&a, &b);
        // Set semantics: one entry per unique relay/kind.
        assert_eq!(plan.relays_added, vec![relay("wss://a")]);
        assert_eq!(plan.kinds_added, vec![0, 3]);
    }

    #[test]
    fn diff_relays_added_sorted_lexicographically() {
        let a = cfg(&[], &[]);
        let b = cfg(&["wss://c", "wss://a", "wss://b"], &[]);
        let plan = AcquirerConfig::diff(&a, &b);
        let urls: Vec<&str> = plan.relays_added.iter().map(RelayUrl::as_str).collect();
        // url::Url normalizes trailing slash, so the strings end with "/".
        assert_eq!(urls, vec!["wss://a/", "wss://b/", "wss://c/"]);
    }

    #[test]
    fn diff_kinds_added_sorted_ascending() {
        let a = cfg(&[], &[]);
        let b = cfg(&[], &[30063, 0, 10002, 3]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.kinds_added, vec![0, 3, 10002, 30063]);
    }

    #[test]
    fn effective_kinds_unrestricted_returns_global() {
        let c = cfg(&["wss://a"], &[0, 3, 30063]);
        assert_eq!(c.effective_kinds_for(&relay("wss://a")), vec![0, 3, 30063]);
    }

    #[test]
    fn effective_kinds_restricted_intersects_with_global() {
        let mut c = cfg(&["wss://zapstore.dev"], &[0, 3, 30063, 30267, 32267]);
        c.relay_only_kinds
            .insert(relay("wss://zapstore.dev"), vec![30063, 32267]);
        // Returned in `kinds`-order (not `only_kinds`-order) so callers
        // don't have to re-sort and the pubkey-shard rollout behavior is
        // deterministic.
        assert_eq!(
            c.effective_kinds_for(&relay("wss://zapstore.dev")),
            vec![30063, 32267]
        );
    }

    // ---- per-kind `only_relays`: the inverse restriction, and how the two
    // ---- scoping directions compose inside effective_kinds_for.

    // ---- the per-relay effective-set diff that drives UpdateKinds ----

    #[test]
    fn diff_pinned_kind_onboarding_is_a_no_op_for_every_other_relay() {
        // The load-bearing case: onboarding a kind pinned to one relay changes
        // the GLOBAL kind set, but must leave every other relay untouched so
        // its forward sub is never torn down and replayed.
        let a = cfg(&["wss://general", "wss://dedicated"], &[0, 3]);
        let mut b = cfg(&["wss://general", "wss://dedicated"], &[0, 3, 30402]);
        b.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://dedicated")]));
        let plan = AcquirerConfig::diff(&a, &b);

        assert!(plan.kind_set_changed, "the global kind set did change");
        let touched: Vec<&str> = plan
            .relay_kind_updates
            .iter()
            .map(|u| u.url.as_str())
            .collect();
        assert_eq!(
            touched,
            vec!["wss://dedicated/"],
            "only the pinned relay may be re-issued"
        );
        assert_eq!(plan.relay_kind_updates[0].gained, vec![30402]);
        assert_eq!(plan.relay_kind_updates[0].effective, vec![0, 3, 30402]);
    }

    #[test]
    fn diff_widening_only_relays_backfills_the_relay_that_gains_the_kind() {
        // Regression: `gained` must come from the effective-set delta, not from
        // `kinds_added`. Here NO kind is added globally - 30402 merely becomes
        // visible to a second relay. If `gained` were derived from
        // `kinds_added` it would be empty, the relay would get the kind in its
        // forward group with no checkpoint and no backfill, and its
        // `min(since)` would collapse to None -> an unbounded REQ.
        let mut a = cfg(&["wss://general", "wss://dedicated"], &[0, 30402]);
        a.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://dedicated")]));
        let mut b = a.clone();
        b.kind_only_relays.insert(
            30402,
            HashSet::from([relay("wss://dedicated"), relay("wss://general")]),
        );
        let plan = AcquirerConfig::diff(&a, &b);

        assert!(
            !plan.kind_set_changed,
            "no kind was added or removed globally"
        );
        assert_eq!(plan.relay_kind_updates.len(), 1);
        assert_eq!(plan.relay_kind_updates[0].url.as_str(), "wss://general/");
        assert_eq!(
            plan.relay_kind_updates[0].gained,
            vec![30402],
            "the relay that gained the kind must be backfilled for it"
        );
    }

    #[test]
    fn diff_narrowing_only_relays_reissues_without_the_kind_and_gains_nothing() {
        let mut a = cfg(&["wss://general", "wss://dedicated"], &[0, 30402]);
        a.kind_only_relays.insert(
            30402,
            HashSet::from([relay("wss://dedicated"), relay("wss://general")]),
        );
        let mut b = a.clone();
        b.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://dedicated")]));
        let plan = AcquirerConfig::diff(&a, &b);

        assert_eq!(plan.relay_kind_updates.len(), 1);
        let u = &plan.relay_kind_updates[0];
        assert_eq!(u.url.as_str(), "wss://general/");
        assert_eq!(u.effective, vec![0], "the kind is dropped from the relay");
        assert!(u.gained.is_empty(), "losing a kind gains nothing");
    }

    #[test]
    fn diff_only_kinds_edit_on_an_existing_relay_is_serviced_too() {
        // The same machinery covers the per-relay allowlist, which used to be
        // reported as restart-required while being silently applied whenever
        // some unrelated kind was added.
        let mut a = cfg(&["wss://scoped"], &[0, 3, 30402]);
        a.relay_only_kinds.insert(relay("wss://scoped"), vec![0]);
        let mut b = a.clone();
        b.relay_only_kinds
            .insert(relay("wss://scoped"), vec![0, 30402]);
        let plan = AcquirerConfig::diff(&a, &b);

        assert_eq!(plan.relay_kind_updates.len(), 1);
        assert_eq!(plan.relay_kind_updates[0].effective, vec![0, 30402]);
        assert_eq!(plan.relay_kind_updates[0].gained, vec![30402]);
    }

    #[test]
    fn diff_unchanged_config_touches_no_relay() {
        let a = cfg(&["wss://a", "wss://b"], &[0, 3, 30402]);
        let plan = AcquirerConfig::diff(&a, &a.clone());
        assert!(plan.relay_kind_updates.is_empty());
        assert!(!plan.kind_set_changed);
    }

    #[test]
    fn diff_added_relay_is_not_in_relay_kind_updates() {
        // A relay being added takes the relay-add path (which spawns it with
        // the right kind set); it must not also be sent an UpdateKinds.
        let a = cfg(&["wss://a"], &[0]);
        let b = cfg(&["wss://a", "wss://new"], &[0]);
        let plan = AcquirerConfig::diff(&a, &b);
        assert_eq!(plan.relays_added, vec![relay("wss://new")]);
        assert!(plan.relay_kind_updates.is_empty());
    }

    #[test]
    fn effective_kinds_pinned_kind_reaches_only_its_listed_relay() {
        // The pinned-kind shape: kind-30402 (markets-product-listings) is
        // acquired from the dedicated relay only; the general relays keep every
        // other kind and never see it. (A shape the primitive supports -- no
        // shipped config pins this kind today; see shipped_configs.rs.)
        let mut c = cfg(&["wss://general", "wss://dedicated"], &[0, 3, 30402]);
        c.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://dedicated")]));
        assert_eq!(c.effective_kinds_for(&relay("wss://general")), vec![0, 3]);
        assert_eq!(
            c.effective_kinds_for(&relay("wss://dedicated")),
            vec![0, 3, 30402]
        );
    }

    #[test]
    fn effective_kinds_unpinned_kinds_are_unaffected_by_another_kinds_pin() {
        // Pinning one kind must not narrow any other kind's relay set - the
        // "all relays" default has to survive for everything not named.
        let mut c = cfg(&["wss://a", "wss://b"], &[0, 3, 30402]);
        c.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://b")]));
        assert_eq!(c.effective_kinds_for(&relay("wss://a")), vec![0, 3]);
        assert_eq!(c.effective_kinds_for(&relay("wss://b")), vec![0, 3, 30402]);
    }

    #[test]
    fn effective_kinds_composes_only_relays_with_only_kinds() {
        // Both directions applied at once. The dedicated relay is itself
        // scoped to [30402, 30405], and 30402 is pinned to it: the result is
        // the intersection, so 0 and 3 stay off the dedicated relay while
        // 30402 stays off the general one.
        let mut c = cfg(&["wss://general", "wss://dedicated"], &[0, 3, 30402, 30405]);
        c.relay_only_kinds
            .insert(relay("wss://dedicated"), vec![30402, 30405]);
        c.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://dedicated")]));
        assert_eq!(
            c.effective_kinds_for(&relay("wss://dedicated")),
            vec![30402, 30405]
        );
        // 30405 is NOT pinned, so the general relay still requests it; 30402 is.
        assert_eq!(
            c.effective_kinds_for(&relay("wss://general")),
            vec![0, 3, 30405]
        );
    }

    #[test]
    fn effective_kinds_pin_to_multiple_relays_admits_each() {
        let mut c = cfg(&["wss://a", "wss://b", "wss://c"], &[0, 30402]);
        c.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://a"), relay("wss://b")]));
        assert_eq!(c.effective_kinds_for(&relay("wss://a")), vec![0, 30402]);
        assert_eq!(c.effective_kinds_for(&relay("wss://b")), vec![0, 30402]);
        assert_eq!(c.effective_kinds_for(&relay("wss://c")), vec![0]);
    }

    #[test]
    fn effective_kinds_contradictory_scoping_yields_empty_for_that_kind() {
        // Pathological: the kind names a relay whose own only_kinds excludes
        // it. The config layer rejects this at load time (the "reaches no
        // relay" guard), but the acquirer must still have defined behavior.
        let mut c = cfg(&["wss://a"], &[0, 30402]);
        c.relay_only_kinds.insert(relay("wss://a"), vec![0]);
        c.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://a")]));
        assert_eq!(c.effective_kinds_for(&relay("wss://a")), vec![0]);
    }

    #[test]
    fn effective_kinds_pin_naming_a_relay_not_in_the_relay_list_excludes_everywhere() {
        // Defensive: a stale pin (relay removed from relays.toml but still
        // named by a kind) must not silently fall back to "all relays".
        let mut c = cfg(&["wss://a"], &[0, 30402]);
        c.kind_only_relays
            .insert(30402, HashSet::from([relay("wss://gone")]));
        assert_eq!(c.effective_kinds_for(&relay("wss://a")), vec![0]);
    }

    #[test]
    fn effective_kinds_restriction_drops_globals_not_listed() {
        let mut c = cfg(&["wss://a"], &[0, 3, 10002]);
        // Restrict to a subset of globals.
        c.relay_only_kinds.insert(relay("wss://a"), vec![3]);
        assert_eq!(c.effective_kinds_for(&relay("wss://a")), vec![3]);
    }

    #[test]
    fn effective_kinds_restriction_to_only_unconfigured_yields_empty() {
        // Pathological: an only_kinds entry that doesn't intersect the
        // global list yields zero kinds for that relay. The config layer
        // rejects this (every only_kinds value must be in kinds.toml), but
        // we still want a defined behavior here for safety.
        let mut c = cfg(&["wss://a"], &[0, 3]);
        c.relay_only_kinds.insert(relay("wss://a"), vec![10002]);
        assert!(c.effective_kinds_for(&relay("wss://a")).is_empty());
    }

    #[test]
    fn effective_kinds_other_relays_unaffected_by_restriction() {
        let mut c = cfg(&["wss://a", "wss://b"], &[0, 3, 30063]);
        c.relay_only_kinds.insert(relay("wss://a"), vec![30063]);
        // wss://a restricted; wss://b sees the full global list.
        assert_eq!(c.effective_kinds_for(&relay("wss://a")), vec![30063]);
        assert_eq!(c.effective_kinds_for(&relay("wss://b")), vec![0, 3, 30063]);
    }
}
