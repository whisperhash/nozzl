//! Validate the configs we actually ship, not synthetic fixtures.
//!
//! `check.rs` covers `--check` mode against hand-built configs. This file
//! instead loads every `config/nozzl*.toml` in the repo through the real
//! loader and the real `acquirer_config_from` mapping, so a config edit that
//! parses but is semantically wrong fails in CI rather than at deploy time.
//!
//! It also pins the shipped config's scoping policy: which relays each kind is
//! requested from. Scoping decides what nozzl acquires, and a policy that lives
//! only in a TOML comment drifts silently -- a stray `only_kinds` or
//! `only_relays` edit changes what is acquired with no test, no alert, and no
//! way to tell from the data afterwards.

use std::path::PathBuf;

use nozzl::mappings::acquirer_config_from;
use nozzl_types::RelayUrl;

fn config_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .canonicalize()
        .expect("repo config/ dir must exist")
}

fn load_env(name: &str) -> nozzl_config::Config {
    let path = config_dir().join(name);
    nozzl_config::load(&path).unwrap_or_else(|e| panic!("shipped config {name} must load: {e}"))
}

/// The shipped config, loaded and mapped.
fn shipped() -> (nozzl_config::Config, nozzl_acquirer::AcquirerConfig) {
    let cfg = load_env("nozzl.toml");
    let acq = acquirer_config_from(&cfg).expect("shipped config mapping");
    (cfg, acq)
}

/// Every `nozzl*.toml` in `config/`, discovered from the directory rather than
/// hardcoded -- a new config must not be able to escape these guards by
/// simply not being listed here.
fn shipped_env_configs() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(config_dir())
        .expect("read config/")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("nozzl") && n.ends_with(".toml"))
        .collect();
    names.sort();
    assert!(!names.is_empty(), "no nozzl*.toml found in config/");
    names
}

/// Every shipped config must load AND validate. Validation includes the
/// "kind reaches no relay" guard, so this also proves no config has a kind that
/// scoping has orphaned.
#[test]
fn every_shipped_config_loads_and_validates() {
    for name in shipped_env_configs() {
        let name = name.as_str();
        let cfg = load_env(name);
        assert!(!cfg.relays.is_empty(), "{name}: no relays");
        assert!(!cfg.kinds.is_empty(), "{name}: no kinds");
        // The mapping re-parses every relay URL and every `only_relays` entry;
        // a URL that validates as a string but not as a RelayUrl dies here.
        acquirer_config_from(&cfg).unwrap_or_else(|e| panic!("{name}: mapping failed: {e}"));
    }
}

/// The GammaMarkets marketplace kinds are acquired from every relay whose own
/// scoping admits them, relay.plebeian.market among them. They are an ecosystem
/// spec (Shopstr, Cypher, Plebeian Market and Conduit) that clients publish to
/// general-purpose relays, so pinning them to one marketplace's relay with
/// `only_relays` would cut coverage to a fraction. Re-adding such a pin, or an
/// unscoped relay growing an `only_kinds` that excludes them, fails here.
#[test]
fn marketplace_kinds_are_acquired_from_every_relay_that_carries_them() {
    let (cfg, acq) = shipped();
    let plebeian = RelayUrl::parse("wss://relay.plebeian.market").expect("valid url");

    for kind in [30402u16, 30405, 30406] {
        // Derived, not hardcoded: every relay that admits this kind, which is
        // every unscoped relay plus every scoped relay whose `only_kinds` names
        // it. A new unscoped relay, or a new scoped one like nostr.mom, is
        // covered automatically -- this asserts "nothing is pinned away", not a
        // frozen relay list.
        let mut expected: Vec<String> = cfg
            .relays
            .iter()
            .filter(|r| {
                r.only_kinds
                    .as_ref()
                    .is_none_or(|only| only.contains(&kind))
            })
            .map(|r| {
                RelayUrl::parse(&r.url)
                    .expect("valid url")
                    .as_str()
                    .to_owned()
            })
            .collect();
        expected.sort();
        assert!(
            expected.len() >= 5,
            "kind-{kind}: expected the 3 unscoped public relays + plebeian + \
             nostr.mom, got {expected:?}"
        );
        assert!(
            expected.contains(&plebeian.as_str().to_owned()),
            "kind-{kind}: the Plebeian relay must still admit it"
        );
        assert_eq!(
            sources_of(&acq, kind),
            expected,
            "kind-{kind} must be acquired from every relay whose own scoping \
             admits it -- no kind may be pinned away via only_relays"
        );
    }

    // The Plebeian relay itself stays scoped: it must not be asked for every
    // configured kind.
    let mut effective = acq.effective_kinds_for(&plebeian);
    effective.sort_unstable();
    assert_eq!(
        effective,
        vec![30402, 30405, 30406],
        "the Plebeian relay's effective kind set must be exactly the marketplace kinds"
    );
}

/// Relays added for a narrow purpose must stay scoped. The failure that matters
/// is `only_kinds` **disappearing** -- a one-line deletion turns the relay into
/// a firehose source for every configured kind, with no error, no alert, and
/// nothing in the data to say it happened. Dropping one of these relays'
/// `only_kinds` fails this test even where every other guard here derives its
/// expectations from the config, and an unscoped relay simply joins the
/// unscoped set.
///
/// Deliberately asserts the *reason* rather than the exact kind list: adding a
/// fourth marketplace kind is a legitimate edit that should not require a
/// mechanical test update, whereas any of the conditions below going false is a
/// real regression.
///
/// This guards the repo, not the running process -- there is no runtime check on
/// a relay's live subscription set.
#[test]
fn narrow_purpose_relays_stay_scoped() {
    /// Relays added to widen coverage of a few specific kinds, never as a
    /// general source.
    const NARROW_PURPOSE: &[&str] = &[
        "wss://nostr.mom",
        "wss://relay.conduit.market",
        "wss://relay.snort.social",
    ];
    /// Kinds whose volume makes them ruinous to pull from a relay added for a
    /// narrow purpose. Not exhaustive -- a floor, not a whitelist.
    const FIREHOSE_KINDS: &[u16] = &[0, 1, 3, 5, 7, 9735];
    /// A narrow-purpose relay serving more than this many kinds has lost its
    /// scoping, whatever the exact list is.
    const MAX_KINDS: usize = 5;

    let (cfg, acq) = shipped();
    for url in NARROW_PURPOSE {
        let entry = cfg
            .relays
            .iter()
            .find(|r| r.url == *url)
            .unwrap_or_else(|| panic!("{url} must be configured"));
        assert!(
            entry.only_kinds.is_some(),
            "{url} was added for a narrow purpose and must stay scoped; \
             without only_kinds it becomes a source for every configured kind"
        );

        let effective = acq.effective_kinds_for(&RelayUrl::parse(url).expect("valid url"));
        assert!(
            effective.len() <= MAX_KINDS,
            "{url} is scoped for a narrow purpose but resolves to {} kinds \
             ({effective:?}); that is a general source, not a narrow one",
            effective.len()
        );
        let firehose: Vec<u16> = effective
            .iter()
            .copied()
            .filter(|k| FIREHOSE_KINDS.contains(k))
            .collect();
        assert!(
            firehose.is_empty(),
            "{url} must never be asked for high-volume kinds, got {firehose:?}"
        );
    }
}

/// relay.fountain.fm, the Fountain (podcast and audio) app relay, carries much
/// general traffic and is asked for kind-9735 (zap-receipts) alone. Unscoped, it
/// would be asked for every configured kind.
#[test]
fn fountain_relay_carries_only_zap_receipts() {
    let (_, acq) = shipped();
    let fountain = RelayUrl::parse("wss://relay.fountain.fm").expect("valid url");
    assert_eq!(
        acq.effective_kinds_for(&fountain),
        vec![9735],
        "relay.fountain.fm must be asked for kind-9735 (zap-receipts) and nothing else"
    );
}

/// The GRASP servers -- git hosts running ngit-grasp, which keep the events
/// connected to the repositories they host -- are asked for exactly the
/// git-connected kinds: the repository kinds and the social kinds the /git
/// surface counts. An unscoped one would be asked for every configured kind, and
/// its forward sub's `min(since)` would follow the sparsest; a kind-1 (posts) or
/// kind-5 (event-deletion-requests) added to one would be a firehose with no
/// error either.
#[test]
fn grasp_relays_carry_exactly_the_git_connected_kinds() {
    let (cfg, acq) = shipped();
    let mut want = GIT_CONNECTED_KINDS.to_vec();
    want.sort_unstable();
    for url in GRASP_RELAYS {
        let entry = cfg
            .relays
            .iter()
            .find(|r| r.url == url)
            .unwrap_or_else(|| panic!("{url} must be configured"));
        assert!(
            entry.only_kinds.is_some(),
            "{url} is a git host and must stay scoped"
        );
        let mut effective = acq.effective_kinds_for(&RelayUrl::parse(url).expect("valid url"));
        effective.sort_unstable();
        assert_eq!(
            effective, want,
            "{url} must be asked for exactly the git-connected kinds (GIT_CONNECTED_KINDS)"
        );
    }
}

/// relay.ditto.pub and relay.nostr.net are general relays with deep history,
/// asked for the git repository kinds and kind-17 (external-content-reactions)
/// only. A general onboarding of either would change this test on purpose. Until
/// then, either relay losing its `only_kinds` (or growing a social kind such as
/// kind-1111 (comments)) would turn a narrow scope into a backfill of a large
/// relay's whole history.
#[test]
fn general_git_relays_carry_exactly_their_kinds() {
    let (_, acq) = shipped();
    for url in GENERAL_GIT_RELAYS {
        let mut effective = acq.effective_kinds_for(&RelayUrl::parse(url).expect("valid url"));
        effective.sort_unstable();
        assert_eq!(
            effective,
            vec![EXTERNAL_CONTENT_REACTIONS, 10317, REPO_ANNOUNCEMENTS, 30618],
            "{url} must be asked for exactly kind-10317 (grasp-server-lists), \
             kind-30617 (repo-announcements), kind-30618 (repo-states) and \
             kind-17 (external-content-reactions)"
        );
    }
}

/// nostr.mom, a general-purpose strfry relay, is asked for the marketplace kinds
/// and kind-17 (external-content-reactions), and pinned exactly:
/// `narrow_purpose_relays_stay_scoped` allows a narrow relay up to five kinds, so
/// at four a stray kind such as kind-30023 (long-form articles) would pass every
/// other test.
#[test]
fn nostr_mom_relay_carries_exactly_its_kinds() {
    let (_, acq) = shipped();
    let mom = RelayUrl::parse("wss://nostr.mom").expect("valid url");
    let mut effective = acq.effective_kinds_for(&mom);
    effective.sort_unstable();
    assert_eq!(
        effective,
        vec![EXTERNAL_CONTENT_REACTIONS, 30402, 30405, 30406],
        "nostr.mom must be asked for the marketplace kinds and \
         kind-17 (external-content-reactions), and nothing else"
    );
}

/// kind-17 (external-content-reactions), Iris Git's repository stars among them
/// (`k=git-repo`), comes from exactly these relays:
/// - the unscoped public relays, where new web stars land;
/// - relay.ditto.pub, nostr.mom and relay.nostr.net, which keep older stars;
/// - relay.snort.social, for new stars live, since it keeps only hours.
///
/// Adding a source should have to touch this test.
#[test]
fn external_content_reactions_come_from_exactly_the_chosen_relays() {
    let (cfg, acq) = shipped();
    let entry = cfg
        .kinds
        .iter()
        .find(|k| k.k == EXTERNAL_CONTENT_REACTIONS)
        .expect("kind-17 (external-content-reactions) must be configured");
    assert_eq!(entry.slug, "external-content-reactions");
    assert!(
        entry.only_relays.is_none(),
        "kind-17 (external-content-reactions) is not pinned: the unscoped public relays \
         must keep taking it, since new Iris Git web stars land there"
    );
    assert_eq!(
        sources_of(&acq, EXTERNAL_CONTENT_REACTIONS),
        sorted_urls(&EXTERNAL_CONTENT_REACTION_RELAYS),
        "kind-17 (external-content-reactions) must be acquired from exactly \
         EXTERNAL_CONTENT_REACTION_RELAYS"
    );

    // relay.snort.social is in-memory and keeps hours. It is here for live stars
    // alone; any other kind would make it a source of whatever it happens to hold.
    let snort = RelayUrl::parse("wss://relay.snort.social").expect("valid url");
    assert_eq!(
        acq.effective_kinds_for(&snort),
        vec![EXTERNAL_CONTENT_REACTIONS],
        "relay.snort.social must be asked for kind-17 (external-content-reactions) and \
         nothing else"
    );
}

/// index.ngit.dev, the ngit-indexer that crawls the git relays, is asked for the
/// three kinds /git reads from it: kind-30617 (repo-announcements),
/// kind-10317 (grasp-server-lists) and kind-5 (event-deletion-requests). It
/// stores other kinds too, and could store any kind it accepts later, so the set
/// is pinned exactly: without `only_kinds` it would be asked for every configured
/// kind. kind-5 (event-deletion-requests) is a firehose elsewhere, which is why
/// this relay is not in `narrow_purpose_relays_stay_scoped`.
#[test]
fn index_ngit_dev_carries_exactly_the_announcement_kinds() {
    let (cfg, acq) = shipped();
    let url = "wss://index.ngit.dev";
    let entry = cfg
        .relays
        .iter()
        .find(|r| r.url == url)
        .unwrap_or_else(|| panic!("{url} must be configured"));
    assert!(
        entry.only_kinds.is_some(),
        "{url} is asked for three kinds and must stay scoped"
    );
    let mut effective = acq.effective_kinds_for(&RelayUrl::parse(url).expect("valid url"));
    effective.sort_unstable();
    assert_eq!(
        effective,
        vec![5, 10317, 30617],
        "{url} must be asked for exactly kind-5 (event-deletion-requests), \
         kind-10317 (grasp-server-lists) and kind-30617 (repo-announcements)"
    );
}

/// relay.zapstore.dev is the app store's own relay, asked for the Zapstore kinds
/// and the two cross-domain kinds the Zapstore domain reads there:
/// kind-5 (event-deletion-requests) and kind-9735 (zap-receipts). Both are
/// firehoses elsewhere, and without `only_kinds` this relay would be asked for
/// every configured kind.
#[test]
fn zapstore_relay_carries_exactly_the_zapstore_kinds() {
    let (cfg, acq) = shipped();
    let url = "wss://relay.zapstore.dev";
    let entry = cfg
        .relays
        .iter()
        .find(|r| r.url == url)
        .unwrap_or_else(|| panic!("{url} must be configured"));
    assert!(entry.only_kinds.is_some(), "{url} must stay scoped");
    let mut effective = acq.effective_kinds_for(&RelayUrl::parse(url).expect("valid url"));
    effective.sort_unstable();
    assert_eq!(
        effective,
        vec![5, 3063, 9735, 30063, 30267, 32267],
        "{url} must be asked for exactly kind-5 (event-deletion-requests), \
         kind-3063 (zapstore-asset-metadata), kind-9735 (zap-receipts), \
         kind-30063 (zapstore-app-releases), kind-30267 (zapstore-app-stacks) and \
         kind-32267 (zapstore-app-catalog)"
    );
}

/// A scoped relay never names a kind whose `only_relays` leaves it out. The two
/// compose as an intersection, so such an entry is dead: the relay is not asked
/// for the kind, and the line reads as if it were. It is the easy mistake with a
/// pinned kind: adding the kind to a relay's `only_kinds` does nothing until the
/// kind's `only_relays` names the relay too.
#[test]
fn scoped_relays_name_no_kind_pinned_away_from_them() {
    let (cfg, _) = shipped();
    for relay in &cfg.relays {
        let Some(only_kinds) = &relay.only_kinds else {
            continue;
        };
        let url = RelayUrl::parse(&relay.url).expect("valid url");
        let dead: Vec<u16> = only_kinds
            .iter()
            .copied()
            .filter(|k| {
                cfg.kinds.iter().any(|entry| {
                    entry.k == *k
                        && entry.only_relays.as_ref().is_some_and(|only| {
                            !only
                                .iter()
                                .any(|u| RelayUrl::parse(u).expect("valid url") == url)
                        })
                })
            })
            .collect();
        assert!(
            dead.is_empty(),
            "{} names {dead:?} in only_kinds, but those kinds' only_relays leave it out",
            relay.url
        );
    }
}

/// The unscoped relays request every kind that is not explicitly pinned away
/// from them. This is the regression guard for `only_relays` being read as
/// "restrict everything": the expected set for each unscoped relay is every
/// configured kind minus the kinds whose `only_relays` does not name it, and any
/// future pin narrows it by exactly that kind.
#[test]
fn unscoped_relays_keep_every_unpinned_kind() {
    let (cfg, acq) = shipped();

    // A kind is pinned away from `url` when its `only_relays` exists and does not
    // name it. Read from the raw config, so the acquirer's mapping is checked
    // against the file rather than against itself.
    let pinned_away_from = |url: &RelayUrl| -> Vec<u16> {
        cfg.kinds
            .iter()
            .filter(|k| {
                k.only_relays.as_ref().is_some_and(|only| {
                    !only
                        .iter()
                        .any(|u| RelayUrl::parse(u).expect("valid url") == *url)
                })
            })
            .map(|k| k.k)
            .collect()
    };

    // Derived, not hardcoded: "every relay without its own only_kinds". A
    // fourth unscoped relay is then covered automatically.
    let unscoped: Vec<&str> = cfg
        .relays
        .iter()
        .filter(|r| r.only_kinds.is_none())
        .map(|r| r.url.as_str())
        .collect();
    assert!(
        unscoped.len() >= 3,
        "expected the public relays to be unscoped, got {unscoped:?}"
    );
    for url in unscoped {
        let relay = RelayUrl::parse(url).expect("valid url");
        let pinned_away = pinned_away_from(&relay);
        let expected: Vec<u16> = acq
            .kinds
            .iter()
            .copied()
            .filter(|k| !pinned_away.contains(k))
            .collect();
        assert_eq!(
            acq.effective_kinds_for(&relay),
            expected,
            "{url} must request every kind that is not pinned elsewhere"
        );
    }
}

/// Every configured relay that is asked for `kind`, normalised and sorted.
fn sources_of(acq: &nozzl_acquirer::AcquirerConfig, kind: u16) -> Vec<String> {
    let mut sources: Vec<String> = acq
        .relays
        .iter()
        .filter(|r| acq.effective_kinds_for(r).contains(&kind))
        .map(|r| r.as_str().to_owned())
        .collect();
    sources.sort();
    sources
}

/// `urls` normalised the way the acquirer stores them, and sorted.
fn sorted_urls(urls: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = urls
        .iter()
        .map(|u| RelayUrl::parse(u).expect("valid url").as_str().to_owned())
        .collect();
    out.sort();
    out
}

/// kind-17 (external-content-reactions).
const EXTERNAL_CONTENT_REACTIONS: u16 = 17;

/// kind-30617 (repo-announcements).
const REPO_ANNOUNCEMENTS: u16 = 30617;

/// The relays kind-17 (external-content-reactions) is acquired from.
const EXTERNAL_CONTENT_REACTION_RELAYS: [&str; 7] = [
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://relay.primal.net",
    "wss://relay.ditto.pub",
    "wss://nostr.mom",
    "wss://relay.snort.social",
    "wss://relay.nostr.net",
];

/// The GRASP servers: relay.ngit.dev, the reference server, and four git hosts.
const GRASP_RELAYS: [&str; 5] = [
    "wss://relay.ngit.dev",
    "wss://gitnostr.com",
    "wss://git.shakespeare.diy",
    "wss://ngit.danconwaydev.com",
    "wss://grasp.budabit.club",
];

/// The general relays asked for the git repository kinds.
const GENERAL_GIT_RELAYS: [&str; 2] = ["wss://relay.ditto.pub", "wss://relay.nostr.net"];

/// The kinds a GRASP server is asked for: the repository kinds
/// kind-10317 (grasp-server-lists), kind-30617 (repo-announcements) and
/// kind-30618 (repo-states), and the social kinds /git counts,
/// kind-1111 (comments), kind-7 (reactions) and kind-9735 (zap-receipts).
const GIT_CONNECTED_KINDS: [u16; 6] = [10317, 30617, 30618, 1111, 7, 9735];
