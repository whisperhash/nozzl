//! The TLS crates' minimum versions, held against every copy of them in `Cargo.lock`.
//!
//! What a relay connection trusts is decided by two crates, and by no line of our code:
//!
//! - `webpki-roots` is the root set those connections compile in. 1.0.9 is the last release holding
//!   emSign ECC Root CA - G3, compromised in the 2026-10 eMudhra incident.
//! - `rustls-webpki` is the verifier. 0.103.16 added a blocklist that refuses a chain through any
//!   of that incident's 8 compromised intermediate keys, even where some store still anchors it.
//!
//! The verifier also serves nozzl's S3 connections, which go through `object_store` and trust the
//! image's own certificate store. Nothing here holds that store.
//!
//! `crates/nozzl-acquirer/Cargo.toml` requires a minimum of each, so a `--locked` build refuses a
//! lock below it. This test reads those two numbers, so they live in one place, and holds what
//! Cargo cannot see:
//!
//! - A requirement applies to one semver line only, so a dependency can bring an older line in
//!   beside it: `rustls-webpki` 0.102 has no blocklist, and `webpki-roots` 0.25 carries roots of
//!   its own. So every copy in the lock has to meet the minimum.
//! - A requirement keeps its crate in the lock by itself. If `rustls` moved to another line of the
//!   verifier, or the relay transport to other roots, it would go on holding a copy nothing uses.
//!   So the crate that uses each one has to reach it on the line its requirement names.
//! - A requirement and the lock can be lowered together, and then both of the above pass. So a
//!   minimum has to stay above the newest release the incident rules out.

use std::path::Path;

/// `(major, minor, patch)`.
type Version = (u64, u64, u64);

/// A crate this test holds to a minimum, as required by nozzl-acquirer's manifest.
struct Held {
    name: &'static str,
    /// The crate that uses it for TLS.
    used_by: &'static str,
    /// The newest release the incident rules out. If a later incident raises the minimum, this
    /// rises with it.
    ruled_out: Version,
}

static HELD: [Held; 2] = [
    Held {
        name: "webpki-roots",
        used_by: "tokio-tungstenite",
        ruled_out: (1, 0, 9),
    },
    Held {
        name: "rustls-webpki",
        used_by: "rustls",
        ruled_out: (0, 103, 15),
    },
];

fn held(name: &str) -> &'static Held {
    HELD.iter()
        .find(|held| held.name == name)
        .expect("a held crate")
}

/// `major.minor.patch` and nothing else.
fn plain_version(s: &str) -> Option<Version> {
    let parts: Vec<u64> = s
        .split('.')
        .map(|p| p.parse().ok())
        .collect::<Option<_>>()?;
    match parts[..] {
        [major, minor, patch] => Some((major, minor, patch)),
        _ => None,
    }
}

/// A version in the lock. A `-prerelease` or `+build` suffix is not a version part, so a prerelease
/// of a minimum reads as that minimum. It cannot reach the lock: the requirement refuses it.
fn locked_version(s: &str) -> Version {
    let core = s.split(['-', '+']).next().unwrap_or("");
    plain_version(core)
        .unwrap_or_else(|| panic!("a version in the lock this test cannot read: {s:?}"))
}

/// The semver line of a version: two versions on one line are what a caret requirement accepts.
fn line(v: Version) -> (u64, u64) {
    if v.0 > 0 { (v.0, 0) } else { (0, v.1) }
}

fn show(v: Version) -> String {
    format!("{}.{}.{}", v.0, v.1, v.2)
}

/// Each crate's minimum, read from the requirement in a crate manifest. Only a plain
/// `major.minor.patch` reads as a minimum: Cargo takes it as `^major.minor.patch`, while another
/// form, such as `>=` or `*`, need not hold the line this test assumes.
fn minimums(manifest: &str) -> Vec<(&'static str, Version)> {
    let manifest: toml::Value = toml::from_str(manifest).expect("the manifest parses");
    HELD.iter()
        .map(|held| {
            let name = held.name;
            let requirement = manifest
                .get("dependencies")
                .and_then(|d| d.get(name))
                .and_then(|d| d.as_str().or_else(|| d.get("version")?.as_str()))
                .unwrap_or_else(|| {
                    panic!(
                        "{name} has no `version` requirement in nozzl-acquirer's manifest, so \
                         nothing holds its minimum. If it went on purpose, say what verifies TLS \
                         instead, and change this test in the same commit"
                    )
                });
            let minimum = plain_version(requirement).unwrap_or_else(|| {
                panic!(
                    "{name}'s requirement {requirement:?} is not a plain major.minor.patch, the \
                     one form this test reads as a minimum"
                )
            });
            (name, minimum)
        })
        .collect()
}

fn packages(lock: &str) -> Vec<toml::Value> {
    let lock: toml::Value = toml::from_str(lock).expect("the lock parses");
    lock.get("package")
        .and_then(toml::Value::as_array)
        .expect("the lock has packages")
        .clone()
}

fn field<'a>(package: &'a toml::Value, key: &str) -> &'a str {
    package.get(key).and_then(toml::Value::as_str).unwrap_or("")
}

/// What `package` writes for its dependency on the crate `on`: the version it names, if it names
/// one. The lock writes `name` when it holds one copy of that crate, `name version` when it holds
/// more, and `name version (source)` when two sources hold that version.
fn dependency<'a>(package: &'a toml::Value, on: &str) -> Option<Option<&'a str>> {
    package
        .get("dependencies")?
        .as_array()?
        .iter()
        .filter_map(toml::Value::as_str)
        .find_map(|d| {
            let mut words = d.split_whitespace();
            (words.next() == Some(on)).then(|| words.next())
        })
}

/// A `webpki-roots` entry on another line can be a shim: 0.26.11 holds no roots of its own, and
/// re-exports the 1.x it depends on. So it passes when that 1.x meets the minimum. 0.25 depends on
/// nothing, so it does not pass, and it should not: its roots are its own.
fn is_a_shim_over_a_good_copy(name: &str, got: Version, copy: &toml::Value, min: Version) -> bool {
    name == "webpki-roots"
        && line(got) != line(min)
        && dependency(copy, name)
            .flatten()
            .is_some_and(|inner| locked_version(inner) >= min)
}

/// One line per copy in the lock below its minimum.
fn findings(lock: &str, minimums: &[(&str, Version)]) -> Vec<String> {
    let packages = packages(lock);
    let mut out = Vec::new();
    for &(name, min) in minimums {
        for copy in packages.iter().filter(|p| field(p, "name") == name) {
            let text = field(copy, "version");
            let got = locked_version(text);
            if got < min && !is_a_shim_over_a_good_copy(name, got, copy, min) {
                out.push(format!("{name} {text} is below the minimum {}", show(min)));
            }
        }
    }
    out
}

/// The version of `name` that `user` reaches, past a shim.
fn reached(packages: &[toml::Value], user: &toml::Value, name: &str) -> Option<Version> {
    let named = dependency(user, name)?;
    let copy = packages
        .iter()
        .find(|p| field(p, "name") == name && named.is_none_or(|v| field(p, "version") == v))?;
    let inner = dependency(copy, name).flatten();
    Some(locked_version(inner.unwrap_or(field(copy, "version"))))
}

/// One line per requirement its user does not reach: no copy of the user depends on the crate, or
/// one reaches it on another line. A copy of the user with no dependency on the crate is no
/// finding by itself, because `tokio-tungstenite` also comes in without TLS.
fn out_of_use(lock: &str, minimums: &[(&str, Version)]) -> Vec<String> {
    let packages = packages(lock);
    let mut out = Vec::new();
    for &(name, min) in minimums {
        let user = held(name).used_by;
        let uses: Vec<Version> = packages
            .iter()
            .filter(|p| field(p, "name") == user)
            .filter_map(|p| reached(&packages, p, name))
            .collect();
        if uses.is_empty() {
            out.push(format!(
                "no {user} in the lock depends on {name}, so its requirement holds a copy nothing \
                 uses"
            ));
        }
        for got in uses {
            if line(got) != line(min) {
                out.push(format!(
                    "{user} reaches {name} {}, so the requirement for {} holds a copy it does not \
                     use",
                    show(got),
                    show(min)
                ));
            }
        }
    }
    out
}

/// One line per minimum that is not above the newest release the incident rules out.
fn minimums_ruled_out(minimums: &[(&str, Version)]) -> Vec<String> {
    let mut out = Vec::new();
    for &(name, min) in minimums {
        let newest = held(name).ruled_out;
        if min <= newest {
            out.push(format!(
                "{name}'s minimum {} is not above {}, which the 2026-10 eMudhra incident rules out",
                show(min),
                show(newest)
            ));
        }
    }
    out
}

fn read(path: &str) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn the_real_minimums() -> Vec<(&'static str, Version)> {
    minimums(&read("crates/nozzl-acquirer/Cargo.toml"))
}

#[test]
fn every_copy_in_the_lock_meets_its_minimum() {
    assert_eq!(
        findings(&read("Cargo.lock"), &the_real_minimums()),
        Vec::<String>::new()
    );
}

#[test]
fn what_each_requirement_holds_is_what_its_user_reaches() {
    assert_eq!(
        out_of_use(&read("Cargo.lock"), &the_real_minimums()),
        Vec::<String>::new()
    );
}

#[test]
fn no_minimum_falls_to_a_release_the_incident_rules_out() {
    assert_eq!(
        minimums_ruled_out(&the_real_minimums()),
        Vec::<String>::new()
    );
}

#[test]
fn the_releases_the_incident_rules_out() {
    let cases: &[(&str, Version, bool)] = &[
        ("webpki-roots", (0, 26, 11), true),
        ("webpki-roots", (1, 0, 7), true),
        ("webpki-roots", (1, 0, 9), true),
        ("webpki-roots", (1, 0, 10), false),
        ("webpki-roots", (2, 0, 0), false),
        ("rustls-webpki", (0, 102, 8), true),
        ("rustls-webpki", (0, 103, 13), true),
        ("rustls-webpki", (0, 103, 15), true),
        ("rustls-webpki", (0, 103, 16), false),
        ("rustls-webpki", (0, 104, 0), false),
    ];
    for &(name, min, is_out) in cases {
        assert_eq!(
            !minimums_ruled_out(&[(name, min)]).is_empty(),
            is_out,
            "{name} {}",
            show(min)
        );
    }
}

#[test]
fn the_minimums_come_from_the_manifest() {
    let manifest = r#"
        [dependencies]
        webpki-roots = "2.3.4"
        rustls-webpki = { version = "5.6.7", default-features = false }
    "#;
    assert_eq!(
        minimums(manifest),
        [("webpki-roots", (2, 3, 4)), ("rustls-webpki", (5, 6, 7))]
    );
}

#[test]
#[should_panic(expected = "rustls-webpki has no `version` requirement")]
fn a_requirement_that_goes_fails() {
    minimums("[dependencies]\nwebpki-roots = { version = \"1.0.10\" }\n");
}

#[test]
#[should_panic(expected = "webpki-roots's requirement \">=1.0.10\" is not a plain")]
fn a_requirement_in_another_form_fails() {
    minimums(
        "[dependencies]\nwebpki-roots = { version = \">=1.0.10\" }\n\
         rustls-webpki = { version = \"0.103.16\" }\n",
    );
}

#[test]
#[should_panic(expected = "a version in the lock this test cannot read")]
fn a_lock_version_this_test_cannot_read_fails() {
    locked_version("1.0");
}

#[test]
fn only_a_plain_version_reads_as_a_minimum() {
    assert_eq!(plain_version("1.0.10"), Some((1, 0, 10)));
    for form in [
        "^1.0.10",
        "~1.0.10",
        ">=1.0.10",
        "=1.0.10",
        ">=1.0.10, <2",
        "*",
        "1.0",
        "1",
        "1.0.10.1",
        "1.0.x",
        "1.0.10-rc.1",
        "",
    ] {
        assert_eq!(plain_version(form), None, "{form:?}");
    }
}

/// `(name, version, dependencies)`.
type Package<'a> = (&'a str, &'a str, &'a [&'a str]);

/// A lock written as the groups of packages that make it up.
type Parts<'a> = &'a [&'a [Package<'a>]];

const ROOTS: &str = "webpki-roots";
const VERIFIER: &str = "rustls-webpki";
const TRANSPORT: &str = "tokio-tungstenite";
const MINIMUMS: [(&str, Version); 2] = [(ROOTS, (1, 0, 10)), (VERIFIER, (0, 103, 16))];

fn lock_of(packages: &[Package]) -> String {
    packages
        .iter()
        .map(|(name, version, dependencies)| {
            format!(
                "[[package]]\nname = {name:?}\nversion = {version:?}\ndependencies = {dependencies:?}\n"
            )
        })
        .collect()
}

/// Each case is a shape a plain minimum-version check gets wrong: `(what, lock, findings)`.
#[test]
fn the_copies_in_a_lock() {
    let cases: &[(&str, &[Package], &[&str])] = &[
        (
            "both exactly at the minimum",
            &[(ROOTS, "1.0.10", &[]), (VERIFIER, "0.103.16", &[])],
            &[],
        ),
        (
            "the real shape: a shim and the 1.x it re-exports",
            &[
                (ROOTS, "0.26.11", &["webpki-roots 1.0.11"]),
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &[],
        ),
        (
            "a shim over a copy exactly at the minimum",
            &[
                (ROOTS, "0.26.11", &["webpki-roots 1.0.10"]),
                (ROOTS, "1.0.10", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &[],
        ),
        (
            "a shim that names its source, as the lock does when two sources hold one version",
            &[
                (
                    ROOTS,
                    "0.26.11",
                    &["webpki-roots 1.0.11 (registry+https://example.com/)"],
                ),
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &[],
        ),
        (
            "the shim re-exports a 1.x below the minimum: both entries are findings",
            &[
                (ROOTS, "0.26.11", &["webpki-roots 1.0.7"]),
                (ROOTS, "1.0.7", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &[
                "webpki-roots 0.26.11 is below the minimum 1.0.10",
                "webpki-roots 1.0.7 is below the minimum 1.0.10",
            ],
        ),
        (
            "an older 0.26 with dependencies, none of them on roots: it holds its own, so no shim",
            &[
                (ROOTS, "0.26.8", &["rustls-pki-types 1.12.0"]),
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &["webpki-roots 0.26.8 is below the minimum 1.0.10"],
        ),
        (
            "a copy on the minimum's own line is no shim, whatever it names",
            &[
                (ROOTS, "1.0.7", &["webpki-roots 1.0.11"]),
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &["webpki-roots 1.0.7 is below the minimum 1.0.10"],
        ),
        (
            "only the roots have a shim: an older verifier line stays a finding, whatever it names",
            &[
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.102.8", &["rustls-webpki 0.103.16"]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &["rustls-webpki 0.102.8 is below the minimum 0.103.16"],
        ),
        (
            "the roots below the minimum",
            &[(ROOTS, "1.0.7", &[]), (VERIFIER, "0.103.16", &[])],
            &["webpki-roots 1.0.7 is below the minimum 1.0.10"],
        ),
        (
            "the verifier below the minimum",
            &[(ROOTS, "1.0.11", &[]), (VERIFIER, "0.103.13", &[])],
            &["rustls-webpki 0.103.13 is below the minimum 0.103.16"],
        ),
        (
            "an older verifier line beside a good one: Cargo accepts this",
            &[
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.102.8", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &["rustls-webpki 0.102.8 is below the minimum 0.103.16"],
        ),
        (
            "an older line of roots beside a good one: it is no shim, and Cargo accepts this",
            &[
                (ROOTS, "0.25.4", &[]),
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &["webpki-roots 0.25.4 is below the minimum 1.0.10"],
        ),
        (
            "a prerelease above the minimum: the suffix is not a version part",
            &[(ROOTS, "1.0.11", &[]), (VERIFIER, "0.103.17-alpha.1", &[])],
            &[],
        ),
        (
            "build metadata above the minimum: the suffix is not a version part",
            &[(ROOTS, "1.0.11+build.1", &[]), (VERIFIER, "0.103.16", &[])],
            &[],
        ),
        (
            "another crate depending on an old copy does not make it a copy",
            &[
                ("some-tls-thing", "1.0.0", &["webpki-roots 1.0.7"]),
                (ROOTS, "1.0.11", &[]),
                (VERIFIER, "0.103.16", &[]),
            ],
            &[],
        ),
    ];
    for (what, packages, expected) in cases {
        assert_eq!(&findings(&lock_of(packages), &MINIMUMS), expected, "{what}");
    }
}

/// Each case is a way a requirement can go on passing while it holds nothing: `(what, lock,
/// findings)`.
#[test]
fn the_users_in_a_lock() {
    let roots: &[Package] = &[
        (TRANSPORT, "0.26.2", &["webpki-roots 0.26.11"]),
        (ROOTS, "0.26.11", &["webpki-roots 1.0.11"]),
        (ROOTS, "1.0.11", &[]),
    ];
    let verifier: &[Package] = &[
        ("rustls", "0.23.40", &["rustls-webpki"]),
        (VERIFIER, "0.103.16", &[]),
    ];
    const OFF_THE_VERIFIER: &str = "no rustls in the lock depends on rustls-webpki, so its requirement holds a copy nothing uses";
    const OFF_THE_ROOTS: &str = "no tokio-tungstenite in the lock depends on webpki-roots, so its requirement holds a copy nothing uses";
    const NEW_VERIFIER_LINE: &str = "rustls reaches rustls-webpki 0.104.1, so the requirement for 0.103.16 holds a copy it does not use";
    const NEW_ROOTS_LINE: &str = "tokio-tungstenite reaches webpki-roots 2.0.0, so the requirement for 1.0.10 holds a copy it does not use";
    let cases: &[(&str, Parts, &[&str])] = &[
        ("the real shape", &[roots, verifier], &[]),
        (
            "the transport on the required line itself: a bare name is the lock's one copy",
            &[
                verifier,
                &[
                    (TRANSPORT, "0.30.0", &["webpki-roots"]),
                    (ROOTS, "1.0.11", &[]),
                ],
            ],
            &[],
        ),
        (
            "a later release on the required line",
            &[
                verifier,
                &[
                    (TRANSPORT, "0.30.0", &["webpki-roots"]),
                    (ROOTS, "1.2.0", &[]),
                ],
            ],
            &[],
        ),
        (
            "another copy of the transport with no TLS, as the real lock has",
            &[
                roots,
                verifier,
                &[(TRANSPORT, "0.27.0", &["tungstenite 0.27.0"])],
            ],
            &[],
        ),
        (
            "a dependency that names its source",
            &[
                roots,
                &[
                    (
                        "rustls",
                        "0.23.40",
                        &["rustls-webpki 0.103.16 (registry+https://example.com/)"],
                    ),
                    (VERIFIER, "0.103.16", &[]),
                ],
            ],
            &[],
        ),
        (
            "another crate on another line is not the user",
            &[
                roots,
                verifier,
                &[
                    ("some-tls-thing", "1.0.0", &["webpki-roots 2.0.0"]),
                    (ROOTS, "2.0.0", &[]),
                ],
            ],
            &[],
        ),
        (
            "rustls gone from the lock",
            &[roots, &[(VERIFIER, "0.103.16", &[])]],
            &[OFF_THE_VERIFIER],
        ),
        (
            "rustls off the verifier: the requirement alone keeps it in the lock",
            &[
                roots,
                &[
                    ("rustls", "0.24.0", &["some-verifier"]),
                    (VERIFIER, "0.103.16", &[]),
                ],
            ],
            &[OFF_THE_VERIFIER],
        ),
        (
            "the transport off the compiled-in roots: the requirement alone keeps them in the lock",
            &[
                verifier,
                &[
                    (TRANSPORT, "0.26.2", &["rustls-native-certs"]),
                    (ROOTS, "1.0.11", &[]),
                ],
            ],
            &[OFF_THE_ROOTS],
        ),
        (
            "rustls on a newer line of the verifier: every copy passes, and the requirement holds nothing",
            &[
                roots,
                &[
                    ("rustls", "0.24.0", &["rustls-webpki 0.104.1"]),
                    (VERIFIER, "0.103.16", &[]),
                    (VERIFIER, "0.104.1", &[]),
                ],
            ],
            &[NEW_VERIFIER_LINE],
        ),
        (
            "a bare name reads the lock's one copy, on another line too",
            &[
                roots,
                &[
                    ("rustls", "0.24.0", &["rustls-webpki"]),
                    (VERIFIER, "0.104.1", &[]),
                ],
            ],
            &[NEW_VERIFIER_LINE],
        ),
        (
            "the transport on a newer line of the roots",
            &[
                verifier,
                &[
                    (TRANSPORT, "0.30.0", &["webpki-roots 2.0.0"]),
                    (ROOTS, "1.0.11", &[]),
                    (ROOTS, "2.0.0", &[]),
                ],
            ],
            &[NEW_ROOTS_LINE],
        ),
        (
            "a shim over a newer line of the roots",
            &[
                verifier,
                &[
                    (TRANSPORT, "0.26.2", &["webpki-roots 0.26.11"]),
                    (ROOTS, "0.26.11", &["webpki-roots 2.0.0"]),
                    (ROOTS, "1.0.11", &[]),
                    (ROOTS, "2.0.0", &[]),
                ],
            ],
            &[NEW_ROOTS_LINE],
        ),
        (
            "one of two copies of rustls has moved",
            &[
                roots,
                &[
                    ("rustls", "0.23.40", &["rustls-webpki 0.103.16"]),
                    ("rustls", "0.24.0", &["rustls-webpki 0.104.1"]),
                    (VERIFIER, "0.103.16", &[]),
                    (VERIFIER, "0.104.1", &[]),
                ],
            ],
            &[NEW_VERIFIER_LINE],
        ),
    ];
    for (what, parts, expected) in cases {
        assert_eq!(
            &out_of_use(&lock_of(&parts.concat()), &MINIMUMS),
            expected,
            "{what}"
        );
    }
}
