#!/usr/bin/env python3
"""Verify the house `kind-NNNNN (slug)` convention.

    scripts/check-kind-slugs.py <file> [<file> ...]
    scripts/check-kind-slugs.py --first-only docs/*.md
    scripts/check-kind-slugs.py --self-test

Exits 1 if any file has a violation, 0 otherwise. Slugs come from
`config/kinds.toml` (the `k = NNNNN` / `slug = "..."` pairs), which is the
authority - this script reads it rather than
carrying its own copy, so a newly onboarded kind needs no change here.

WHY THIS EXISTS AS A SCRIPT rather than a grep in a comment. There were three
divergent hand-written copies of "the sweep", and the one the project's rules
prescribed could not match the hyphenated spelling at all: it returned
0 on a file holding 38 violations and a commit claimed compliance on that basis
(2026-09-23). Four categories are reported,
and a regex-only sweep can see only the first two:

  1. PROSE      a bare `kind NNNNN` / `Kinds NNNNN`. Anchored with (^|[^-A-Za-z]),
                because a line that BEGINS with the word has no preceding
                character and the original pattern silently skipped it - the
                common case in wrapped Markdown. The number may wear Markdown
                emphasis or code (`kind **1622**`, `kind `5``): a costume, not a  kind-slug-ok
                slug. A number placed BEFORE the word (`the 1622 reply kind`) is
                not caught - any pattern for it also matches `500 kinds`.
  2. BARE       a bare `kind-NNNNN` with no slug. Case-insensitive and
                plural-tolerant: `Kind-NNNNN` and `kinds-NNNNN` escaped every
                earlier version of this check.
  3. WRONG SLUG `kind-NNNNN (the-slug-of-a-different-kind)` - a parenthetical
                SHAPED like a slug but naming another kind. No sweep can catch
                this, and it is STRICTLY WORSE than a bare number: a bare number
                tells the reader to look it up, a plausible wrong slug tells them
                not to. Catching it is why this reads the authority file.
  4. UNSLUGGED  `kind-NNNNN (some prose, a cross-reference, a SQL clause)` - a
                parenthetical that was never a slug claim, so the mention simply
                has no slug. Split from 3 deliberately: reporting "authority says
                X" about text nobody meant as a slug makes the whole report look
                wrong, and the fix differs - reword so the slug comes first.

Check 2 tolerates a slug that WRAPPED onto the next line - `kind-NNNNN` at the
end of one line and its `(slug)` at the start of the next is correct, and a
line-by-line checker reports it as a violation. The lookahead is bounded to at
most one newline so that a bare mention followed by an unrelated parenthetical
three paragraphs later is still caught.

Do NOT reach for `grep -P` with `-z` to do check 2 in one line: BSD grep
rejects the combination (exit 2, "invalid option -- P"), and the failure is
easy to miss if stderr is redirected away.

`--self-test` runs the checker against a corpus of its own past bugs. Every case
in it is a defect this script shipped, or a correct spelling an earlier version
wrongly flagged. It needs no test runner and no dependency, because this repo is
Rust and has neither - and a checker nobody can run against known cases is how
the first three bugs survived review. Mutate a pattern and it fails by name.

A line carrying `kind-slug-ok` is exempt, for the two cases where the wrong
spelling is the point: a test FIXTURE, and an assertion on RUNTIME OUTPUT that a
program produced without access to the slug table. It is per-line, and it is not
a way to make the rule stop applying to prose you would rather not fix.
"""

import argparse
import re
import sys
from pathlib import Path

AUTHORITY = Path(__file__).resolve().parent.parent / "config" / "kinds.toml"

# All three are case-INSENSITIVE. `KIND-NNNNN` and `Kind-NNNNN` escaped every
# earlier hand-written version of this sweep, including the first draft of this
# script: an explicit [Kk] class silently excludes the all-caps spelling.
# A number wrapped in Markdown emphasis or code - `kind **1622**`, `kind `5`` - is a bare  kind-slug-ok
# number in costume. Both shapes escaped the checker until review 3 of /ngit (2026-09-25).
# The anchor also excludes `_`: `notes_kind5_volume.md` is an identifier, not a
# mention, and file names are full of them.
PROSE = re.compile(r"(^|[^-A-Za-z_])(kinds?) ?(?:\*\*|`)?[0-9]{1,5}", re.M | re.I)
# A mention with a parenthetical: capture the number and what was claimed.
SLUGGED = re.compile(r"(?<![-\w])(kinds?)-(\d+)(?!\d)[ \t]*\n?[ \t]*\(([^)]*)\)", re.I)
# A mention with NO parenthetical within one wrapped line.
BARE = re.compile(r"(?<![-\w])(kinds?)-(\d+)(?!\d)(?![ \t]*\n?[ \t]*\()(?!-)", re.I)
# A parenthetical SHAPED like a slug (lowercase words joined by hyphens) was
# meant as one; anything else - prose, a cross-reference, a SQL clause - was not.
# The lookahead requires at least one LETTER. Without it an all-digit
# parenthetical - a date, a version, a count - reads as slug-shaped. An index
# line reading `kind-3 (09-07)` drew "authority says (follow-lists)".  kind-slug-ok
# That asserts somebody wrote the WRONG slug when nobody wrote a slug at all.
# Every real slug has letters, so requiring one cannot hide a genuine wrong
# slug, and the mention is still reported - as `unslugged`, which it is.
SLUG_SHAPED = re.compile(r"^(?=[a-z0-9-]*[a-z])[a-z0-9]+(-[a-z0-9]+)*$")

# A line carrying this marker is exempt. It exists for the two cases where a
# wrong spelling is the POINT: a test FIXTURE, and an assertion on RUNTIME
# OUTPUT that a program produced without access to the slug table (a test of such
# a program asserts a bare kind number for exactly that reason).
# Without it this file reports eight violations in its own corpus, and a
# checker that flags itself teaches people to ignore it. It is per-LINE, not
# per-file, because a whole-file opt-out would also silence the ordinary prose
# around the fixtures - which is where the real violations hide.
EXEMPT = "kind-slug-ok"


def SLUG_SHAPED_HINT(claimed, limit=40):
    """Truncate a long parenthetical so one bad line cannot flood the report."""
    claimed = " ".join(claimed.split())
    return claimed if len(claimed) <= limit else claimed[:limit - 3] + "..."


def load_slugs(path):
    """{kind_number: slug} from the TOML, without a toml dependency.

    The field is `k = NNNNN`, not `kind = NNNNN`; reading it as the latter
    silently yields an empty map and every check then passes vacuously.
    """
    text = path.read_text(encoding="utf-8")
    out = {}
    for block in re.split(r"\[\[kind\]\]", text)[1:]:
        k = re.search(r"^\s*k\s*=\s*(\d+)", block, re.M)
        slug = re.search(r'^\s*slug\s*=\s*"([^"]+)"', block, re.M)
        if k and slug:
            out[int(k.group(1))] = slug.group(1)
    if not out:
        raise SystemExit("no kinds parsed from %s - has the schema changed?" % path)
    return out


def line_of(text, pos):
    return text.count("\n", 0, pos) + 1


def check(path, slugs, first_only):
    return check_text(path.read_text(encoding="utf-8"), slugs, first_only)


def check_text(text, slugs, first_only=False):
    problems = []

    for m in PROSE.finditer(text):
        # group(2), not the whole match: group 1 is the preceding character, and
        # for a mention at the START of a line that character is the PREVIOUS
        # line's newline - reporting m.start() blames the wrong line.
        problems.append((line_of(text, m.start(2)), "prose", m.group(0).strip()))

    for m in BARE.finditer(text):
        num = int(m.group(2))
        want = slugs.get(num)
        hint = "(%s)" % want if want else "not in the kinds file - give a short description"
        problems.append((line_of(text, m.start()), "bare", "%s -> want %s-%d %s" % (m.group(0), m.group(1), num, hint)))

    for m in SLUGGED.finditer(text):
        num, claimed = int(m.group(2)), m.group(3).strip()
        want = slugs.get(num)
        # An uncaptured kind has no slug to check against; the rule only asks
        # for a short description there, which cannot be validated mechanically.
        if want is None or claimed == want:
            continue
        # Both are violations of "write kind-NNNNN (slug)", but they are
        # different mistakes and conflating them makes the report untrustworthy.
        # A parenthetical SHAPED like a slug is a wrong slug - someone named the
        # wrong kind, which actively misinforms. A parenthetical that is prose, a
        # cross-reference or an expression was never a slug claim at all; the
        # mention simply has no slug and needs rewording so the slug comes first.
        if SLUG_SHAPED.match(claimed):
            problems.append((line_of(text, m.start()), "wrong-slug",
                             "%s-%d (%s) -> authority says (%s)" % (m.group(1), num, claimed, want)))
        else:
            problems.append((line_of(text, m.start()), "unslugged",
                             "%s-%d (%s) -> parenthetical is not a slug; want (%s)"
                             % (m.group(1), num, SLUG_SHAPED_HINT(claimed), want)))

    lines = text.split("\n")
    problems = [p for p in problems
                if not (0 < p[0] <= len(lines) and EXEMPT in lines[p[0] - 1])]

    if first_only:
        seen = set()
        kept = []
        for ln, kind, msg in problems:
            key = (kind, msg.split(" -> ")[0])
            if key in seen:
                continue
            seen.add(key)
            kept.append((ln, kind, msg))
        problems = kept

    return sorted(problems)


# Every case below is a bug this checker actually shipped, or a correct spelling
# an earlier version wrongly flagged. Kept as a self-test rather than a separate
# file because this repo has no Python test runner, and a checker nobody can run
# against known cases is how the first three bugs survived review.
SELF_TEST_SLUGS = {1: "posts", 3: "follow-lists", 5: "event-deletion-requests",
                   30402: "markets-product-listings"}

SELF_TEST_CASES = [
    # (name, text, expected [(line, category), ...])
    ("line-initial prose", "Kinds 30402 are addressable\n", [(1, "prose")]),  # kind-slug-ok
    ("bold-wrapped number", "reply kind **1622 is gone**\n", [(1, "prose")]),  # kind-slug-ok
    ("backtick-wrapped number", "acquire kind `5` too\n", [(1, "prose")]),  # kind-slug-ok
    ("prose blames its own line", "intro\nKind 5 is noisy\n", [(2, "prose")]),  # kind-slug-ok
    ("uppercase hyphenated", "See KIND-5 today\n", [(1, "bare")]),  # kind-slug-ok
    ("plural hyphenated", "kinds-30402 here\n", [(1, "bare")]),  # kind-slug-ok
    ("slug wrapped one line is FINE", "see kind-5\n(event-deletion-requests) now\n", []),  # kind-slug-ok
    ("blank line breaks the wrap", "see kind-5\n\n(event-deletion-requests)\n", [(1, "bare")]),  # kind-slug-ok
    ("correct spelling", "kind-5 (event-deletion-requests)\n", []),  # kind-slug-ok
    ("wrong slug", "kind-5 (deletion-requests)\n", [(1, "wrong-slug")]),  # kind-slug-ok
    ("date is not a wrong slug", "kind-3 (09-07)\n", [(1, "unslugged")]),  # kind-slug-ok
    ("uncaptured kind is unverifiable", "kind-24133 (NIP-46 remote-signing)\n", []),  # kind-slug-ok
    ("hyphen prefix is not a mention", "sub-kind 5 things\n", []),  # kind-slug-ok
    ("underscore-joined identifier is not a mention", "see notes_kind5_volume.md\n", []),  # kind-slug-ok
    ("bare on the second line", "line one\nkind-5 bare\n", [(2, "bare")]),  # kind-slug-ok
    ("the exemption marker silences one line",
     "kind-5 here is deliberate kind-slug-ok\nkind-5 here is not\n", [(2, "bare")]),  # kind-slug-ok
]


def self_test(kinds_path=AUTHORITY):
    failures = []
    for name, text, expected in SELF_TEST_CASES:
        got = [(ln, cat) for ln, cat, _ in check_text(text, SELF_TEST_SLUGS)]
        if got != expected:
            failures.append("%-32s expected %s, got %s" % (name, expected, got))

    # The authority is data, so assert its SHAPE rather than its contents: a slug
    # that is not slug-shaped would make the wrong-slug check compare against
    # something it can never match.
    slugs = load_slugs(kinds_path)
    if len(slugs) < 20:
        failures.append("authority parsed only %d kinds - schema change?" % len(slugs))
    for num, slug in sorted(slugs.items()):
        if not SLUG_SHAPED.match(slug):
            failures.append("kind-%d slug %r is not slug-shaped" % (num, slug))

    for line in failures:
        print("FAIL  " + line, file=sys.stderr)
    print("%d/%d self-test cases pass, %d authority slugs checked"
          % (len(SELF_TEST_CASES) - len([f for f in failures if not f.startswith(("authority", "kind-"))]),
             len(SELF_TEST_CASES), len(slugs)), file=sys.stderr)
    return 1 if failures else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="*", type=Path)
    ap.add_argument("--self-test", action="store_true",
                    help="run the checker against its own known-bug corpus and exit")
    ap.add_argument("--kinds", type=Path, default=AUTHORITY, help="the kinds file to read slugs from (default: config/kinds.toml)")
    ap.add_argument("--first-only", action="store_true",
                    help="report each distinct violation once per file. The rule asks for a slug on "
                         "EVERY mention, so this understates; it is for triaging a large backlog.")
    args = ap.parse_args()

    if args.self_test:
        return self_test(args.kinds)
    if not args.files:
        ap.error("give at least one file, or --self-test")

    slugs = load_slugs(args.kinds)
    total = 0
    for path in args.files:
        if not path.is_file():
            print("%s: not a file" % path, file=sys.stderr)
            total += 1
            continue
        for ln, kind, msg in check(path, slugs, args.first_only):
            print("%s:%d: %s: %s" % (path, ln, kind, msg))
            total += 1

    if total:
        print("\n%d violation(s). Authority: %s" % (total, args.kinds), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
