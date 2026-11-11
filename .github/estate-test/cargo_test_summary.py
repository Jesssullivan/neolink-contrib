#!/usr/bin/env python3
"""Count the results of a `cargo test` run per test binary and check them.

The estate-test job (TIN-8227) pipes `cargo test --workspace` through this
script. It reads the combined stdout and stderr of cargo, writes a table of
passed, failed and ignored tests per crate to the job summary, and exits
non-zero when:

- the output holds no `test result:` line at all,
- any test binary reports a failed test or a `FAILED` result,
- a crate named with `--require` ran fewer than `--min` passing tests.

The last rule is the check `estate_test_job_runs_core_tests`: the job must
run at least one test from `neolink_core`, so a change that stops the core
tests from running cannot pass the job.

Grammar read, from cargo and libtest:

    Running unittests src/lib.rs (target/debug/deps/neolink_core-0a1b2c3d)
    Running tests/foo.rs (target\\debug\\deps\\foo-0a1b2c3d.exe)
    Doc-tests neolink_core
    running 3 tests
    test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s
    test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
RUNNING = re.compile(r"^\s*Running\s+(?:unittests\s+)?\S.*\((?P<path>[^()]+)\)\s*$")
DOCTESTS = re.compile(r"^\s*Doc-tests\s+(?P<crate>[A-Za-z0-9_-]+)\s*$")
RESULT = re.compile(
    r"^test result: (?P<status>ok|FAILED)\. "
    r"(?P<passed>\d+) passed; (?P<failed>\d+) failed; (?P<ignored>\d+) ignored"
)
HASH_SUFFIX = re.compile(r"-[0-9a-f]{8,}$")


@dataclass
class Counts:
    passed: int = 0
    failed: int = 0
    ignored: int = 0
    binaries: int = 0
    failed_binaries: int = 0


@dataclass
class Summary:
    crates: dict[str, Counts] = field(default_factory=dict)
    results: int = 0

    def add(self, crate: str, status: str, passed: int, failed: int, ignored: int) -> None:
        c = self.crates.setdefault(crate, Counts())
        c.passed += passed
        c.failed += failed
        c.ignored += ignored
        c.binaries += 1
        if status != "ok" or failed:
            c.failed_binaries += 1
        self.results += 1


def crate_from_path(path: str) -> str:
    """Return the crate name of a test binary path such as deps/neolink_core-0a1b."""
    base = re.split(r"[\\/]", path.strip())[-1]
    if base.endswith(".exe"):
        base = base[: -len(".exe")]
    return HASH_SUFFIX.sub("", base)


def parse(text: str) -> Summary:
    summary = Summary()
    current = "unknown"
    for raw in text.splitlines():
        line = ANSI.sub("", raw).rstrip("\r")
        m = RUNNING.match(line)
        if m:
            current = crate_from_path(m.group("path"))
            continue
        m = DOCTESTS.match(line)
        if m:
            current = m.group("crate").replace("-", "_")
            continue
        m = RESULT.match(line.strip())
        if m:
            summary.add(
                current,
                m.group("status"),
                int(m.group("passed")),
                int(m.group("failed")),
                int(m.group("ignored")),
            )
    return summary


def check(summary: Summary, require: list[str], minimum: int) -> list[str]:
    problems = []
    if summary.results == 0:
        problems.append("no `test result:` line in the cargo output")
    for crate, c in sorted(summary.crates.items()):
        if c.failed or c.failed_binaries:
            problems.append(f"{crate}: {c.failed} failed test(s) in {c.failed_binaries} binary(ies)")
    for crate in require:
        got = summary.crates.get(crate, Counts()).passed
        if got < minimum:
            problems.append(f"{crate}: {got} passing test(s), at least {minimum} required")
    return problems


def markdown(summary: Summary, problems: list[str]) -> str:
    lines = [
        "### cargo test per crate",
        "",
        "| crate | binaries | passed | failed | ignored |",
        "|---|---|---|---|---|",
    ]
    for crate, c in sorted(summary.crates.items()):
        lines.append(f"| `{crate}` | {c.binaries} | {c.passed} | {c.failed} | {c.ignored} |")
    lines.append("")
    if problems:
        lines.append("**Check failed:**")
        lines.extend(f"- {p}" for p in problems)
    else:
        lines.append("Check passed.")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("log", help="file holding the cargo test output, or - for stdin")
    ap.add_argument("--require", action="append", default=[], metavar="CRATE")
    ap.add_argument("--min", type=int, default=1, dest="minimum")
    args = ap.parse_args(argv)

    if args.log == "-":
        text = sys.stdin.read()
    else:
        with open(args.log, encoding="utf-8", errors="replace") as fh:
            text = fh.read()

    summary = parse(text)
    problems = check(summary, args.require, args.minimum)
    report = markdown(summary, problems)
    print(report)
    out = os.environ.get("GITHUB_STEP_SUMMARY")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(report + "\n")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
