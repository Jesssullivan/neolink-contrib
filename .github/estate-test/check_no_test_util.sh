#!/usr/bin/env bash
# Fail when a normal or release build would compile neolink_core with its
# test-util feature (TIN-8227). That feature holds FakeCamera, test support
# that no shipped binary may carry. The root crate names it as a
# dev-dependency only, and Cargo's resolver 2 keeps dev-dependency features
# out of a normal build. This check fails when resolver 1 comes back or a
# normal dependency turns the feature on.
#
# It asks cargo tree for the features neolink_core gets from normal and build
# edges, on every target, with every feature of the root crate on, as the
# release cross build in build.yml does.
#
# Usage: check_no_test_util.sh
# ESTATE_CARGO overrides the cargo binary, default cargo.
set -euo pipefail

cargo="${ESTATE_CARGO:-cargo}"

out="$("$cargo" tree --locked --all-features --target all -e normal,build \
  -i neolink_core --depth 0 --format '{p} [{f}]')"
echo "$out"

line="$(printf '%s\n' "$out" | grep -m1 '^neolink_core v' || true)"
if [[ ! "$line" =~ \[([^]]*)\]$ ]]; then
  echo "check_no_test_util: no feature list for neolink_core in the cargo tree output" >&2
  exit 2
fi
features="${BASH_REMATCH[1]}"

if [[ ",${features// /}," == *",test-util,"* ]]; then
  echo "check_no_test_util: a normal build turns on neolink_core/test-util" >&2
  exit 1
fi
echo "check_no_test_util: neolink_core features in a normal build: [${features}]"
