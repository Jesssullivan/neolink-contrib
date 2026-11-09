# Estate recipes for xoxd-ai/neolink. Upstream build steps stay in cargo and
# .github/workflows; these recipes cover hooks and the local build.

root := justfile_directory()

# The hook files, byte-identical to the Great Falls Tool Bus organization mirror.
hook_files := "_lib.sh pre-commit commit-msg pre-push test.sh"
hooks_mirror := "https://raw.githubusercontent.com/Great-Falls-Tool-Bus/.github/main/githooks"

default:
    @just --list

# Use .githooks for this clone, push to the fork by default, and disable pushes to alex and upstream.
hooks-install:
    #!/usr/bin/env bash
    set -euo pipefail
    cd "{{ root }}"
    git config core.hooksPath .githooks
    git config remote.pushDefault origin
    for r in alex upstream; do
      if git remote get-url "$r" >/dev/null 2>&1; then
        git remote set-url --push "$r" DISABLED-fork-first
      fi
    done
    if [ "$(git config --get commit.gpgsign || true)" != "true" ]; then
      echo "warning: commit.gpgsign is not set; the pre-push hook refuses unsigned commits." >&2
    fi
    echo "Git hooks installed from .githooks; pushes default to origin."

# Fail when .githooks differs from the organization mirror.
hooks-check:
    #!/usr/bin/env bash
    set -euo pipefail
    cd "{{ root }}"
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    status=0
    for f in {{ hook_files }}; do
      curl -fsSL "{{ hooks_mirror }}/$f" -o "$tmp/$f"
      if ! cmp -s "$tmp/$f" ".githooks/$f"; then
        echo "hooks-check: .githooks/$f differs from {{ hooks_mirror }}/$f" >&2
        status=1
      fi
    done
    [ "$status" -eq 0 ] && echo "hooks-check: .githooks matches the organization mirror."
    exit "$status"

# Run the hook self-test against a throwaway repository.
hooks-test:
    cd {{ root }} && bash .githooks/test.sh

# Fail when a commit on this branch that alex/master and upstream/master lack carries AI attribution.
attribution-check base="alex/master":
    #!/usr/bin/env bash
    set -euo pipefail
    cd "{{ root }}"
    . .githooks/_lib.sh
    status=0
    for sha in $(git rev-list HEAD --not "{{ base }}" --remotes=upstream); do
      if line="$(git log -1 --format=%B "$sha" | gftb_ai_attribution_line)"; then
        echo "attribution-check: $(git rev-parse --short "$sha") carries AI attribution: $line" >&2
        status=1
      fi
    done
    [ "$status" -eq 0 ] && echo "attribution-check: no AI attribution above {{ base }}."
    exit "$status"

# Build and test with the system GStreamer development packages (see README).
test:
    cd {{ root }} && cargo test --workspace
