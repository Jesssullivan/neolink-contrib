#!/usr/bin/env bash
# materialize.sh: copy this fork-only agents overlay into the current checkout.
#
# Run from inside any checkout or worktree of this repository:
#   git fetch origin agents-overlay && git show origin/agents-overlay:materialize.sh | bash
# Preview without writing anything:
#   git show origin/agents-overlay:materialize.sh | bash -s -- --dry-run
#
# What it does:
#   1. fetches origin/agents-overlay;
#   2. extracts every overlay file (except materialize.sh and OVERLAY.md) into
#      the top of the current worktree with git archive | tar;
#   3. never overwrites a path tracked in the current HEAD or index (older
#      branches still carry some of these files): it skips them and says so;
#   4. appends each top-level overlay path to the shared info/exclude, once,
#      so the files stay untracked and out of every commit.
# It touches no branch, index entry or tracked file.
set -euo pipefail

dry_run=0
case "${1:-}" in
  --dry-run|-n) dry_run=1 ;;
  "") ;;
  *) echo "usage: materialize.sh [--dry-run]" >&2; exit 2 ;;
esac

export GIT_LITERAL_PATHSPECS=1
ref=refs/remotes/origin/agents-overlay
top=$(git rev-parse --show-toplevel)
cd "$top"

git fetch --quiet origin "+refs/heads/agents-overlay:$ref"

has_head=0
git rev-parse --verify --quiet HEAD >/dev/null && has_head=1

in_head() { [ "$has_head" = 1 ] && git cat-file -e "HEAD:$1" 2>/dev/null; }
tracked() { in_head "$1" || git ls-files --error-unmatch -- "$1" >/dev/null 2>&1; }

files=()
entries=()
skipped=0
added=0
replaced=0
unchanged=0

add_entry() {
  local e
  for e in ${entries[@]+"${entries[@]}"}; do [ "$e" = "$1" ] && return 0; done
  entries+=("$1")
}

while IFS= read -r -d '' p; do
  case "$p" in materialize.sh|OVERLAY.md) continue ;; esac
  if tracked "$p"; then
    echo "skip (tracked in HEAD, left as is): $p"
    skipped=$((skipped + 1))
    continue
  fi
  files+=("$p")
  if [ -L "$p" ]; then
    if [ "$(readlink "$p")" = "$(git cat-file -p "$ref:$p")" ]; then
      unchanged=$((unchanged + 1))
    else
      replaced=$((replaced + 1)); echo "replace local copy: $p"
    fi
  elif [ -e "$p" ]; then
    if [ "$(git hash-object -- "$p")" = "$(git rev-parse "$ref:$p")" ]; then
      unchanged=$((unchanged + 1))
    else
      replaced=$((replaced + 1)); echo "replace local copy: $p"
    fi
  else
    added=$((added + 1))
  fi
  # Exclude entry: the shortest leading part of the path that HEAD does not
  # track, so a directory owned wholly by the overlay is excluded as one entry
  # and a shared directory (docs/, scripts/) only loses the overlay's files.
  acc=""
  rest="$p"
  while :; do
    part=${rest%%/*}
    acc="${acc:+$acc/}$part"
    if [ "$part" = "$rest" ]; then add_entry "/$acc"; break; fi
    rest=${rest#*/}
    if ! in_head "$acc"; then add_entry "/$acc/"; break; fi
  done
done < <(git ls-tree -r -z --name-only "$ref")

echo "overlay $(git rev-parse --short "$ref"): ${#files[@]} to write (new $added, replaced $replaced, unchanged $unchanged), skipped $skipped tracked"

if [ "$dry_run" = 1 ]; then
  for e in ${entries[@]+"${entries[@]}"}; do echo "would exclude: $e"; done
  echo "dry run: nothing written"
  exit 0
fi

if [ "${#files[@]}" -gt 0 ]; then
  # An existing symlink is removed first so tar writes the overlay's link
  # instead of following the old one.
  for p in "${files[@]}"; do [ -L "$p" ] && rm -f -- "$p"; done
  git archive --format=tar "$ref" "${files[@]}" | tar -x -f - -C "$top"
fi

exclude="$(git rev-parse --path-format=absolute --git-common-dir)/info/exclude"
mkdir -p "$(dirname "$exclude")"
touch "$exclude"
marker="# agents-overlay (materialize.sh): fork-only agent files, never committed"
grep -qxF -- "$marker" "$exclude" || printf '%s\n' "$marker" >>"$exclude"
new_excludes=0
for e in ${entries[@]+"${entries[@]}"}; do
  if ! grep -qxF -- "$e" "$exclude"; then
    printf '%s\n' "$e" >>"$exclude"
    new_excludes=$((new_excludes + 1))
  fi
done
echo "info/exclude: $new_excludes new of ${#entries[@]} entries ($exclude)"
