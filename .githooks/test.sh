#!/usr/bin/env bash
# Self-test for the Great Falls Tool Bus git hooks. Run by `just hooks-test`.
#
# Builds a throwaway repository under ${TMPDIR:-/tmp} with fixture commits
# (signed with a throwaway SSH key, unsigned, AI attribution, em dash, good and
# bad branch names), pushes to local bare repositories standing in for the
# organization remote and a fork, and asserts exit codes and warnings. The
# operator's own git configuration is never read: HOME and the global config
# point into the temporary directory, which is removed on exit. The real HOME
# is restored before that removal, because a home-root delete guard refuses to
# remove any ancestor of the current HOME.
set -euo pipefail

hooks="$(cd "$(dirname "$0")" && pwd -P)"
real_home="${HOME:-}"
work="$(mktemp -d "${TMPDIR:-/tmp}/gftb-hooks-test.XXXXXX")"
cleanup() {
  export HOME="$real_home"
  rm -rf "$work"
}
trap cleanup EXIT

export HOME="$work/home"
export GIT_CONFIG_GLOBAL="$work/gitconfig"
export GIT_CONFIG_NOSYSTEM=1
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE SSH_AUTH_SOCK GFTB_HOOKS_CHAINED || true
mkdir -p "$HOME" "$work/marks" "$work/global-hooks"

em="$(printf '\342\200\224')"
robot="$(printf '\360\237\244\226')"

# Vendor and GitHub addresses are assembled at run time so no source line
# carries a literal address outside the documentation domains; the public PII
# validators in every repository scan this file. The hooks still see the full
# address.
ai_vendor_domain=anthropic.com
ai_vendor_alt_domain=openai.com
github_domain=github.com
claude_addr="noreply@${ai_vendor_domain}"
codex_addr="codex@${ai_vendor_alt_domain}"
github_merge_addr="noreply@${github_domain}"

# A stand-in for a machine-wide hook layer; records that it was chained to.
for hook in pre-commit commit-msg; do
  printf '#!/usr/bin/env bash\n: > "%s/%s"\n' "$work/marks" "$hook" > "$work/global-hooks/$hook"
done
printf '#!/usr/bin/env bash\ncat > "%s/pre-push"\n' "$work/marks" > "$work/global-hooks/pre-push"
chmod +x "$work/global-hooks/"*

ssh-keygen -q -t ed25519 -N '' -C gftb-hooks-test -f "$work/signing"
git config --global user.name "Hook Test"
git config --global user.email "hook-test@example.org"
git config --global init.defaultBranch main
git config --global gpg.format ssh
git config --global user.signingkey "$work/signing"
git config --global commit.gpgsign true
git config --global core.hooksPath "$work/global-hooks"

passed=0
failed=0
out="$work/out"

# check NAME WANT_STATUS [--warns|--quiet|--refused] -- COMMAND...
check() {
  local name="$1" want="$2" expect="$3" got
  shift 4
  set +e
  "$@" > "$out" 2>&1
  got=$?
  set -e
  local ok=1
  if [ "$want" = 0 ] && [ "$got" -ne 0 ]; then ok=0; fi
  if [ "$want" != 0 ] && [ "$got" -eq 0 ]; then ok=0; fi
  case "$expect" in
    --warns) grep -q '^gftb-hooks: warning:' "$out" || ok=0 ;;
    --quiet) grep -q '^gftb-hooks:' "$out" && ok=0 ;;
    --refused) grep -q '^gftb-hooks: REFUSED:' "$out" && grep -q '^gftb-hooks:   fix:' "$out" || ok=0 ;;
  esac
  if [ "$ok" -eq 1 ]; then
    passed=$((passed + 1))
    printf 'ok   %s\n' "$name"
  else
    failed=$((failed + 1))
    printf 'FAIL %s (exit %s, wanted %s, %s)\n' "$name" "$got" "$want" "$expect"
    sed 's/^/     | /' "$out"
  fi
}

mark_check() {
  local name="$1" file="$2"
  if [ -f "$work/marks/$file" ]; then
    passed=$((passed + 1))
    printf 'ok   %s\n' "$name"
  else
    failed=$((failed + 1))
    printf 'FAIL %s (global %s hook did not run)\n' "$name" "$file"
  fi
}

out_has() {
  if grep -qF -- "$2" "$out"; then
    passed=$((passed + 1))
    printf 'ok   %s\n' "$1"
  else
    failed=$((failed + 1))
    printf 'FAIL %s\n' "$1"
  fi
}

repo="$work/repo"
git init -q "$repo"
cd "$repo"
git config core.hooksPath "$hooks"

commit() { git commit -q --allow-empty "$@"; }

# commit-msg and pre-commit
printf 'plain text\n' > a.txt
git add a.txt
check "signed conventional commit passes quietly" 0 --quiet -- commit -m "feat: add a"
mark_check "commit-msg chains to the global hooks path" commit-msg
mark_check "pre-commit chains to the global hooks path" pre-commit
base="$(git rev-parse HEAD)"

check "human Co-Authored-By trailer passes" 0 --quiet -- \
  commit -m "docs: pair work" -m "Co-Authored-By: Ada Lovelace <ada@example.org>"
check "human named Aiden passes" 0 --quiet -- \
  commit -m "docs: pair work" -m "Co-Authored-By: Aiden Smith <aiden@example.org>"
check "Claude trailer refused" 1 --refused -- \
  commit -m "feat: x" -m "Co-Authored-By: Claude <${claude_addr}>"
check "Codex trailer refused" 1 --refused -- \
  commit -m "feat: x" -m "Co-authored-by: Codex <${codex_addr}>"
check "Copilot trailer refused" 1 --refused -- \
  commit -m "feat: x" -m "Co-Authored-By: Copilot <copilot@example.org>"
check "an AI trailer refused" 1 --refused -- \
  commit -m "feat: x" -m "Co-Authored-By: Some AI <bot@example.org>"
check "Gemini trailer refused" 1 --refused -- \
  commit -m "feat: x" -m "Co-Authored-By: Gemini <bot@example.org>"
check "GPT trailer refused" 1 --refused -- \
  commit -m "feat: x" -m "Co-Authored-By: ChatGPT <bot@example.org>"
check "Generated with line refused" 1 --refused -- \
  commit -m "feat: x" -m "Generated with some tool"
check "robot face refused" 1 --refused -- \
  commit -m "feat: x" -m "$robot made this"
check "[codex] subject prefix refused" 1 --refused -- \
  commit -m "[codex] feat: x"
check "non-conventional subject warns" 0 --warns -- commit -m "update stuff"
check "em dash in message warns" 0 --warns -- commit -m "fix: one ${em} two"

printf 'new %s dash\n' "$em" > dash.txt
git add dash.txt
check "em dash added to a new file warns" 0 --warns -- commit -m "docs: add dash file"
printf 'more %s dash\n' "$em" >> dash.txt
git add dash.txt
check "em dash added to a file that had one is quiet" 0 --quiet -- commit -m "docs: extend dash file"

# Loop guard: a global hooks path equal to this directory is never chained to.
git config --global core.hooksPath "$hooks"
check "global hooks path equal to the repo hooks does not loop" 0 --quiet -- commit -m "chore: loop guard"
git config --global core.hooksPath "$work/global-hooks"

# pre-push
mkdir -p "$work/github.com/Great-Falls-Tool-Bus" "$work/github.com/someone"
git init -q --bare "$work/github.com/Great-Falls-Tool-Bus/repo.git"
git init -q --bare "$work/github.com/someone/repo.git"
git remote add upstream "file://$work/github.com/Great-Falls-Tool-Bus/repo.git"
git remote add origin "file://$work/github.com/someone/repo.git"

git switch -q -c feat/ok "$base"
commit -m "feat: signed work"
check "push of signed work to the organization remote refused" 1 --refused -- git push -q upstream feat/ok
out_has "refusal names the Fork first section" 'Fork first'
check "push of signed work to the fork passes quietly" 0 --quiet -- git push -q origin feat/ok
if [ -s "$work/marks/pre-push" ] && grep -q 'refs/heads/feat/ok' "$work/marks/pre-push"; then
  passed=$((passed + 1)); echo "ok   pre-push chains to the global hooks path with stdin"
else
  failed=$((failed + 1)); echo "FAIL pre-push chain did not receive stdin"
fi

sha="$(git rev-parse HEAD)"
null="$(printf '%0*d' "${#sha}" 0)"
for url in git@github.com:Great-Falls-Tool-Bus/meta.git https://github.com/Great-Falls-Tool-Bus/meta.git \
  ssh://git@github.com/great-falls-tool-bus/meta.git; do
  check "synthetic push to $url refused" 1 --refused -- \
    bash -c "printf 'refs/heads/feat/ok %s refs/heads/feat/ok %s\n' '$sha' '$null' | '$hooks/pre-push' upstream '$url'"
done
check "synthetic push to a fork URL passes" 0 --quiet -- \
  bash -c "printf 'refs/heads/feat/ok %s refs/heads/feat/ok %s\n' '$sha' '$null' | '$hooks/pre-push' origin git@github.com:someone/meta.git"

git switch -q -c wip/x "$base"
commit -m "feat: signed work on a loose branch"
check "branch wip/x warns but pushes" 0 --warns -- git push -q origin wip/x

git switch -q -c feat/unsigned "$base"
commit --no-verify --no-gpg-sign -m "feat: unsigned work"
check "unsigned commit refused" 1 --refused -- git push -q origin feat/unsigned
out_has "unsigned refusal gives the rebase command" 'rebase --force-rebase --gpg-sign'

git switch -q -c feat/trailer "$base"
commit --no-verify -m "feat: trailer" -m "Co-Authored-By: Claude <${claude_addr}>"
check "AI trailer committed with --no-verify refused at push" 1 --refused -- git push -q origin feat/trailer

git switch -q -c feat/generated "$base"
commit --no-verify -m "feat: generated" -m "Generated with a tool"
check "generated-by line refused at push" 1 --refused -- git push -q origin feat/generated

# Commits already on a remote-tracking ref are not walked again.
git switch -q -c feat/legacy "$base"
commit --no-verify --no-gpg-sign -m "chore: legacy unsigned"
git push -q --no-verify origin feat/legacy
git switch -q -c feat/stacked
commit -m "feat: signed on top of legacy"
check "only commits not on a remote-tracking ref are walked" 0 --quiet -- git push -q origin feat/stacked

# Merge commits made by GitHub are exempt from the signature rule.
git switch -q -c feat/merged "$base"
commit -m "feat: left"
GIT_COMMITTER_NAME=GitHub GIT_COMMITTER_EMAIL="$github_merge_addr" \
  git merge -q --no-verify --no-ff --no-gpg-sign -m "Merge pull request #1 from someone/feat/ok" feat/ok
check "unsigned GitHub merge commit is exempt" 0 --quiet -- git push -q origin feat/merged

git switch -q -c feat/local-merge "$base"
commit -m "feat: right"
git merge -q --no-verify --no-ff --no-gpg-sign -m "Merge branch 'feat/ok'" feat/ok
check "unsigned local merge commit refused" 1 --refused -- git push -q origin feat/local-merge

check "branch deletion passes" 0 --quiet -- git push -q origin :wip/x

printf '\nhooks-test: %s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
