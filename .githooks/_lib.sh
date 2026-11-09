# shellcheck shell=bash
# shellcheck disable=SC2034 # variables are used by the hooks that source this file
# Shared helpers for the Great Falls Tool Bus git hooks.
#
# Canonical copy: meta steering/githooks/. Mirror: the organization .github
# repository, githooks/. Every repository carries a byte-identical .githooks/
# and `just hooks-check` compares it with the mirror.
#
# Plain bash and git only, compatible with bash 3.2. Every refusal or warning
# prints three lines: what happened, the CONTRIBUTING.md section that explains
# the rule, and the exact command that fixes it. The text is written for a
# person or an agent reading stderr.

GFTB_CONTRIBUTING="https://github.com/Great-Falls-Tool-Bus/.github/blob/main/CONTRIBUTING.md"

# Characters built from bytes so these sources contain none of them.
GFTB_EM_DASH="$(printf '\342\200\224')"
GFTB_ROBOT="$(printf '\360\237\244\226')"

GFTB_REFUSED=0

gftb_refuse() {
  # $1 what was refused, $2 CONTRIBUTING.md section, $3 fix command
  printf 'gftb-hooks: REFUSED: %s\n' "$1" >&2
  printf 'gftb-hooks:   rule: CONTRIBUTING.md, section "%s" (%s)\n' "$2" "$GFTB_CONTRIBUTING" >&2
  printf 'gftb-hooks:   fix:  %s\n' "$3" >&2
  GFTB_REFUSED=1
}

gftb_warn() {
  # $1 what looks wrong, $2 CONTRIBUTING.md section, $3 fix command
  printf 'gftb-hooks: warning: %s\n' "$1" >&2
  printf 'gftb-hooks:   rule: CONTRIBUTING.md, section "%s" (%s)\n' "$2" "$GFTB_CONTRIBUTING" >&2
  printf 'gftb-hooks:   fix:  %s\n' "$3" >&2
}

# Print the message body without comment lines and without anything below a
# `git commit --verbose` scissors line.
gftb_message_text() {
  sed -e '/^# -\{1,\} >8 -\{1,\}$/,$d' -e '/^#/d' "$1"
}

# Exit 0 when the message text on stdin carries AI attribution, printing the
# offending line. Human Co-Authored-By trailers pass.
gftb_ai_attribution_line() {
  local text first
  text="$(cat)"
  first="$(printf '%s\n' "$text" | sed -n '/[^[:space:]]/{p;q;}')"
  if printf '%s\n' "$first" | grep -iqE '^[[:space:]]*\[codex\]'; then
    printf '%s\n' "$first"
    return 0
  fi
  printf '%s\n' "$text" | grep -m1 -iE '^[[:space:]]*co-authored-by:.*(claude|codex|copilot|gpt|gemini|anthropic\.com|openai\.com)' && return 0
  printf '%s\n' "$text" | grep -m1 -E '^[[:space:]]*[Cc][Oo]-[Aa][Uu][Tt][Hh][Oo][Rr][Ee][Dd]-[Bb][Yy]:(.*[^[:alnum:]])?AI([^[:alnum:]]|$)' && return 0
  printf '%s\n' "$text" | grep -m1 -iE 'generated with' && return 0
  printf '%s\n' "$text" | grep -m1 -F "$GFTB_ROBOT" && return 0
  return 1
}

# Run the hook of the same name from the global core.hooksPath, when one is
# set, executable, and a different directory, so a machine-wide hook layer
# keeps running. Never chains twice and never chains to itself.
# $1 hook name, $2 file holding the stdin to replay (or empty), rest: args.
gftb_chain() {
  local hook="$1" input="$2" global here there
  shift 2
  [ -z "${GFTB_HOOKS_CHAINED:-}" ] || return 0
  global="$(git config --global --type=path --get core.hooksPath 2>/dev/null || true)"
  [ -n "$global" ] || return 0
  here="$(cd "$(dirname "$0")" && pwd -P)" || return 0
  there="$(cd "$global" 2>/dev/null && pwd -P)" || return 0
  [ "$here" != "$there" ] || return 0
  [ -f "$there/$hook" ] && [ -x "$there/$hook" ] || return 0
  export GFTB_HOOKS_CHAINED=1
  if [ -n "$input" ]; then
    "$there/$hook" "$@" < "$input"
  else
    "$there/$hook" "$@"
  fi
}
