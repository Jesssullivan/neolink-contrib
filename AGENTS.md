# neolink: agent contract (fork-only overlay)

This file lives on the `agents-overlay` branch of Jesssullivan/neolink-contrib
only. `materialize.sh` copies it into a checkout as an untracked, excluded
file. Never commit it to a working branch, and never push it to xoxd-ai/neolink,
alexbahder/neolink or QuantumEntangledAndy/neolink.

## Project

This is the Penguin Receiver base: Reolink (Baichuan) cameras into UniFi
Protect and into Murmur. Linear initiative I-83, project P-TIN-231.

| Issue | What |
|---|---|
| TIN-8094 | Custody, hooks and CI |
| TIN-8095 | Review of the alexbahder fork and a reproduction harness |
| TIN-8096 | Hand-off PR to alexbahder |
| TIN-8097 | Receiver research |
| TIN-8098 | Receiver MVP |

The live board is the I-83 board document on Linear.

Rulings: R-MURMUR-20261109-a to -d (Linear TIN-7811, comment 0fd755cd).

## Start here

1. `git status --short --branch` and `git remote -v`. Confirm the remotes:

   | remote | repository | push |
   |---|---|---|
   | origin | neolink-contrib | allowed |
   | estate | xoxd-ai/neolink | allowed |
   | alex | alexbahder/neolink | DISABLED-fork-first |
   | upstream | QuantumEntangledAndy/neolink | DISABLED-fork-first |

2. Run `just hooks-install` once per clone. Its checks:
   - `just hooks-test` runs the hook tests.
   - `just hooks-check` checks the hooks against the mirror.
   - `just attribution-check` checks for AI attribution.
3. Read `ESTATE.md`.

## Rules

- **No AI attribution.** No Co-authored-by trailer naming an AI, no
  "Generated with" line, no robot emoji and no `[codex]` prefix in commits,
  PR titles or PR bodies. The hooks refuse these.
- **Never rewrite alexbahder history.** Commit f1a9ae72 carries an AI
  co-author trailer. It is already on a remote-tracking ref, so the hooks
  skip it.
- **Signed commits.** Use conventional subjects with a TIN ticket. PR
  bodies start with `Linear:`. Write to the xoxd-lexical digest: no em
  dashes and no marketing words.
- **Estate PRs only.** Agents open PRs only into xoxd-ai/neolink, from a
  neolink-contrib branch, and always pass `--repo xoxd-ai/neolink`. Without
  it, gh targets the parent, alexbahder.
- **PRs to alexbahder or upstream go by hand-off.** Cut a branch from
  `alex/master` that holds only fix commits and no estate tooling. Put the
  copy-ready PR text in the Linear hand-off issue (TIN-8096). The operator
  opens the PR in Chrome.
- **Bench cameras.** Credentials stay out of git and out of Linear. Keep
  camera config in an ignored `neolink.toml` or SOPS.
- **Durable notes** go to Linear comments on the owning issue. This repo
  has no agent-notes directory.
