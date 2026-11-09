# agents-overlay

This branch lives only on the contributor's fork. It holds the agent tooling
for this repository (agent instructions, skills, plugin links, agent docs and
past agent notes) that no longer ships on upstream `main`.

- It is an orphan branch with no shared history with `main`.
- It is never merged, never pushed to the organization repository, and never
  used as the base or head of a pull request.
- Its files are copied into a checkout as untracked files and listed in the
  clone's `info/exclude`, so they stay out of every commit and pull request.

## Use it

From inside any checkout or worktree of this repository:

```sh
git fetch origin agents-overlay && git show origin/agents-overlay:materialize.sh | bash
```

Preview first with `... | bash -s -- --dry-run`. The script is safe to run
again; it refreshes the files and adds each exclude entry once. It never
overwrites a path tracked in the current `HEAD` (older branches still carry
some of these files) and reports each one it skips.

## Change it

Edit on this branch in a separate worktree
(`git worktree add <dir> agents-overlay`), commit signed, push to `origin`
only, then run `materialize.sh` again in each checkout.
