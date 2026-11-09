# xoxd-ai/neolink

This is the xoxd-ai estate copy of neolink. It is forked from
[alexbahder/neolink](https://github.com/alexbahder/neolink), which carries
RTSP stability and A/V sync fixes. That fork in turn comes from
[QuantumEntangledAndy/neolink](https://github.com/QuantumEntangledAndy/neolink).
It is the base for the Penguin Receiver, which brings Reolink cameras into
UniFi Protect and Murmur (Linear P-TIN-231).

## Remotes

| remote | repository | push |
|---|---|---|
| origin | Jesssullivan/neolink-contrib | yes, working branches |
| estate | xoxd-ai/neolink | through pull requests only |
| alex | alexbahder/neolink | disabled |
| upstream | QuantumEntangledAndy/neolink | disabled |

A pull request to alexbahder/neolink or upstream is opened by the maintainer
by hand. It is cut from `alex/master` and holds only the fix commits.

## Rules

- No agent instructions, skills or agent notes on any branch of this
  repository. They live on the `agents-overlay` branch of the contrib fork
  and are excluded per clone.
- `.githooks/` is byte-identical to the Great Falls Tool Bus mirror
  (`just hooks-check`). It refuses AI attribution and unsigned commits in the
  pushed range. Commits that already exist on a remote-tracking branch, such
  as alexbahder history, are not checked again.
- Run `just hooks-install` once per clone.
