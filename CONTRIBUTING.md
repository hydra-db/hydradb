# Contributing to HydraDB

Contributions follow a gated flow to keep review load sustainable. Please read
this before opening a pull request. Join us on
[Discord](https://discord.gg/YPtwenvxY8): discussing a PR, getting it reviewed
and merged, and getting an open PR closed all happen in the `#pr-discussion`
channel.

## The flow

```
open an issue  ->  get vouched  ->  issue /approved  ->  open a PR  ->  review & merge
   (anyone)        (maintainer)       (maintainer)        (you)
```

1. Open an issue describing the bug or change. Anyone can open and discuss issues.
2. Get vouched. A maintainer must vouch for you before your issues can be approved.
3. Get the issue `/approved` by a maintainer once the discussion settles.
4. Open a PR that links the approved issue (`Closes #123`). A PR without a linked, approved issue is closed automatically.
5. Keep the PR small and scoped to that issue. Review and merge are coordinated in the `#pr-discussion` channel on Discord.

## Vouching

Being vouched means a maintainer is willing to shepherd your contributions.

- Only maintainers vouch, by commenting `/vouch @your-github-handle` on an issue.
- Each maintainer may vouch at most 5 people.
- The roster is [`.github/vouched.json`](.github/vouched.json), recording who vouched whom.
- Maintainers are implicitly vouched.

If you are not vouched yet, you can still open and discuss issues; you just need
a vouch before an issue of yours can be `/approved`. Ask on Discord.

## Pull request rules

- A linked `/approved` issue is required (`Closes #N`), or the PR is closed.
- At most 3 open PRs per person. The newest over that is closed; get an earlier one merged or closed first (ask in `#pr-discussion` on Discord).
- Keep PRs small and scoped. Large or unrelated changes are asked to split.
- Describe breaking changes explicitly.
- Fill in the AI-assistance section of the PR template and do not remove it. You are responsible for every line you submit.

## One PR, one thread

Each PR gets a single thread in the `#pr-discussion` channel on Discord. Keep
conversation about a change in that one thread (and the PR itself) so the
history stays readable.

## Building and testing

See [`README.md`](README.md) for setup and [`DEVELOPMENT.md`](DEVELOPMENT.md)
for the workflow. Run tasks through the `justfile` (`just --list`), not bare
`cargo`.
