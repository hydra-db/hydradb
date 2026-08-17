# Contributing to HydraDB

Thanks for taking the time. This file is about process: what to open, what to
expect from us, and how a change gets reviewed. It does not repeat the build
documentation.

- **[DEVELOPMENT.md](DEVELOPMENT.md)** — recipes and the test harness.
- **[AGENTS.md](AGENTS.md)** — repository conventions and the local run-through.
  Read the failure-modes table before debugging a build or a node that will not
  start; it will probably save you an hour.
- **[architecture.md](architecture.md)** — storage model, query pipeline, writer
  coordination, index lifecycle, failure semantics.
- **[rfcs/](rfcs/README.md)** — the design process, for the small set of changes
  that need one.

## Licensing

HydraDB is licensed under **AGPL-3.0** ([LICENSE](LICENSE)). Contributions are
accepted under the same licence: by opening a pull request you agree that your
work is licensed under AGPL-3.0 and that you have the right to submit it. There
is no CLA and nothing to sign.

If you are contributing on behalf of an employer, confirm you have their
permission before you open the pull request rather than after.

## Start with an issue

Everything starts as an issue, including changes from maintainers. It costs you
one issue instead of one weekend to find out that something is already being
worked on, already rejected, or blocked behind something else.

Most work stops there and goes straight to a pull request. A few things need a
design document first:

| You want to | Do this | RFC? |
|---|---|---|
| Fix a typo or a documentation error | Pull request, no issue needed | No |
| Report a wrong query result | Issue with the `CREATE` **and** the `MATCH` | No |
| Fix a bug | Issue, then a pull request referencing it | No |
| Improve performance | Issue with a benchmark number, then a pull request | No |
| Add a Cypher function or clause | Issue first; we will say if it needs more | Maybe |
| Change the on-disk or wire format | Issue, then an [RFC](rfcs/README.md) | **Yes** |
| Change query semantics, or remove anything user-visible | Issue, then an [RFC](rfcs/README.md) | **Yes** |
| Anything over a week, or spanning multiple components | Issue, then an [RFC](rfcs/README.md) | **Yes** |

For a wrong query result, the `CREATE` that built the data and the `MATCH` that
misbehaves are what make the report reproducible. Without both there is nothing
for us to run.

For performance work, bring a measurement rather than a design document. The
[benchmark site](https://hydra-db.github.io/benchmark/) publishes the current
numbers and `scripts/` has the harnesses.

This is a database, and the three RFC rows are there for one reason: changes to
query semantics, the wire protocols, or the object-store layout have
consequences that outlive the release, and they are much cheaper to argue about
in an issue than in a review of a finished branch.

## Claiming an issue

Comment on it. That holds it for a week, after which it is fair game again. If
you need longer, say so on the issue; nobody will take it from you for being
slow, only for being silent.

Please check for an open pull request before starting. Duplicate work on the
same issue is the most expensive thing that happens in this repository, because
it costs two contributors' time and two reviews.

## Building and testing

Full instructions are in [AGENTS.md](AGENTS.md). Two things matter enough to
repeat here.

**Build through `just`, not bare `cargo`.** The justfile exports three variables
that builds require: `BINDGEN_EXTRA_CLANG_ARGS` and `LIBRARY_PATH` so bindgen
and the linker find Homebrew's `libcypher-parser` on macOS, and
`RUST_MIN_STACK=33554432` on every platform, without which OpenCypher's async
futures overflow the default test-thread stack. Missing the third gives you a
node that builds, serves `/readyz`, and then aborts on the first query, which
looks like a query bug and is not. Anything under `scripts/` invokes `cargo`
directly and does not inherit these; export them yourself.

**Run CI on your fork before opening the pull request.** `just ci` reproduces
the checks locally, and GitHub Actions is free for public forks. The full run
takes about 25 minutes and there is a review queue, so a pull request that
arrives already green gets looked at sooner. If you only ran part of it, say
which part rather than leaving the reviewer to guess.

Do not open draft pull requests. Some checks do not run on drafts, so a draft
is a pull request nobody can evaluate. Open it when it is ready, or say
"please don't merge yet" in the description.

## Pull requests

Work on a branch in a fork and open the pull request against `main`.

**Keep them small.** A large pull request is not more impressive, it is less
likely to be merged, because it is harder to review and a reviewer who cannot
hold it in their head will not approve it. If the work is genuinely large,
split it into a sequence that each land on their own, and say in the first one
where the sequence is going.

**Add coverage for behavioural changes.** Changes to storage, fencing,
snapshots, routing, or index publication should state the invariant they
preserve and include a test that fails without the fix.

**Explain why, not just what.** The diff shows what changed. The description
should say what was wrong and why this is the right repair.

We reserve full and final discretion over what we merge. Following this guide
makes a pull request likely to land; it does not guarantee it. Where we say no,
we will say why.

## AI-assisted contributions

Welcome, with conditions. This repository ships `AGENTS.md` and `CLAUDE.md`, so
we are not going to pretend otherwise. What we ask:

- Say that the change is largely AI-generated, and which tool and model.
- Understand the change. You will be asked about it in review, and "the model
  wrote it" is not an answer we can act on.
- Review it yourself before submitting, and run `just ci` on your fork.
- Write the pull request description and the review replies yourself.

The reason for the disclosure is practical rather than ideological. It tells the
reviewer which question to ask first, and it is the difference between a
generated patch that gets merged and one that stalls.

## What we will not merge

Saying this once is cheaper than saying it per pull request.

- Pull requests that only reformat code, rename things for taste, or fix a
  single typo. Bundle typo fixes into a documentation pull request instead.
- Dependency bumps outside Dependabot.
- New dependencies without a justification in the description. This binary
  links GraphBLAS and `libcypher-parser` already; every addition is a build
  someone has to make work on two platforms.
- Changes to the workspace dependency table in `Cargo.toml` that do not engage
  with the comments already there. They record why specific versions and
  features are pinned, and each one is load-bearing.
- Large pull requests, as above.

## What you can expect from us

- A first response within two weeks. If we are going to say no, we would rather
  say it early than leave you waiting.
- A reason when we close something.
- A pull request with no linked issue that changes query semantics, the wire
  protocols, or the object-store layout will be closed with a pointer to the
  table above. That is not a judgement on the code.

If a pull request of yours has gone quiet past that window, comment on it. That
is a reasonable thing to do and not a nuisance.

## Code of conduct

Be straightforward and assume good faith. Report anything that falls short to
the maintainers.
