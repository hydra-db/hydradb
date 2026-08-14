# Contributing to HydraDB

Thanks for taking the time. This file covers the process — how to get set up,
what CI expects, and how a change gets reviewed. It deliberately does not repeat
the build documentation:

- **[DEVELOPMENT.md](DEVELOPMENT.md)** — the recipe and harness surface.
- **[AGENTS.md](AGENTS.md)** — repository conventions, the local run-through,
  and the failure modes that cost the most time to rediscover. Read the
  "Failure modes worth knowing before you debug" table before debugging a build
  or a node that will not start; it will probably save you an hour.
- **[architecture.md](architecture.md)** — storage model, query pipeline, writer
  coordination, index lifecycle, failure semantics.

## Licensing

HydraDB is licensed under the **AGPL-3.0** ([LICENSE](LICENSE)). Contributions
are accepted under the same licence — by opening a pull request you agree that
your work is licensed under AGPL-3.0 and that you have the right to submit it.
There is no CLA and nothing to sign.

If you are contributing on behalf of an employer, make sure you have their
permission before you open the pull request rather than after.

## Ways to contribute

For a wrong query result, the `CREATE` that built the data and the `MATCH` that
misbehaves are what make the report reproducible; without both there is nothing
to run. Documentation corrections are welcome as ordinary pull requests with no
issue first.

For anything larger, **open an issue before writing the code.** This is a
database: changes to query semantics, the wire protocols, or the object-store
layout have consequences that are cheaper to argue about in an issue than in a
review of a finished branch.

## Getting set up

Full instructions are in [AGENTS.md](AGENTS.md), which walks the whole sequence
end to end. The short version:

```bash
# macOS
brew install just cmake pkg-config llvm suite-sparse
brew install cleishm/neo4j/libcypher-parser

# Ubuntu / WSL
sudo apt-get install -y build-essential clang libclang-dev cmake pkg-config \
  libcypher-parser-dev libgraphblas-dev python3-venv
cargo install just --locked
```

Then confirm the native libraries resolve before compiling anything:

```bash
just native-check   # silence and exit 0 mean both were found
just smoke          # local object-store round trip
```

`libcypher-parser` is not in homebrew-core, hence the `cleishm/neo4j/` tap.
SuiteSparse GraphBLAS is **not** a Cargo feature — it is linked unconditionally,
so its headers and library are required even for a plain `cargo build`.

### Build through `just`, not bare `cargo`

This matters enough to state on its own. The justfile exports three variables
that builds need:

| Variable | Why |
|---|---|
| `BINDGEN_EXTRA_CLANG_ARGS` | macOS: bindgen must find `cypher-parser.h` |
| `LIBRARY_PATH` | macOS: the linker must find Homebrew's libraries |
| `RUST_MIN_STACK=33554432` | every platform: OpenCypher's async futures overflow the default test-thread stack |

Without the third, a node builds fine, serves `/readyz`, and then aborts on the
first query — which looks like a query bug and is not. CI sets these in the
workflow environment, so a bare `cargo` line that passes in CI can still fail on
your machine. `just --list` shows every recipe.

Anything under `scripts/` invokes `cargo` directly and so does **not** inherit
these. Export them yourself before running a script.

## What CI checks

`.github/workflows/ci.yml` runs three jobs on every pull request:

| Job | What it covers |
|---|---|
| **Helm chart** | Renders the chart under every shipped values file, asserts the security defaults stay closed, and validates the output with kubeconform |
| **Linux feature matrix** | Formatting, six clippy configurations at `-D warnings`, both workspace crates, the compile-only feature combinations, the full test matrix, and an end-to-end runtime smoke against a real Neo4j driver |
| **macOS default features** | The default and chaos-harness feature sets, because macOS breaks in ways Linux cannot see |

Two more workflows run alongside it: `container.yml` builds the production image
for `linux/amd64` and `linux/arm64`, and `opencypher-tck.yml` regenerates the
openCypher TCK compatibility report.

**Reproduce the Linux job locally with `just ci`**, or equivalently
`bash scripts/ci_local.sh`, which sets a shared Cargo target directory and
delegates to it. It covers every lint, check and test step; the one thing it
does not run is the runtime smoke, which is `bash scripts/runtime_smoke.sh` and
needs the `neo4j` Python package on the interpreter you point `PYTHON` at.

Every step in that job names the `just` recipe it mirrors, in the same order, so
the two are meant to be edited together — if you add a feature combination, add
it to the justfile's `ci` recipe *and* to the workflow in the same commit.

It is a long run — 25m 41s for the clean run recorded in DEVELOPMENT.md. If you
only ran part of it, say which part in the pull request rather than leaving the
reviewer to guess.

## Pull requests

Work on a branch in a fork and open the pull request against `main`.

**`main` is rebased and moves fast.** Before concluding that a bug exists,
confirm you are on the current tip; before concluding a local branch has unique
work, compare commit *subjects* against `origin/main` rather than SHAs.

Commit subjects are written in the imperative mood ("Bound sparse WAL history",
not "Bounded" or "Bounds"). An area prefix — `ci:`, `docs:`, `chore:` — is
common and encouraged where it fits, but is not enforced. Keep the subject under
about 72 characters and put the reasoning in the body.

What review looks for, beyond correctness:

- **A test that fails without the change.** For a query fix, that usually means
  a case in the relevant test module, not a manual reproduction in the
  description.
- **Comments that explain why, not what.** This is the most distinctive
  convention in the codebase — read the `[workspace.dependencies]` block in
  `Cargo.toml` or almost any recipe in the justfile for the house style. A
  non-obvious feature flag, a pinned version, a `--all-targets` that is
  load-bearing: all of these get a comment saying what breaks without them.
  A comment that restates the code is worse than none.
- **Feature gating that holds up.** The feature matrix is large and combinations
  do not subsume one another — a lint or a compile error only fires inside the
  cfg arms its feature set compiles. If you add a gate, check it against the
  configurations CI builds.
- **Scope.** Unrelated cleanups in the same branch make a change harder to
  review and harder to revert. Open a second pull request.

Design documents for larger work go in `docs/plans/`, named
`YYYY-MM-DD-kebab-case-title.md` and opening with a YAML frontmatter block;
[AGENTS.md](AGENTS.md#repository-conventions) documents the format. Agree the
shape in an issue first, then write the plan.

## Reporting bugs and vulnerabilities

Bugs go through the [issue templates](.github/ISSUE_TEMPLATE). The forms ask for
a version, a platform, the object-store backend and the sparse kernel because
those four decide which code path ran.

**Security vulnerabilities do not go in issues.** Report them privately to the
maintainers; [SECURITY.md](SECURITY.md) covers what to include.
