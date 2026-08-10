# AGENTS.md

Operating instructions for AI coding agents working in this repository. Human
documentation lives in `README.md` and `architecture.md`; this file is the
executable subset — what to run, what breaks, and what not to do.

## What this repository is

`slatedb-graph-kernel` (product name **Turbolay**) is a Rust graph database
built on [SlateDB](https://github.com/usecortex/slatedb). An object store — S3
in production, a local directory in development — is the durable source of
truth. Local SSD/NVMe and memory are caches.

The split of responsibility matters when diagnosing a failure:

- **SlateDB owns** writer fencing, WAL durability, storage snapshots,
  compaction, and object-store coordination. It is a pinned Git dependency in
  `Cargo.toml`, not vendored. Errors of the form `Error: Slate(...)` come from
  it, not from this crate.
- **This crate owns** the graph model, topology artifacts, traversal kernels,
  the query planner, the public protocols, and the operational harnesses.

One SlateDB writer and any number of readers exist per graph store. Nodes are
stateless and controllerless: every node can read every configured cell, and a
write request lazily opens a cached writer, relying on SlateDB's writer epoch
and WAL barrier for safety. Derived topology (CSC indexes) is never written back
into SlateDB — indexer workers publish immutable objects under
`_graph_index/<cell>/<edge-type>/generations/` and advance a pointer by
compare-and-swap.

Clients speak **Bolt 5.1-5.4** (Neo4j-driver compatible) or an **HTTPS query
API**. Query language is OpenCypher, parsed through `libcypher-parser`.

## Layout

```text
src/core/          configuration, error types, public model types, cache policy
src/shard/         GraphShard lifecycle, writes, reads, query execution
src/engine/        immutable graph indexes, GC, routed multi-reader runtime
src/query/         OpenCypher lowering, algebra, optimizer, transport, TCK
src/client/        shared query service plus Bolt/TLS and HTTPS adapters
src/sparse_kernel/ Rust sparse traversal and SuiteSparse GraphBLAS FFI
src/bin/           graph-node and graph-indexer binaries
crates/placement/  cell placement
crates/telemetry/  tracing and OTLP export
examples/          smoke, stress, correctness, benchmark, profiling binaries
scripts/           local, MinIO, query, stress, and chaos harnesses
charts/turbolay/   production Helm chart
docs/plans/        design plans, dated; see conventions below
docs/runbooks/     operational procedures
interactive/       standalone HTML design documents
architecture.md    system design and component flows
```

Two binaries: `graph-node` (serves reads and canonical writes) and
`graph-indexer` (builds CSC generations asynchronously).

## Build and test through the justfile

**Run `just <recipe>`, never bare `cargo`.** The justfile exports three
variables (justfile:11-19) that builds require:

| Variable | Why |
|---|---|
| `BINDGEN_EXTRA_CLANG_ARGS` | macOS: bindgen must find `cypher-parser.h` |
| `LIBRARY_PATH` | macOS: linker must find Homebrew libs |
| `RUST_MIN_STACK=33554432` | every platform: OpenCypher async futures overflow the default stack |

CI is Linux and sets these elsewhere, so a bare `cargo` line that passes in CI
fails on macOS. `just --list` shows every recipe. Common ones:

```bash
just native-check   # verify native libs; silent + exit 0 means pass
just smoke          # local object-store smoke test
just test           # library tests
just ci             # full CI-equivalent set (long)
just clippy         # lint the default feature set
just fmt            # format
```

**Anything under `scripts/` invokes `cargo` directly** and therefore does not
inherit that environment. Export it yourself before running a script.

**There is no `just` recipe that builds `graph-node`**, so step 7 below runs
`cargo build` directly. That is the one sanctioned exception, and it is why that
step exports the same three variables the justfile would have.

## Running it locally

The complete sequence, executed end to end on macOS (arm64) and on a clean
Ubuntu 24.04 container. Everything runs one process against the local
filesystem: no Docker, no S3, no Kubernetes. `README.md`'s "Run a local server"
covers the same ground for humans, in less detail.

The ports and paths here (`17687`/`18443`/`19091`, `/tmp/sgk-*`) deliberately
differ from the README's (`7687`/`8443`/`9090`, `.turbolay/`) so an agent's node
cannot collide with one a developer is already running, and so nothing is
written inside the checkout.

**Environment does not survive between steps.** If each command you run gets a
fresh shell — which is true for most agent tool calls — then every `export` is
gone by the next step, **and so is the working directory**. Do not assume
otherwise. Step 3 writes the environment, including the repository root, to a
file; every later step opens by sourcing it and returning to that directory.
Those lines are not optional decoration.

Every command below runs from the repository root unless stated otherwise.

**Two different stores.** Steps 3-6 use `/tmp/sgk-store` for the harnesses;
step 7 uses `/tmp/sgk-local/store` for the long-running node. That is
deliberate, so a harness run cannot disturb a server you are keeping alive.
Step 7 sources the same file for `REPO_ROOT` and the build paths, then overrides
`LOCAL_PATH` with its own store.

**Builds are slow.** Step 4 compiles the dependency tree (~1-3 minutes cold) and
step 6 compiles again with more features enabled (~1 minute more). Step 7's
build is normally a cache hit and returns immediately. Set tool timeouts
accordingly; a long silence during these is normal, not a hang.

### 1. Native dependencies

macOS:

```bash
# Check first — `brew list --versions just cmake pkg-config llvm suite-sparse
# libcypher-parser` lists what you already have. Reinstalling costs minutes.
xcode-select --install
brew install just cmake pkg-config llvm suite-sparse
brew install cleishm/neo4j/libcypher-parser

# Rust, only if `rustup toolchain list` does not already show a stable toolchain:
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Ubuntu or WSL:

```bash
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev cmake pkg-config \
  libcypher-parser-dev libgraphblas-dev python3-venv curl git
cargo install just --locked
```

`libcypher-parser` is not in homebrew-core; the `cleishm/neo4j/` prefix adds the
tap.

Rust comes from the official installer rather than Homebrew on purpose.
Homebrew's `rustup` formula no longer ships a `rustup-init` binary **and is
keg-only**, so `brew install rustup` leaves nothing named `rustup` on `PATH` —
the next command fails with `command not found`. Any rustup-managed stable
toolchain works; `rust-toolchain.toml` pins `channel = "stable"`.

### 2. Verify the native libraries

```bash
just native-check
```

Silence and exit 0 mean both resolved. It compiles nothing: it runs
`pkg-config --exists cypher-parser` and a `test -f` for `libgraphblas`. Do not
proceed past a failure here — everything after it will fail more confusingly.

### 3. Write the environment file

Everything from here on needs these variables. Write them once:

Run this **from the repository root** — it records that path so later steps can
return to it, since a fresh shell starts wherever the harness puts it.

```bash
mkdir -p /tmp/sgk-store
printf 'export REPO_ROOT=%q\n' "$PWD" > /tmp/sgk-env.sh
cat >> /tmp/sgk-env.sh <<'EOF'
export CLOUD_PROVIDER=local        # object store: a local directory, not S3
export LOCAL_PATH=/tmp/sgk-store   # must already exist; not created for you
export RUST_MIN_STACK=33554432     # every platform; the node aborts without it
export SGK_VENV=/tmp/sgk-venv      # created in step 5; used by steps 6 and 8
# macOS: scripts call cargo directly and miss the justfile's exports.
# On Linux apt already puts these on the default search paths.
if command -v brew >/dev/null; then
  export BINDGEN_EXTRA_CLANG_ARGS="-I$(brew --prefix)/include"
  export LIBRARY_PATH="$(brew --prefix)/lib"
fi
EOF
```

### 4. Smoke-test the storage kernel

```bash
source /tmp/sgk-env.sh && cd "$REPO_ROOT"
just smoke
```

Prints `graph object-store smoke passed at epoch 10`. In-process, exits. Does
not start a server. First run compiles the dependency tree — allow several
minutes.

### 5. Python driver for the Bolt checks

```bash
source /tmp/sgk-env.sh
python3 -m venv "$SGK_VENV" && "$SGK_VENV/bin/pip" install neo4j
```

Both Homebrew's and Debian's Python refuse a bare `pip install` under PEP 668,
hence the virtualenv. Safe to re-run: `venv` reuses an existing directory and
`pip` reports the package already satisfied.

### 6. Full runtime smoke

```bash
source /tmp/sgk-env.sh && cd "$REPO_ROOT"
PYTHON="$SGK_VENV/bin/python" bash scripts/runtime_smoke.sh
```

Prints `runtime-smoke-ok`. Builds `graph-node` with `--features server-runtime`,
starts it, polls `/readyz`, drives it over Bolt and HTTP, then stops it. The
extra features mean another minute of compiling. Node log at
`/tmp/sgk-runtime-smoke/node.log` — read it first on failure.

### 7. Start a node you can connect to

`runtime_smoke.sh` stops the node when it finishes. To leave one running:

```bash
source /tmp/sgk-env.sh && cd "$REPO_ROOT"   # for REPO_ROOT and the brew paths

# Start from an empty store. Step 8 is not idempotent: each CREATE adds another
# edge, and a second run makes the Bolt check below fail with
# ResultNotSingleError. The cache directory holds state too, so wipe both.
ROOT=/tmp/sgk-local
rm -rf -- "$ROOT"
mkdir -p "$ROOT/store" "$ROOT/cache"
printf '%s\n' 'local-dev-auth-token-32-characters-long' >"$ROOT/auth-token"

export CLOUD_PROVIDER=local LOCAL_PATH="$ROOT/store"
export GRAPH_NAMESPACE=local GRAPH_ID=default
export GRAPH_CELL_ID=cell-0 GRAPH_CELLS=cell-0 GRAPH_DATA_PATH=data
export GRAPH_ALLOW_PLAINTEXT=true GRAPH_AUTH_TOKEN_FILE="$ROOT/auth-token"
export GRAPH_DATA_CACHE_BYTES=67108864 GRAPH_DATA_CACHE_DIR="$ROOT/cache"
export GRAPH_NODE_ID=node-0
export GRAPH_BOLT_ADDR=127.0.0.1:17687 GRAPH_ADVERTISED_BOLT_ADDR=127.0.0.1:17687
export GRAPH_BOLT_NODE_ADDRESSES=node-0=127.0.0.1:17687
export GRAPH_HTTP_ADDR=127.0.0.1:18443 GRAPH_ADMIN_ADDR=127.0.0.1:19091
export RUST_MIN_STACK=33554432 RUST_LOG=info

# Normally a cache hit after step 6, so this returns immediately.
cargo build --locked --features server-runtime --bin graph-node

# Started in the background on purpose: graph-node never returns, so running it
# in the foreground blocks this block forever. nohup + disown detach it from
# this shell, so it survives the shell exiting -- which is what happens when an
# agent tool call returns. The PID goes to a file because $node_pid does not
# survive either.
nohup target/debug/graph-node >"$ROOT/node.log" 2>&1 &
node_pid=$!
disown "$node_pid" 2>/dev/null || true
echo "$node_pid" >"$ROOT/node.pid"

until curl -fsS http://127.0.0.1:19091/readyz >/dev/null 2>&1; do
  kill -0 "$node_pid" 2>/dev/null || break
  sleep 1
done

if curl -fsS http://127.0.0.1:19091/readyz >/dev/null 2>&1; then
  echo "ready, pid $node_pid (also in $ROOT/node.pid)"
else
  echo "node failed to start:"; tail -20 "$ROOT/node.log"
fi
```

Run that whole block as one unit — the background start depends on `$ROOT` and
every export above it. It reports failure rather than calling `exit`, so pasting
or sourcing it cannot kill your shell. If you would rather watch the log live,
replace everything from `nohup` down with plain `target/debug/graph-node`; it
will hold that terminal, and step 8 then needs a second shell.

All of those variables are required. The auth token must be at least 32
characters. `GRAPH_ALLOW_PLAINTEXT=true` disables the TLS requirement on both
public adapters — local development only.

| Endpoint | Address |
|---|---|
| Bolt | `127.0.0.1:17687` |
| HTTP query API | `127.0.0.1:18443` |
| Admin, health, metrics | `127.0.0.1:19091` |

**Stopping and resetting.** From any shell:

```bash
kill "$(cat /tmp/sgk-local/node.pid)"     # or: pkill -f target/debug/graph-node
```

`$node_pid` is gone in a fresh shell, which is why the PID is also on disk. To
start over from a clean state, stop the node and re-run step 7 from the top —
its `rm -rf` is what resets the store.

### 8. Prove it works

```bash
curl -fsS http://127.0.0.1:19091/readyz && echo READY
curl -fsS http://127.0.0.1:19091/metrics | grep graph_runtime_ready
```

```bash
TOKEN=local-dev-auth-token-32-characters-long

curl -fsS -X POST http://127.0.0.1:18443/v1/graphs/default/query \
  -H "Authorization: Bearer $TOKEN" -H 'X-Graph-Namespace: local' \
  -H 'Content-Type: application/json' \
  --data '{"cell_id":"cell-0","query":"CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})"}'

curl -fsS -X POST http://127.0.0.1:18443/v1/graphs/default/query \
  -H "Authorization: Bearer $TOKEN" -H 'X-Graph-Namespace: local' \
  -H 'Content-Type: application/json' \
  --data '{"cell_id":"cell-0","query":"MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id"}'
```

The `CREATE` returns an envelope with no rows — `"columns":[]`, `"rows":[]`,
`"read_epoch":null`. That is correct for a mutation, not a failure.

The `MATCH` is the real check. What matters is that its `rows` contain exactly
one row holding `{"type":"vertex_id","value":2}`:

```json
{"query_id":"http-query-2","columns":["id"],
 "rows":[[{"type":"vertex_id","value":2}]],
 "read_epoch":1,"next_cursor":null,
 "bookmark":"sgk:1:6c6f63616c:64656661756c74:63656c6c2d30:1"}
```

The `bookmark` is a SlateDB commit sequence with hex-encoded scope components;
its exact value varies and is not part of the check.

A listening port is not proof; a round-tripped write is. `X-Graph-Namespace`
must match `GRAPH_NAMESPACE`.

If `rows` holds **two** entries, the store was not empty and the `CREATE` ran a
second time. Redo step 7 from its `rm -rf`.

Over Bolt, with any Neo4j driver:

```bash
source /tmp/sgk-env.sh
"$SGK_VENV/bin/python" - <<'PY'
from neo4j import GraphDatabase
TOKEN = "local-dev-auth-token-32-characters-long"
with GraphDatabase.driver("bolt://127.0.0.1:17687", auth=("neo4j", TOKEN)) as d:
    d.verify_connectivity()
    with d.session(database="default") as s:
        print(dict(s.run("MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id").single(strict=True)))
PY
```

Prints `{'id': 2}`.

All scratch paths above (`/tmp/sgk-*`) are disposable. `/tmp` is cleared on
reboot; use a persistent directory for data you intend to keep.

## Failure modes worth knowing before you debug

These cost real time to rediscover. Check them before investigating anything
deeper.

| Symptom | Cause |
|---|---|
| `invalid environment variable CLOUD_PROVIDER value \`null\`` | `CLOUD_PROVIDER` unset. `null` means absent, not the string. Accepted: `local`, `memory`, `aws`, `azure`, `gcp`. `local` also needs `LOCAL_PATH`, pointing at a directory that **already exists**. |
| `wrapper.h:4:10: fatal error: 'cypher-parser.h' file not found` | `BINDGEN_EXTRA_CLANG_ARGS` unset on macOS. Use `just`, or export it. |
| Node serves `/readyz`, then aborts with `has overflowed its stack` on the first query | `RUST_MIN_STACK` unset. The node starts fine and dies on first use, so this looks like a query bug and is not. |
| `curl: (7) Failed to connect ... 19091` | `graph-node` runs in the **foreground** and never returns. That is it working. Start it in its own shell. |
| `cannot close database reader while snapshots are active` | A `GraphSnapshot` is still alive when `close()` runs. Drop it first. Fixed on current `main`; appears on older commits. |
| `No available formula with the name "libcypher-parser"` | Not in homebrew-core. `brew install cleishm/neo4j/libcypher-parser`. |

## Repository conventions

**Never use the Artifact tool in this repository.** Visual and long-form
deliverables are standalone HTML files written into `interactive/` with ordinary
file writes. Requirements: complete document with `<!doctype html>`, `<meta
charset="utf-8">` and a viewport tag; must work by double-clicking a `file://`
URL; fully self-contained with no CDN scripts, external stylesheets, remote
fonts or images; light and dark following `interactive/assets/textbook.css`.
`interactive/README.md` documents the house style.

**Plan documents** live in `docs/plans/` and are named
`YYYY-MM-DD-kebab-case-title.md`, dated the day the plan was written, so the
directory sorts chronologically and staleness is obvious. Every plan opens with
YAML frontmatter:

```yaml
---
title: Sparse kernel backend consolidation
status: draft-for-review        # draft-for-review | step-N-complete | done | superseded
date: 2026-07-25
branch: Turbolay-V3.5
base_commit: 989cc72            # tree the plan was written against
head_commit: 73309df            # add once the work lands; omit while unstarted
tags:
  - sparse-kernel
  - refactor
---
```

A plan resting on prior analysis opens with a **Sources** section naming the
files that hold it — design notes under `interactive/`, memory entries, and
exact paths in any reference repo (`../sleet`, `../tidb-master`). Name the file
and what it holds, not just the repo, so the next session reads instead of
re-deriving. `docs/plans/2026-07-25-sparse-kernel-backend-consolidation.md` is
the reference example. `optimisation-phases.md` predates the convention; leave
it unless asked, since `build.rs:10` references it by name.

## Things that will mislead you

**`main` moves fast and is rebased.** Six hundred commits landed in a single day
during August 2026, and `origin/main` was force-pushed, leaving local clones
with diverged histories full of duplicate commits under different SHAs. Before
concluding that a bug exists, confirm you are on the current tip. Before
concluding a local branch has unique work, compare commit *subjects* against
`origin/main`, not SHAs.

**Sparse kernel selection is runtime, not compile time.** `GRAPH_SPARSE_KERNEL`
picks `adjacency`, `compact`, or `suitesparse` (default) and is read by the
`graph-node` binary only; it has no effect on `cargo test`. The older
`GRAPH_COMPILED_KERNEL=compact` changes the default the policy field starts at,
so it *does* apply to the library and its tests. `adjacency` is a capability
downgrade, not merely slower: no count or window pushdown, and it enforces scan
and row limits the compiled path does not. A large traversal that succeeds on
`suitesparse` can fail outright on `adjacency`.

**SuiteSparse GraphBLAS is not a Cargo feature.** It is always linked, via
`#[link(name = "graphblas")]`. Its headers and library are required for a plain
`cargo build`. `build.rs` resolves the link path itself — `GRAPHBLAS_LIB_DIR`,
then `pkg-config`, then `brew --prefix suite-sparse` — so do not add
`LIBRARY_PATH` or `RUSTFLAGS="-L ..."` for it.

**Query limits are real and enforced.** `max_query_scan_edges`,
`max_query_intermediate_rows`, `max_query_result_vertices`, byte budgets,
cancellation, and timeouts all apply. A query returning fewer rows than expected
may have hit a budget rather than a correctness bug.
