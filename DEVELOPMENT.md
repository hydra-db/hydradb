# Development

Detailed build, verification, and harness surface for contributors working on
HydraDB from source. See the [README](README.md) for a first-run walkthrough and
[AGENTS.md](AGENTS.md) for repository conventions and failure modes.

Run `just` or `just help` to list the command surface. Recipes use Bash and run
from the repository root. The full native suite requires `libcypher-parser` and
SuiteSparse GraphBLAS.

## Verification Recipes

| Recipe | Coverage |
|---|---|
| `just native-check` | Verifies that `cypher-parser` and GraphBLAS are discoverable |
| `just fmt`, `just fmt-check` | Formats Rust or checks formatting without modifying files |
| `just clippy` | Default-feature lint used by CI |
| `just clippy-chaos`, `just clippy-opencypher` | Chaos-harness and OpenCypher lint configurations |
| `just clippy-native`, `just clippy-client-protocols`, `just clippy-runtime` | Full native, public protocol, and production runtime lint configurations |
| `just check` | Checks every default-feature target |
| `just check-all-features` | Checks every target with every Cargo feature |
| `just check-client-api`, `just check-bolt-server` | Checks shared client code and standalone Bolt independently |
| `just check-examples`, `just check-examples-native`, `just check-examples-chaos` | Checks example targets under their supported feature sets |
| `just test [cargo test args]` | Runs default library tests and forwards optional arguments |
| `just test-opencypher`, `just test-native`, `just test-client-protocols`, `just test-chaos` | Runs the major library and public-protocol test matrices |
| `just test-server-runtime`, `just test-indexer`, `just test-node-otlp` | Runs graph-node, indexer, and OTLP binary tests |
| `just test-placement`, `just test-telemetry` | Lints and tests the two workspace crates |
| `just ci` | Runs the complete local CI-equivalent sequence; a clean feature-matrix run can take tens of minutes (25m 41s in the verification run for this README) |

`scripts/ci_local.sh` is a compatibility entry point that sets a shared Cargo
target directory and delegates to `just ci`, so the script and Justfile cannot
silently drift into different test matrices.

## Local Harnesses

| Recipe | What it runs | Output or side effect |
|---|---|---|
| `just smoke` | Isolated local object-store write, traversal, reopen, and verification | Temporary directory removed on exit |
| `just smoke-graphblas` | The same smoke flow with SuiteSparse selected explicitly | Temporary directory removed on exit |
| `just query-correctness` | Exact OpenCypher result checks | `bench-results/query_correctness.csv` and `.log` |
| `just stress` | Multiprocess writes, restart recovery, compaction, GC, and verification | Temporary local stores removed on exit |
| `just fence` | Hard SlateDB writer-takeover proof | Temporary local stores removed on exit |

## Docker And MinIO Harnesses

| Recipe | Purpose |
|---|---|
| `just minio-smoke` | Runs the object-store smoke flow against an ephemeral MinIO container |
| `just minio-query-correctness` | Runs exact query checks against MinIO |
| `just minio-chaos` | Pauses, restarts, and recovers MinIO during graph operations |
| `just minio-fence` | Runs writer takeover against MinIO |
| `just minio-mbt` | Replays the formal MBT adapters against MinIO; unavailable in this checkout because the referenced `tests/formal_mbt*.rs` targets are absent |

The MinIO recipes create isolated containers, networks, buckets, and temporary
configuration files. Their cleanup traps remove those resources unless a
recipe-specific keep flag is set. Docker may pull pinned images on the first
run.

## Standalone Scripts

The scripts below are not all exposed as Just recipes because several operate
external infrastructure or incur cloud cost.

| Script | Requirements and behavior |
|---|---|
| `scripts/runtime_smoke.sh` | Builds `graph-node`, checks readiness and metrics, then exercises Bolt, scoped databases, HTTP, and graceful shutdown; requires Python `neo4j` |
| `scripts/bolt_neo4j_driver_smoke.sh` | Exercises direct and routing Bolt URIs through the official Python Neo4j driver |
| `scripts/query_correctness.sh` | Implements the local `query-correctness` recipe |
| `scripts/multiprocess_stress.sh`, `scripts/fence_takeover.sh` | Implement the local stress and fencing recipes |
| `scripts/minio_*.sh` | MinIO smoke, correctness, chaos, fencing, MBT, and write-profile harnesses; require Docker |
| `scripts/multinode_k3s.sh` | Creates a disposable multi-node K3d cluster and performs disruptive failover tests; requires Docker, K3d, kubectl, and Helm |
| `scripts/deploy_single_node_k3s.sh` | Builds and deploys to an existing K3s host using an S3 bucket; changes Kubernetes and AWS resources |
| `scripts/bolt_graphblas_client.py` | Python Bolt client for GraphBLAS query exercises; requires the `neo4j` package |
| `scripts/multinode_k3s_client.py` | Runs inside the disposable K3d client Pod created by `multinode_k3s.sh` |

`just update-slatedb` is a maintenance command, not a verification command. It
updates the pinned SlateDB dependency in `Cargo.lock`; review and test the
resulting lockfile diff before committing it.
