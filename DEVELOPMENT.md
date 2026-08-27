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

The recipes below are wired in the `justfile` but their shell scripts are not
yet tracked in this repository ([#88](https://github.com/hydra-db/hydradb/issues/88)).
Running them prints a clear error naming the missing script. Use
`just verify-harness-scripts` to list which scripts are present on a checkout.

| Recipe | What it runs | Status |
|---|---|---|
| `just smoke` | Isolated local object-store write, traversal, reopen, and verification | Available |
| `just smoke-graphblas` | The same smoke flow with SuiteSparse selected explicitly | Available |
| `just query-memory-profile` | Build/query memory matrix with runtime-selectable kernels | Available |
| `just query-correctness` | Exact OpenCypher result checks | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just query-bench` | Configurable cold, hot, and concurrent query benchmark | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just stress` | Multiprocess writes, restart recovery, compaction, GC, and verification | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just fence` | Hard SlateDB writer-takeover proof | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |

The benchmark recipes intentionally use production-sized defaults and can run
for a long time. Override their documented `GRAPH_QUERY_*` environment
variables for a small development sample.

## Docker And MinIO Harnesses

These recipes also depend on shell scripts that are not tracked in the public
tree ([#88](https://github.com/hydra-db/hydradb/issues/88)).

| Recipe | Purpose | Status |
|---|---|---|
| `just minio-smoke` | Runs the object-store smoke flow against an ephemeral MinIO container | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just minio-query-correctness` | Runs exact query checks against MinIO | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just minio-query-bench` | Runs the query benchmark against MinIO | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just minio-chaos` | Pauses, restarts, and recovers MinIO during graph operations | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just minio-fence` | Runs writer takeover against MinIO | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |
| `just minio-mbt` | Replays the formal MBT adapters against MinIO | Script not tracked ([#88](https://github.com/hydra-db/hydradb/issues/88)) |

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
| `scripts/query_bench.sh`, `scripts/query_correctness.sh`, `scripts/query_memory_profile.sh` | Implement the corresponding local Just recipes |
| `scripts/multiprocess_stress.sh`, `scripts/fence_takeover.sh` | Implement the local stress and fencing recipes |
| `scripts/minio_*.sh` | MinIO smoke, correctness, benchmark, chaos, fencing, MBT, and write-profile harnesses; require Docker |
| `scripts/multinode_k3s.sh` | Creates a disposable multi-node K3d cluster and performs disruptive failover tests; requires Docker, K3d, kubectl, and Helm |
| `scripts/deploy_single_node_k3s.sh` | Builds and deploys to an existing K3s host using an S3 bucket; changes Kubernetes and AWS resources |
| `scripts/ec2_graphblas_benchmark.sh` | Runs the containerized GraphBLAS benchmark against S3 on EC2; uses AWS credentials and can incur cost |
| `scripts/run_s3_bolt_benchmark.sh` | Starts the S3-backed Bolt benchmark and optionally deletes its benchmark prefix afterward |
| `scripts/neo4j_exact_hop_benchmark.sh` | Pulls and runs Neo4j in Docker for comparison measurements |
| `scripts/bolt_graphblas_client.py`, `scripts/falkordb_bolt_benchmark.py`, `scripts/s3_bolt_driver_benchmark.py` | Python benchmark clients used by the shell harnesses; require the `neo4j` package |
| `scripts/multinode_k3s_client.py` | Runs inside the disposable K3d client Pod created by `multinode_k3s.sh` |

`just update-slatedb` is a maintenance command, not a verification command. It
updates the pinned SlateDB dependency in `Cargo.lock`; review and test the
resulting lockfile diff before committing it.
