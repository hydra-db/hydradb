#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${TURBOLAY_MULTINODE_CLUSTER:-turbolay-multinode}"
NAMESPACE="${TURBOLAY_MULTINODE_NAMESPACE:-turbolay-multinode}"
MINIO_NAMESPACE="${TURBOLAY_MULTINODE_MINIO_NAMESPACE:-minio}"
IMAGE="${TURBOLAY_MULTINODE_IMAGE:-turbolay-multinode:test}"
STATE_DIR="${TURBOLAY_MULTINODE_STATE_DIR:-$ROOT/bench-results/multinode-k3s}"
KEEP="${TURBOLAY_MULTINODE_KEEP:-0}"
SKIP_BUILD="${TURBOLAY_MULTINODE_SKIP_BUILD:-0}"
TOKEN="multinode-test-token-with-at-least-32-characters"
STOPPED_HOST=""

log() { printf '[multinode] %s\n' "$*"; }
die() { printf '[multinode] error: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }

collect_runtime_logs() {
  : >"$STATE_DIR/runtime.log"
  while read -r pod; do
    [[ -n "$pod" ]] || continue
    printf '===== %s =====\n' "$pod" >>"$STATE_DIR/runtime.log"
    kubectl -n "$NAMESPACE" logs "$pod" --all-containers --prefix \
      >>"$STATE_DIR/runtime.log" 2>&1 || true
  done < <(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/instance=turbolay \
    -o name 2>/dev/null || true)
}

for command in docker k3d kubectl helm; do need "$command"; done

case "$STATE_DIR" in
  "$ROOT"/bench-results/*|/tmp/turbolay-multinode*) ;;
  *) die "state directory must be under $ROOT/bench-results or /tmp/turbolay-multinode*" ;;
esac

cleanup() {
  local status=$?
  if [[ -n "$STOPPED_HOST" ]]; then
    docker start "$STOPPED_HOST" >/dev/null 2>&1 || true
    STOPPED_HOST=""
  fi
  mkdir -p "$STATE_DIR"
  collect_runtime_logs
  kubectl -n "$NAMESPACE" get pods -o wide >"$STATE_DIR/pods.txt" 2>&1 || true
  if [[ "$KEEP" != "1" ]]; then
    k3d cluster delete "$CLUSTER" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT

rm -rf "$STATE_DIR"
mkdir -p "$STATE_DIR"

if k3d cluster list --no-headers 2>/dev/null | awk '{print $1}' | grep -Fxq "$CLUSTER"; then
  log "deleting stale cluster $CLUSTER"
  k3d cluster delete "$CLUSTER" >/dev/null
fi

log "creating one storage server, three query-node agents, and two indexer agents"
k3d cluster create "$CLUSTER" \
  --servers 1 \
  --agents 5 \
  --wait \
  --timeout 180s \
  --k3s-arg '--disable=traefik@server:*'

kubectl label node "k3d-${CLUSTER}-server-0" turbolay-role=storage --overwrite
for agent in 0 1 2; do
  kubectl label node "k3d-${CLUSTER}-agent-${agent}" turbolay-role=query --overwrite
done
for agent in 3 4; do
  kubectl label node "k3d-${CLUSTER}-agent-${agent}" turbolay-role=indexer --overwrite
done

if [[ "$SKIP_BUILD" == "1" ]]; then
  docker image inspect "$IMAGE" >/dev/null 2>&1 || die "requested image does not exist: $IMAGE"
  log "reusing existing graph-node image $IMAGE"
else
  log "building current graph-node image"
  docker build --target runtime -t "$IMAGE" "$ROOT"
fi
k3d image import "$IMAGE" --cluster "$CLUSTER"

log "deploying clean MinIO object storage"
kubectl create namespace "$MINIO_NAMESPACE"
kubectl -n "$MINIO_NAMESPACE" apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  replicas: 1
  selector:
    matchLabels: {app: minio}
  template:
    metadata:
      labels: {app: minio}
    spec:
      nodeSelector:
        turbolay-role: storage
      containers:
        - name: minio
          image: minio/minio:RELEASE.2025-07-23T15-54-02Z
          args: ["server", "/data"]
          env:
            - {name: MINIO_ROOT_USER, value: minioadmin}
            - {name: MINIO_ROOT_PASSWORD, value: minioadmin}
          ports:
            - {name: api, containerPort: 9000}
          readinessProbe:
            httpGet: {path: /minio/health/ready, port: api}
            periodSeconds: 2
          resources:
            requests: {cpu: 50m, memory: 128Mi}
            limits: {cpu: "1", memory: 512Mi}
          volumeMounts:
            - {name: data, mountPath: /data}
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  selector: {app: minio}
  ports:
    - {name: api, port: 9000, targetPort: api}
YAML
kubectl -n "$MINIO_NAMESPACE" rollout status deployment/minio --timeout=180s
kubectl -n "$MINIO_NAMESPACE" run minio-init \
  --image=minio/mc:RELEASE.2025-04-16T18-13-26Z \
  --restart=Never \
  --command -- sh -c \
  'until mc alias set local http://minio:9000 minioadmin minioadmin; do sleep 1; done; mc mb --ignore-existing local/graph-multinode'
kubectl -n "$MINIO_NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded pod/minio-init --timeout=180s

kubectl create namespace "$NAMESPACE"
cat >"$STATE_DIR/values.yaml" <<YAML
fullnameOverride: turbolay
image:
  repository: ${IMAGE%:*}
  tag: ${IMAGE##*:}
  pullPolicy: Never
objectStore:
  cloudProvider: aws
  aws:
    bucketName: graph-multinode
    region: us-east-1
    allowHttp: true
    endpoint: http://minio.${MINIO_NAMESPACE}.svc.cluster.local:9000
graph:
  namespace: multinode
  graphId: correctness
  database: default
  cells: [cell-0]
  dataPath: tests/multinode/data
auth:
  create: true
  token: ${TOKEN}
tls:
  public:
    enabled: false
node:
  replicaCount: 3
  nodeSelector: {turbolay-role: query}
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels:
              app.kubernetes.io/name: turbolay
              app.kubernetes.io/instance: turbolay
              app.kubernetes.io/component: node
          topologyKey: kubernetes.io/hostname
  resources:
    requests: {cpu: 100m, memory: 256Mi}
    limits: {cpu: "2", memory: 2Gi}
  cache:
    type: emptyDir
    memoryLimitBytes: 268435456
    emptyDir: {sizeLimit: 1Gi, medium: ""}
  extraEnv:
    - {name: AWS_ACCESS_KEY_ID, value: minioadmin}
    - {name: AWS_SECRET_ACCESS_KEY, value: minioadmin}
indexer:
  enabled: true
  replicaCount: 2
  nodeSelector: {turbolay-role: indexer}
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels:
              app.kubernetes.io/name: turbolay
              app.kubernetes.io/instance: turbolay
              app.kubernetes.io/component: indexer
          topologyKey: kubernetes.io/hostname
  extraEnv:
    - {name: AWS_ACCESS_KEY_ID, value: minioadmin}
    - {name: AWS_SECRET_ACCESS_KEY, value: minioadmin}
runtime:
  maxGraphblasBytes: 268435456
  maxQueryScanEdges: 64
networkPolicy:
  enabled: false
podDisruptionBudget:
  enabled: false
serviceMonitor:
  enabled: false
tests:
  enabled: false
YAML

log "deploying three query nodes and two independent indexers"
helm upgrade --install turbolay "$ROOT/charts/turbolay" \
  --namespace "$NAMESPACE" \
  --values "$STATE_DIR/values.yaml" \
  --wait --timeout 10m
kubectl -n "$NAMESPACE" rollout status statefulset/turbolay-node --timeout=300s
kubectl -n "$NAMESPACE" rollout status deployment/turbolay-indexer --timeout=300s

log "starting official Neo4j driver client"
kubectl -n "$NAMESPACE" create configmap multinode-client \
  --from-file=client.py="$ROOT/scripts/multinode_k3s_client.py"
kubectl -n "$NAMESPACE" run multinode-client \
  --image=python:3.12-slim \
  --restart=Never \
  --overrides='{"spec":{"nodeSelector":{"turbolay-role":"storage"}}}' \
  --command -- sh -c 'pip install --disable-pip-version-check neo4j==6.2.0 >/tmp/pip.log && sleep infinity'
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/multinode-client --timeout=300s
for _ in $(seq 1 120); do
  if kubectl -n "$NAMESPACE" exec multinode-client -- python -c 'import neo4j' \
    >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
kubectl -n "$NAMESPACE" exec multinode-client -- python -c 'import neo4j; print(neo4j.__version__)'
kubectl -n "$NAMESPACE" exec multinode-client -- mkdir -p /tests
kubectl -n "$NAMESPACE" cp "$ROOT/scripts/multinode_k3s_client.py" multinode-client:/tests/client.py

client() { kubectl -n "$NAMESPACE" exec multinode-client -- python /tests/client.py "$@"; }

indexer_metric() {
  local pod=$1
  local metric=$2
  local ip
  ip="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.podIP}')"
  kubectl -n "$NAMESPACE" exec multinode-client -- python -c '
import sys
import urllib.request

body = urllib.request.urlopen(f"http://{sys.argv[1]}:9091/metrics", timeout=2).read().decode()
print(next(int(line.rsplit(" ", 1)[1]) for line in body.splitlines() if line.startswith(sys.argv[2] + " ")))
' "$ip" "$metric"
}

indexer_published_sum() {
  local total=0
  local pod
  while read -r pod; do
    [[ -n "$pod" ]] || continue
    local value
    value="$(indexer_metric "$pod" graph_indexer_generations_published 2>/dev/null || true)"
    [[ "$value" =~ ^[0-9]+$ ]] || continue
    total=$((total + value))
  done < <(kubectl -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/component=indexer \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null)
  printf '%s\n' "$total"
}

wait_for_each_indexer_cycle() {
  local expected=${1:-2}
  for _ in $(seq 1 120); do
    local pods=()
    mapfile -t pods < <(kubectl -n "$NAMESPACE" get pods \
      -l app.kubernetes.io/component=indexer \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null)
    local ready=0
    if [[ "${#pods[@]}" -eq "$expected" ]]; then
      for pod in "${pods[@]}"; do
        if [[ "$(indexer_metric "$pod" graph_indexer_successful_cycles 2>/dev/null || true)" -gt 0 ]]; then
          ready=$((ready + 1))
        fi
      done
    fi
    if [[ "$ready" -eq "$expected" ]] && (( $(indexer_published_sum) > 0 )); then
      return 0
    fi
    sleep 1
  done
  die "indexers did not all complete a successful cycle with a published generation"
}

wait_for_indexer_advance() {
  local pod=$1
  local previous=$2
  for _ in $(seq 1 120); do
    local current
    current="$(indexer_metric "$pod" graph_indexer_generations_published 2>/dev/null || true)"
    if [[ "$current" =~ ^[0-9]+$ ]] && (( current > previous )); then
      return 0
    fi
    sleep 1
  done
  die "indexer $pod did not publish after graph topology changed"
}

wait_for_indexer_cycle_advance() {
  local pod=$1
  local previous=$2
  for _ in $(seq 1 120); do
    local current
    current="$(indexer_metric "$pod" graph_indexer_successful_cycles 2>/dev/null || true)"
    if [[ "$current" =~ ^[0-9]+$ ]] && (( current > previous )); then
      return 0
    fi
    sleep 1
  done
  die "indexer $pod did not complete another successful cycle"
}

wait_for_any_indexer_publication() {
  local previous=$1
  for _ in $(seq 1 120); do
    if (( $(indexer_published_sum) > previous )); then
      return 0
    fi
    sleep 1
  done
  die "no indexer published after graph topology changed"
}

wait_for_node_unavailable() {
  local node=$1
  for _ in $(seq 1 120); do
    local ready
    ready="$(kubectl get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
    if [[ "$ready" != "True" ]]; then
      return 0
    fi
    sleep 1
  done
  die "Kubernetes node remained ready after host loss: $node"
}

log "seeding graph through Bolt routing"
client seed | tee "$STATE_DIR/seed.json"
wait_for_each_indexer_cycle
client verify | tee "$STATE_DIR/verify-initial.json"
client strong 710 | tee "$STATE_DIR/strong-without-bookmark.json"
client traversal | tee "$STATE_DIR/traversal-initial.json"
client metrics | tee "$STATE_DIR/graphblas-metrics-initial.json"
client bookmark 700 | tee "$STATE_DIR/bookmark-initial.json"
client routing | tee "$STATE_DIR/routing.json"
client concurrent | tee "$STATE_DIR/concurrent.json"

log "removing one indexer host and proving the surviving indexer publishes"
mapfile -t INDEXER_PODS < <(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/component=indexer \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
[[ "${#INDEXER_PODS[@]}" -eq 2 ]] || die "expected two indexer pods"
FAILED_INDEXER_POD="${INDEXER_PODS[0]}"
SURVIVING_INDEXER_POD="${INDEXER_PODS[1]}"
SURVIVING_INDEXER_BEFORE="$(indexer_metric "$SURVIVING_INDEXER_POD" graph_indexer_generations_published)"
STOPPED_HOST="$(kubectl -n "$NAMESPACE" get pod "$FAILED_INDEXER_POD" -o jsonpath='{.spec.nodeName}')"
docker stop "$STOPPED_HOST" >/dev/null
wait_for_node_unavailable "$STOPPED_HOST"
client topology-tail | tee "$STATE_DIR/topology-tail-one-indexer.json"
wait_for_indexer_advance "$SURVIVING_INDEXER_POD" "$SURVIVING_INDEXER_BEFORE"
docker start "$STOPPED_HOST" >/dev/null
kubectl wait --for=condition=Ready "node/$STOPPED_HOST" --timeout=300s
STOPPED_HOST=""
kubectl -n "$NAMESPACE" rollout status deployment/turbolay-indexer --timeout=300s
wait_for_each_indexer_cycle

log "creating a graph-index baseline for the bounded WAL-tail outage test"
mapfile -t INDEXER_PODS < <(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/component=indexer \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
INDEXER_0_BEFORE="$(indexer_metric "${INDEXER_PODS[0]}" graph_indexer_successful_cycles)"
INDEXER_1_BEFORE="$(indexer_metric "${INDEXER_PODS[1]}" graph_indexer_successful_cycles)"
INDEXER_PUBLISHED_BEFORE="$(indexer_published_sum)"
client tail-limit-seed | tee "$STATE_DIR/tail-limit-seed.json"
wait_for_indexer_cycle_advance "${INDEXER_PODS[0]}" "$INDEXER_0_BEFORE"
wait_for_indexer_cycle_advance "${INDEXER_PODS[1]}" "$INDEXER_1_BEFORE"
wait_for_any_indexer_publication "$INDEXER_PUBLISHED_BEFORE"

log "stopping every indexer and proving the strong-read WAL-tail bound fails safely"
kubectl -n "$NAMESPACE" scale deployment/turbolay-indexer --replicas=0
kubectl -n "$NAMESPACE" wait --for=delete pod -l app.kubernetes.io/component=indexer --timeout=300s
client tail-limit-overflow | tee "$STATE_DIR/tail-limit-overflow.json"
client tail-limit-bounded | tee "$STATE_DIR/tail-limit-bounded.json"
client metrics | tee "$STATE_DIR/graphblas-metrics-tail.json"

log "restoring both indexers and proving the bounded strong read recovers"
kubectl -n "$NAMESPACE" scale deployment/turbolay-indexer --replicas=2
kubectl -n "$NAMESPACE" rollout status deployment/turbolay-indexer --timeout=300s
wait_for_each_indexer_cycle
client tail-limit-recovered | tee "$STATE_DIR/tail-limit-recovered.json"
client topology-verify | tee "$STATE_DIR/topology-after-reindex.json"

log "deleting reader pod 2"
kubectl -n "$NAMESPACE" delete pod turbolay-node-2 --wait=true
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/turbolay-node-2 --timeout=300s
client bookmark 701 | tee "$STATE_DIR/bookmark-after-reader-restart.json"

log "starting stable-id relationship writes before killing the active writer"
client ambiguous-write-loop 120 >"$STATE_DIR/ambiguous-writes.jsonl" 2>&1 &
AMBIGUOUS_PID=$!
for _ in $(seq 1 120); do
  if grep -q '"phase": "ambiguous-start"' "$STATE_DIR/ambiguous-writes.jsonl" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$AMBIGUOUS_PID" 2>/dev/null; then
    cat "$STATE_DIR/ambiguous-writes.jsonl" >&2
    die "ambiguous-write workload exited before writer loss"
  fi
  sleep 0.1
done
grep -q '"phase": "ambiguous-start"' "$STATE_DIR/ambiguous-writes.jsonl" \
  || die "ambiguous-write workload did not start"

log "stopping the machine hosting preferred writer pod 0 during the write stream"
STOPPED_HOST="$(kubectl -n "$NAMESPACE" get pod turbolay-node-0 -o jsonpath='{.spec.nodeName}')"
FAILED_WRITER_IP="$(kubectl -n "$NAMESPACE" get pod turbolay-node-0 -o jsonpath='{.status.podIP}')"
docker stop "$STOPPED_HOST" >/dev/null
wait_for_node_unavailable "$STOPPED_HOST"

if ! wait "$AMBIGUOUS_PID"; then
  cat "$STATE_DIR/ambiguous-writes.jsonl" >&2
  die "stable-id retries did not recover from writer loss"
fi
cat "$STATE_DIR/ambiguous-writes.jsonl"
AMBIGUOUS_FAILURES="$(python3 - "$STATE_DIR/ambiguous-writes.jsonl" <<'PY'
import json
import sys

records = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.startswith("{")]
complete = next(record for record in records if record.get("phase") == "ambiguous-complete")
print(complete["ambiguous_failures"])
PY
)"
if (( AMBIGUOUS_FAILURES < 1 )); then
  die "writer stopped without producing an ambiguous client failure"
fi

log "issuing a normal routed write while preferred writer pod 0 remains unavailable"
HOST_LOSS_RESULT="$(client routed-write 702 writer-host-loss)"
printf '%s\n' "$HOST_LOSS_RESULT" | tee "$STATE_DIR/write-during-writer-host-loss.json"
FAILOVER_WRITER="$(printf '%s' "$HOST_LOSS_RESULT" | python3 -c 'import json, sys; print(json.load(sys.stdin)["writer"])')"
if [[ "$FAILOVER_WRITER" == "$FAILED_WRITER_IP:7687" ]]; then
  die "routing returned the unavailable preferred writer: $FAILOVER_WRITER"
fi
FAILOVER_WRITER_IP="${FAILOVER_WRITER%:*}"
FAILOVER_POD_INDEX=""
for index in 0 1 2; do
  candidate_ip="$(kubectl -n "$NAMESPACE" get pod "turbolay-node-$index" -o jsonpath='{.status.podIP}')"
  if [[ "$candidate_ip" == "$FAILOVER_WRITER_IP" ]]; then
    FAILOVER_POD_INDEX="$index"
    break
  fi
done
[[ -n "$FAILOVER_POD_INDEX" ]] || die "could not map failover writer $FAILOVER_WRITER to a query pod"
client routed-verify 702 writer-host-loss | tee "$STATE_DIR/read-during-writer-host-loss.json"

log "restoring the failed machine and reopening its query process as the preferred writer"
docker start "$STOPPED_HOST" >/dev/null
kubectl wait --for=condition=Ready "node/$STOPPED_HOST" --timeout=300s
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/turbolay-node-0 --timeout=300s
STOPPED_HOST=""
client direct-write 0 703 restored-preferred-writer | tee "$STATE_DIR/write-after-host-restore.json"

log "proving the still-running failover process rejects its newly stale writer handle"
if client direct-write "$FAILOVER_POD_INDEX" 704 stale-failover-writer >"$STATE_DIR/stale-writer.out" 2>"$STATE_DIR/stale-writer.err"; then
  die "failover query node unexpectedly committed through its stale SlateDB writer"
fi

log "proving Bolt routing has returned to preferred-writer affinity"
RECOVERY_RESULT="$(client routed-write 706 preferred-writer-recovered)"
printf '%s\n' "$RECOVERY_RESULT" | tee "$STATE_DIR/write-after-fence-recovery.json"
RECOVERED_WRITER_IP="$(kubectl -n "$NAMESPACE" get pod turbolay-node-0 -o jsonpath='{.status.podIP}')"
RECOVERED_WRITER="$(printf '%s' "$RECOVERY_RESULT" | python3 -c 'import json, sys; print(json.load(sys.stdin)["writer"])')"
if [[ "$RECOVERED_WRITER" != "$RECOVERED_WRITER_IP:7687" ]]; then
  die "preferred writer affinity did not recover on node 0: $RECOVERED_WRITER"
fi
client bookmark 707 | tee "$STATE_DIR/bookmark-after-host-recovery.json"

log "performing a full cold restart with disposable caches"
kubectl -n "$NAMESPACE" scale statefulset/turbolay-node --replicas=0
kubectl -n "$NAMESPACE" wait --for=delete pod/turbolay-node-0 --timeout=300s
kubectl -n "$NAMESPACE" scale statefulset/turbolay-node --replicas=3
kubectl -n "$NAMESPACE" rollout status statefulset/turbolay-node --timeout=300s
client routed-write 1000 post-restart | tee "$STATE_DIR/write-after-cold-restart.json"
client verify --expect-extra | tee "$STATE_DIR/verify-cold-restart.json"
client traversal --expect-tail | tee "$STATE_DIR/traversal-cold-restart.json"
client bookmark 707 | tee "$STATE_DIR/bookmark-cold-restart.json"

log "checking logs for panics and corruption"
collect_runtime_logs
if grep -Eiq 'panic|corrupt|data loss' "$STATE_DIR/runtime.log"; then
  grep -Ein 'panic|corrupt|data loss' "$STATE_DIR/runtime.log" >&2
  die "fatal pattern found in node logs"
fi

log "PASS: two-indexer failover, bounded WAL-tail recovery, ambiguous stable-id retries, multi-reader routing, fencing, and restart checks completed"
