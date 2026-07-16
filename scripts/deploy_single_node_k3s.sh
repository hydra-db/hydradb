#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/charts/turbolay"

usage() {
  cat <<'EOF'
Deploy a public, S3-backed Turbolay instance to an existing single-node K3s cluster.

Required:
  TURBOLAY_S3_BUCKET       S3 bucket used as durable storage.

Usually automatic on EC2, otherwise required:
  TURBOLAY_PUBLIC_HOST     Public DNS name or IPv4 address used by clients.

Common optional settings:
  TURBOLAY_AWS_REGION              Default: AWS config region or us-east-1
  TURBOLAY_K8S_NAMESPACE           Default: turbolay
  TURBOLAY_RELEASE                 Default: turbolay
  TURBOLAY_GRAPH_NAMESPACE         Default: single-node
  TURBOLAY_GRAPH_ID                Default: public-test
  TURBOLAY_DATABASE                Default: default
  TURBOLAY_S3_PREFIX               Default: turbolay/<graph namespace>
  TURBOLAY_BOLT_NODE_PORT          Default: 30687
  TURBOLAY_HTTPS_NODE_PORT         Default: 30443
  TURBOLAY_AUTH_TOKEN              Optional token of at least 32 characters
  TURBOLAY_AWS_SECRET_NAME         Default: <release>-aws-credentials
  TURBOLAY_USE_INSTANCE_ROLE       Set to 1 to avoid a static AWS Secret
  TURBOLAY_SKIP_BUILD              Set to 1 to reuse the imported image
  TURBOLAY_ROTATE_SECRETS          Set to 1 to rotate auth and TLS Secrets
  TURBOLAY_STATE_DIR               Default: ~/.config/turbolay-single-node

Host prerequisites:
  A running K3s cluster, kubectl, Helm, Docker, AWS CLI, curl, and OpenSSL.

Examples:
  TURBOLAY_S3_BUCKET=graph-benchmark ./scripts/deploy_single_node_k3s.sh
  TURBOLAY_S3_BUCKET=graph-benchmark TURBOLAY_PUBLIC_HOST=203.0.113.10 \
    ./scripts/deploy_single_node_k3s.sh

Validation without changing a cluster:
  TURBOLAY_S3_BUCKET=ci-bucket TURBOLAY_PUBLIC_HOST=203.0.113.10 \
    ./scripts/deploy_single_node_k3s.sh --render-only
EOF
}

RENDER_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --help|-h)
      usage
      exit 0
      ;;
    --render-only)
      RENDER_ONLY=1
      ;;
    *)
      echo "unknown argument: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

log() {
  printf '[turbolay] %s\n' "$*"
}

die() {
  printf '[turbolay] error: %s\n' "$*" >&2
  exit 1
}

need_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

privileged() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

is_ipv4() {
  [[ "$1" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]
}

validate_dns_or_ipv4() {
  local value="$1"
  if is_ipv4 "$value"; then
    local octet
    local -a octets
    IFS='.' read -r -a octets <<<"$value"
    for octet in "${octets[@]}"; do
      ((10#$octet >= 0 && 10#$octet <= 255)) || return 1
    done
    return 0
  fi
  [[ "$value" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]]
}

validate_kubernetes_name() {
  [[ "$1" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] && ((${#1} <= 63))
}

validate_graph_component() {
  [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]
}

validate_node_port() {
  [[ "$1" =~ ^[0-9]+$ ]] && ((10#$1 >= 30000 && 10#$1 <= 32767))
}

imds_value() {
  local path="$1"
  local token
  token="$(curl -fsS --max-time 2 -X PUT \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null || true)"
  [[ -n "$token" ]] || return 0
  curl -fsS --max-time 2 \
    -H "X-aws-ec2-metadata-token: $token" \
    "http://169.254.169.254/latest/meta-data/$path" 2>/dev/null || true
}

secret_exists() {
  kubectl -n "$K8S_NAMESPACE" get secret "$1" >/dev/null 2>&1
}

apply_secret_tls() {
  local name="$1"
  local cert="$2"
  local key="$3"
  kubectl -n "$K8S_NAMESPACE" create secret tls "$name" \
    --cert="$cert" \
    --key="$key" \
    --dry-run=client \
    -o yaml | kubectl apply -f - >/dev/null
}

certificate_matches_host() {
  local cert="$1"
  local host="$2"
  if is_ipv4 "$host"; then
    openssl x509 -in "$cert" -noout -checkip "$host" >/dev/null 2>&1
  else
    openssl x509 -in "$cert" -noout -checkhost "$host" >/dev/null 2>&1
  fi
}

S3_BUCKET="${TURBOLAY_S3_BUCKET:-}"
AWS_REGION_VALUE="${TURBOLAY_AWS_REGION:-}"
K8S_NAMESPACE="${TURBOLAY_K8S_NAMESPACE:-turbolay}"
RELEASE="${TURBOLAY_RELEASE:-turbolay}"
GRAPH_NAMESPACE="${TURBOLAY_GRAPH_NAMESPACE:-single-node}"
GRAPH_ID="${TURBOLAY_GRAPH_ID:-public-test}"
DATABASE="${TURBOLAY_DATABASE:-default}"
S3_PREFIX="${TURBOLAY_S3_PREFIX:-turbolay/$GRAPH_NAMESPACE}"
BOLT_NODE_PORT="${TURBOLAY_BOLT_NODE_PORT:-30687}"
HTTPS_NODE_PORT="${TURBOLAY_HTTPS_NODE_PORT:-30443}"
PUBLIC_HOST="${TURBOLAY_PUBLIC_HOST:-}"
PUBLIC_IP="${TURBOLAY_PUBLIC_IP:-}"
ROTATE_SECRETS="${TURBOLAY_ROTATE_SECRETS:-0}"
SKIP_BUILD="${TURBOLAY_SKIP_BUILD:-0}"
USE_INSTANCE_ROLE="${TURBOLAY_USE_INSTANCE_ROLE:-0}"
STATE_DIR="${TURBOLAY_STATE_DIR:-$HOME/.config/turbolay-single-node}"
IMAGE_REPOSITORY="${TURBOLAY_IMAGE_REPOSITORY:-docker.io/library/turbolay}"
IMAGE_TAG="${TURBOLAY_IMAGE_TAG:-single-node}"
IMAGE="$IMAGE_REPOSITORY:$IMAGE_TAG"
DEPLOYMENT_NONCE="${TURBOLAY_DEPLOYMENT_NONCE:-$(date +%s)}"
AWS_SECRET_NAME="${TURBOLAY_AWS_SECRET_NAME:-$RELEASE-aws-credentials}"
AUTH_SECRET_NAME="${TURBOLAY_AUTH_SECRET_NAME:-$RELEASE-auth}"
PUBLIC_TLS_SECRET_NAME="${TURBOLAY_PUBLIC_TLS_SECRET_NAME:-$RELEASE-public-tls}"

[[ -n "$S3_BUCKET" ]] || die "TURBOLAY_S3_BUCKET is required"
[[ "$S3_BUCKET" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] || die "invalid S3 bucket name"
validate_kubernetes_name "$K8S_NAMESPACE" || die "invalid TURBOLAY_K8S_NAMESPACE"
validate_kubernetes_name "$RELEASE" || die "invalid TURBOLAY_RELEASE"
validate_graph_component "$GRAPH_NAMESPACE" || die "invalid TURBOLAY_GRAPH_NAMESPACE"
validate_graph_component "$GRAPH_ID" || die "invalid TURBOLAY_GRAPH_ID"
validate_graph_component "$DATABASE" || die "invalid TURBOLAY_DATABASE"
[[ "$S3_PREFIX" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*[A-Za-z0-9]$ ]] || die "invalid TURBOLAY_S3_PREFIX"
[[ "$S3_PREFIX" != *'..'* ]] || die "TURBOLAY_S3_PREFIX must not contain '..'"
validate_node_port "$BOLT_NODE_PORT" || die "TURBOLAY_BOLT_NODE_PORT must be in 30000-32767"
validate_node_port "$HTTPS_NODE_PORT" || die "TURBOLAY_HTTPS_NODE_PORT must be in 30000-32767"
[[ "$BOLT_NODE_PORT" != "$HTTPS_NODE_PORT" ]] || die "Bolt and HTTPS NodePorts must differ"
[[ "$DEPLOYMENT_NONCE" =~ ^[A-Za-z0-9._-]+$ ]] || die "invalid TURBOLAY_DEPLOYMENT_NONCE"

if [[ -z "$AWS_REGION_VALUE" ]]; then
  if command -v aws >/dev/null 2>&1; then
    AWS_REGION_VALUE="$(aws configure get region 2>/dev/null || true)"
  fi
  AWS_REGION_VALUE="${AWS_REGION_VALUE:-us-east-1}"
fi
[[ "$AWS_REGION_VALUE" =~ ^[a-z0-9-]+$ ]] || die "invalid TURBOLAY_AWS_REGION"

if [[ -z "$PUBLIC_HOST" ]]; then
  PUBLIC_HOST="$(imds_value public-hostname)"
fi
if [[ -z "$PUBLIC_IP" ]]; then
  PUBLIC_IP="$(imds_value public-ipv4)"
fi
if [[ -z "$PUBLIC_HOST" ]]; then
  PUBLIC_HOST="$PUBLIC_IP"
fi
[[ -n "$PUBLIC_HOST" ]] || die "set TURBOLAY_PUBLIC_HOST; EC2 public metadata was unavailable"
validate_dns_or_ipv4 "$PUBLIC_HOST" || die "invalid TURBOLAY_PUBLIC_HOST"
if [[ -n "$PUBLIC_IP" ]]; then
  is_ipv4 "$PUBLIC_IP" || die "invalid TURBOLAY_PUBLIC_IP"
  validate_dns_or_ipv4 "$PUBLIC_IP" || die "invalid TURBOLAY_PUBLIC_IP"
fi

need_command helm
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"
umask 077
VALUES_FILE="$STATE_DIR/values.yaml"
RENDERED_FILE="$STATE_DIR/rendered.yaml"
TOKEN_FILE="$STATE_DIR/auth-token"

AWS_EXTRA_ENV='    []'
if [[ "$USE_INSTANCE_ROLE" != "1" ]]; then
  AWS_EXTRA_ENV="$(cat <<EOF
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: $AWS_SECRET_NAME
          key: AWS_ACCESS_KEY_ID
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: $AWS_SECRET_NAME
          key: AWS_SECRET_ACCESS_KEY
    - name: AWS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          name: $AWS_SECRET_NAME
          key: AWS_SESSION_TOKEN
          optional: true
EOF
)"
fi

cat >"$VALUES_FILE" <<EOF
fullnameOverride: "$RELEASE"

image:
  repository: "$IMAGE_REPOSITORY"
  tag: "$IMAGE_TAG"
  pullPolicy: Never

objectStore:
  cloudProvider: aws
  aws:
    bucketName: "$S3_BUCKET"
    region: "$AWS_REGION_VALUE"
    allowHttp: false

graph:
  namespace: "$GRAPH_NAMESPACE"
  graphId: "$GRAPH_ID"
  database: "$DATABASE"
  cells: ["cell-0"]
  dataPath: "$S3_PREFIX/data"

auth:
  existingSecret: "$AUTH_SECRET_NAME"
  secretKey: auth-token
  create: false

tls:
  public:
    enabled: true
    secretName: "$PUBLIC_TLS_SECRET_NAME"
  certManager:
    enabled: false

node:
  replicaCount: 1
  podAnnotations:
    graph.usecortex.io/deployment-nonce: "$DEPLOYMENT_NONCE"
  resources:
    requests:
      cpu: "500m"
      memory: 1Gi
      ephemeral-storage: 4Gi
    limits:
      cpu: "4"
      memory: 8Gi
      ephemeral-storage: 12Gi
  cache:
    type: emptyDir
    memoryLimitBytes: 1073741824
    emptyDir:
      sizeLimit: 4Gi
      medium: ""
    persistentVolume:
      size: 8Gi
      storageClassName: ""
      accessModes: [ReadWriteOnce]
      annotations: {}
  tmpSizeLimit: 1Gi
  extraEnv:
$AWS_EXTRA_ENV

service:
  advertisedBoltAddress: "$PUBLIC_HOST:$BOLT_NODE_PORT"
  bolt:
    type: ClusterIP
    port: 7687
  https:
    type: ClusterIP
    port: 443

podDisruptionBudget:
  enabled: false

networkPolicy:
  enabled: false

serviceMonitor:
  enabled: false

tests:
  enabled: false

extraObjects:
  - apiVersion: v1
    kind: Service
    metadata:
      name: '{{ include "turbolay.fullname" . }}-public-bolt'
      labels:
        app.kubernetes.io/name: '{{ include "turbolay.name" . }}'
        app.kubernetes.io/instance: '{{ .Release.Name }}'
        app.kubernetes.io/component: node
    spec:
      type: NodePort
      selector:
        app.kubernetes.io/name: '{{ include "turbolay.name" . }}'
        app.kubernetes.io/instance: '{{ .Release.Name }}'
        app.kubernetes.io/component: node
        graph.usecortex.io/serving: "true"
      ports:
        - name: bolt
          port: 7687
          targetPort: bolt
          nodePort: $BOLT_NODE_PORT
  - apiVersion: v1
    kind: Service
    metadata:
      name: '{{ include "turbolay.fullname" . }}-public-https'
      labels:
        app.kubernetes.io/name: '{{ include "turbolay.name" . }}'
        app.kubernetes.io/instance: '{{ .Release.Name }}'
        app.kubernetes.io/component: node
    spec:
      type: NodePort
      selector:
        app.kubernetes.io/name: '{{ include "turbolay.name" . }}'
        app.kubernetes.io/instance: '{{ .Release.Name }}'
        app.kubernetes.io/component: node
        graph.usecortex.io/serving: "true"
      ports:
        - name: https
          port: 443
          targetPort: https
          nodePort: $HTTPS_NODE_PORT
EOF

helm lint --strict "$CHART" --values "$VALUES_FILE" >/dev/null
helm template "$RELEASE" "$CHART" \
  --namespace "$K8S_NAMESPACE" \
  --values "$VALUES_FILE" >"$RENDERED_FILE"
grep -q "nodePort: $BOLT_NODE_PORT" "$RENDERED_FILE" || die "Bolt NodePort did not render"
grep -q "nodePort: $HTTPS_NODE_PORT" "$RENDERED_FILE" || die "HTTPS NodePort did not render"

if [[ "$RENDER_ONLY" == "1" ]]; then
  log "render validation passed"
  log "values: $VALUES_FILE"
  log "manifest: $RENDERED_FILE"
  exit 0
fi

for command in kubectl docker k3s openssl curl aws; do
  need_command "$command"
done
if [[ "$(id -u)" -ne 0 ]]; then
  need_command sudo
fi

kubectl cluster-info >/dev/null
kubectl create namespace "$K8S_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

if ! aws s3api head-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1; then
  die "cannot access s3://$S3_BUCKET with the current AWS identity"
fi

if [[ "$USE_INSTANCE_ROLE" != "1" ]]; then
  ACCESS_KEY_VALUE="${AWS_ACCESS_KEY_ID:-$(aws configure get aws_access_key_id 2>/dev/null || true)}"
  SECRET_KEY_VALUE="${AWS_SECRET_ACCESS_KEY:-$(aws configure get aws_secret_access_key 2>/dev/null || true)}"
  SESSION_TOKEN_VALUE="${AWS_SESSION_TOKEN:-$(aws configure get aws_session_token 2>/dev/null || true)}"
  if [[ -n "$ACCESS_KEY_VALUE" && -n "$SECRET_KEY_VALUE" ]]; then
    AWS_ENV_FILE="$(mktemp)"
    trap 'rm -f "${AWS_ENV_FILE:-}"' EXIT
    printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' \
      "$ACCESS_KEY_VALUE" "$SECRET_KEY_VALUE" >"$AWS_ENV_FILE"
    if [[ -n "$SESSION_TOKEN_VALUE" ]]; then
      printf 'AWS_SESSION_TOKEN=%s\n' "$SESSION_TOKEN_VALUE" >>"$AWS_ENV_FILE"
    fi
    kubectl -n "$K8S_NAMESPACE" create secret generic "$AWS_SECRET_NAME" \
      --from-env-file="$AWS_ENV_FILE" \
      --dry-run=client \
      -o yaml | kubectl apply -f - >/dev/null
    rm -f "$AWS_ENV_FILE"
    trap - EXIT
  elif ! secret_exists "$AWS_SECRET_NAME"; then
    die "AWS credentials are unavailable; configure AWS CLI, set AWS environment variables, pre-create $AWS_SECRET_NAME, or set TURBOLAY_USE_INSTANCE_ROLE=1"
  fi
fi

if secret_exists "$AUTH_SECRET_NAME" && [[ "$ROTATE_SECRETS" != "1" ]]; then
  kubectl -n "$K8S_NAMESPACE" get secret "$AUTH_SECRET_NAME" \
    -o jsonpath='{.data.auth-token}' | base64 --decode >"$TOKEN_FILE"
else
  AUTH_TOKEN="${TURBOLAY_AUTH_TOKEN:-$(openssl rand -hex 32)}"
  printf '%s' "$AUTH_TOKEN" >"$TOKEN_FILE"
  kubectl -n "$K8S_NAMESPACE" create secret generic "$AUTH_SECRET_NAME" \
    --from-file="auth-token=$TOKEN_FILE" \
    --dry-run=client \
    -o yaml | kubectl apply -f - >/dev/null
fi
AUTH_TOKEN="$(cat "$TOKEN_FILE")"
((${#AUTH_TOKEN} >= 32)) || die "$AUTH_SECRET_NAME auth-token must contain at least 32 characters"
chmod 600 "$TOKEN_FILE"

PKI_DIR="$(mktemp -d)"
cleanup_pki() {
  rm -rf "$PKI_DIR"
}
trap cleanup_pki EXIT

PUBLIC_SAN=""
if is_ipv4 "$PUBLIC_HOST"; then
  PUBLIC_SAN="IP:$PUBLIC_HOST"
else
  PUBLIC_SAN="DNS:$PUBLIC_HOST"
fi
if [[ -n "$PUBLIC_IP" && "$PUBLIC_IP" != "$PUBLIC_HOST" ]]; then
  PUBLIC_SAN="$PUBLIC_SAN,IP:$PUBLIC_IP"
fi

if secret_exists "$PUBLIC_TLS_SECRET_NAME" && [[ "$ROTATE_SECRETS" != "1" ]]; then
  kubectl -n "$K8S_NAMESPACE" get secret "$PUBLIC_TLS_SECRET_NAME" \
    -o jsonpath='{.data.tls\.crt}' | base64 --decode >"$PKI_DIR/public-existing.crt"
  certificate_matches_host "$PKI_DIR/public-existing.crt" "$PUBLIC_HOST" || \
    die "existing public certificate does not cover $PUBLIC_HOST; set TURBOLAY_ROTATE_SECRETS=1"
else
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 365 \
    -subj "/CN=$PUBLIC_HOST" \
    -addext "subjectAltName=$PUBLIC_SAN" \
    -addext 'basicConstraints=critical,CA:FALSE' \
    -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
    -addext 'extendedKeyUsage=serverAuth' \
    -keyout "$PKI_DIR/public.key" \
    -out "$PKI_DIR/public.crt" >/dev/null 2>&1
  apply_secret_tls "$PUBLIC_TLS_SECRET_NAME" "$PKI_DIR/public.crt" "$PKI_DIR/public.key"
fi

if [[ "$SKIP_BUILD" != "1" ]]; then
  case "$(uname -m)" in
    x86_64|amd64) PLATFORM=linux/amd64 ;;
    aarch64|arm64) PLATFORM=linux/arm64 ;;
    *) die "unsupported machine architecture: $(uname -m)" ;;
  esac
  log "building $IMAGE for $PLATFORM"
  if ! privileged docker build --platform "$PLATFORM" -t "$IMAGE" "$ROOT"; then
    log "Docker build failed once; retrying to recover from transient BuildKit snapshot errors"
    privileged docker build --platform "$PLATFORM" -t "$IMAGE" "$ROOT"
  fi
  log "importing $IMAGE into K3s"
  privileged docker save "$IMAGE" | privileged k3s ctr images import -
else
  privileged k3s ctr images list | grep -F "$IMAGE" >/dev/null || \
    die "$IMAGE is not imported in K3s"
fi

helm upgrade --install "$RELEASE" "$CHART" \
  --namespace "$K8S_NAMESPACE" \
  --values "$VALUES_FILE" \
  --wait \
  --timeout 15m

kubectl -n "$K8S_NAMESPACE" get pods
log "deployment is ready"
log "Bolt URI: bolt+ssc://$PUBLIC_HOST:$BOLT_NODE_PORT"
log "HTTPS URI: https://$PUBLIC_HOST:$HTTPS_NODE_PORT"
log "database: $DATABASE"
log "username: neo4j"
log "password file: $TOKEN_FILE"
log "allow inbound TCP $BOLT_NODE_PORT and $HTTPS_NODE_PORT in the host firewall/security group"
