#!/usr/bin/env bash
# Run the parity matrix against a pinned local Neo4j (query-language/cypher/scripts/neo4j_parity_oracle.py).
#
#   neo4j_oracle_server.sh up        start the container, or reuse a running one
#   neo4j_oracle_server.sh run ...   start if needed, then run the oracle with ...
#   neo4j_oracle_server.sh down      remove the container
#
# The image is pinned by digest so a Neo4j release cannot change the oracle's
# answers between two runs of the same matrix. Ports bind to 127.0.0.1 only.
# The password is generated once into $STATE_DIR and handed to the oracle
# through the environment; it is never printed.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PYTHON="${PYTHON:-python3}"
IMAGE="${NEO4J_ORACLE_IMAGE:-neo4j:2026.09-community@sha256:0dfcadbd51e1d2e5a0cf86d054a0bb537a4bce212796d967cbd88445aaf3fbf3}"
CONTAINER="${NEO4J_ORACLE_CONTAINER:-hydradb-neo4j-oracle}"
BOLT_PORT="${NEO4J_ORACLE_BOLT_PORT:-37687}"
HTTP_PORT="${NEO4J_ORACLE_HTTP_PORT:-37474}"
STATE_DIR="${NEO4J_ORACLE_STATE:-${XDG_STATE_HOME:-$HOME/.local/state}/hydradb-neo4j-oracle}"
PASSWORD_FILE="$STATE_DIR/password"
OWNER_LABEL="io.hydradb.neo4j-oracle"
STATE_LABEL="io.hydradb.neo4j-oracle-state"
OWNER_VALUE="cypher-compat-oracle-v1"
[[ "$IMAGE" =~ @sha256:[0-9a-f]{64}$ ]] || { echo "Oracle image must be digest-pinned" >&2; exit 2; }

password() {
  if [[ ! -s "$PASSWORD_FILE" ]]; then
    mkdir -p "$STATE_DIR"
    (umask 077 && "$PYTHON" -c 'import secrets; print(secrets.token_hex(24), end="")' >"$PASSWORD_FILE")
  fi
  cat "$PASSWORD_FILE"
}

exists() {
  docker inspect -f '{{.Id}}' "$CONTAINER" >/dev/null 2>&1
}

require_owner() {
  local owner state
  owner="$(docker inspect -f '{{ index .Config.Labels "io.hydradb.neo4j-oracle" }}' "$CONTAINER")"
  state="$(docker inspect -f '{{ index .Config.Labels "io.hydradb.neo4j-oracle-state" }}' "$CONTAINER")"
  if [[ "$owner" != "$OWNER_VALUE" || "$state" != "$STATE_DIR" ]]; then
    echo "Refusing to reuse or remove unowned container: $CONTAINER" >&2
    return 1
  fi
}

up() {
  if exists; then
    require_owner
    if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER")" == true ]]; then
      if [[ "$(docker inspect -f '{{.Config.Image}}' "$CONTAINER")" != "$IMAGE" ]]; then
        echo "Refusing oracle reuse with a different image: $CONTAINER" >&2
        return 1
      fi
      return 0
    fi
    docker rm "$CONTAINER" >/dev/null
  fi
  docker run -d --name "$CONTAINER" \
    --label "$OWNER_LABEL=$OWNER_VALUE" --label "$STATE_LABEL=$STATE_DIR" \
    -p "127.0.0.1:$HTTP_PORT:7474" -p "127.0.0.1:$BOLT_PORT:7687" \
    -e "NEO4J_AUTH=neo4j/$(password)" \
    -e NEO4J_server_memory_heap_max__size=2G \
    "$IMAGE" >/dev/null
  for _ in $(seq 1 90); do
    if docker logs "$CONTAINER" 2>&1 | grep -q 'Started\.'; then
      return 0
    fi
    sleep 1
  done
  echo "Neo4j did not start; see: docker logs $CONTAINER" >&2
  exit 1
}

case "${1:-}" in
  up)
    up
    echo "neo4j oracle: bolt://127.0.0.1:$BOLT_PORT ($CONTAINER)"
    ;;
  run)
    shift
    if ! "$PYTHON" -c 'import neo4j' >/dev/null 2>&1; then
      echo "Python package neo4j is required (set PYTHON to a venv interpreter)." >&2
      exit 2
    fi
    up
    NEO4J_PASSWORD="$(password)" GIT_COMMIT="${GIT_COMMIT:-$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || true)}" \
      "$PYTHON" "$PROJECT_ROOT/query-language/cypher/scripts/neo4j_parity_oracle.py" run --uri "bolt://127.0.0.1:$BOLT_PORT" "$@"
    ;;
  down)
    if exists; then
      require_owner
      docker rm -f -v "$CONTAINER" >/dev/null
    fi
    ;;
  *)
    echo "usage: $0 up|run [oracle args]|down" >&2
    exit 2
    ;;
esac
