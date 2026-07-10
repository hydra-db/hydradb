#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export BOLT_COMPAT_ADDR="${BOLT_COMPAT_ADDR:-127.0.0.1:17687}"
export BOLT_COMPAT_URI="bolt://${BOLT_COMPAT_ADDR}"
export BOLT_ROUTING_COMPAT_URI="neo4j://${BOLT_COMPAT_ADDR}"

cargo run --locked --features bolt-server --example bolt_compat_server > /tmp/sgk-bolt-compat.log 2>&1 &
server_pid=$!
cleanup() {
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
}
trap cleanup EXIT

python3 - <<'PY'
import os
import socket
import time

host, port = os.environ["BOLT_COMPAT_ADDR"].rsplit(":", 1)
deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    try:
        with socket.create_connection((host, int(port)), timeout=0.5):
            break
    except OSError:
        time.sleep(0.1)
else:
    raise SystemExit("Bolt compatibility server did not become ready")
PY

python3 - <<'PY'
import os
from neo4j import GraphDatabase

for uri in (os.environ["BOLT_COMPAT_URI"], os.environ["BOLT_ROUTING_COMPAT_URI"]):
    with GraphDatabase.driver(uri, auth=("neo4j", "bolt-secret")) as driver:
        driver.verify_connectivity()
        with driver.session(database="default") as session:
            record = session.run(
                "MATCH (n {id: 1}) RETURN n.id AS answer"
            ).single(strict=True)
            assert record["answer"] == 42, (uri, record)
PY
