#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PG_CONFIG="${PG_CONFIG:-pg_config}"
PG_BINDIR="$("$PG_CONFIG" --bindir)"
PYTHON="${PYTHON:-python3}"
for program in initdb pg_ctl psql; do
  if [[ ! -x "$PG_BINDIR/$program" ]]; then
    printf 'Missing PostgreSQL executable: %s\n' "$PG_BINDIR/$program" >&2
    exit 2
  fi
done
if [[ "$(id -u)" == 0 ]]; then
  printf 'Run just pg-test as a non-root user; initdb refuses root.\n' >&2
  exit 2
fi
command -v "$PYTHON" >/dev/null

ROOT="$(mktemp -d /tmp/pg-hydradb-test.XXXXXXXX)"
cleanup() {
  local status=$?
  trap - EXIT
  if [[ -f "$ROOT/data/postmaster.pid" ]]; then
    "$PG_BINDIR/pg_ctl" -D "$ROOT/data" -m immediate -w stop >/dev/null 2>&1 || true
  fi
  if [[ "$status" != 0 ]]; then
    printf '\nDisposable PostgreSQL log:\n' >&2
    tail -100 "$ROOT/server.log" >&2 2>/dev/null || true
  fi
  rm -rf -- "$ROOT"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir "$ROOT/socket"
"$PG_BINDIR/initdb" -D "$ROOT/data" -U hydradb_test_admin -A trust --no-locale -E UTF8 >"$ROOT/initdb.log"
cat >>"$ROOT/data/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$ROOT/socket'
unix_socket_permissions = 0700
port = 5432
fsync = on
full_page_writes = on
synchronous_commit = on
EOF

export PGHOST="$ROOT/socket" PGPORT=5432 PGUSER=hydradb_test_admin PGDATABASE=postgres
export PGCONNECT_TIMEOUT=10
export PYTHONDONTWRITEBYTECODE=1
unset PGSERVICE PGSERVICEFILE PGOPTIONS PGHOSTADDR PGPASSWORD PGPASSFILE
export PGHYDRADB_TEST_PSQL="$PG_BINDIR/psql" PGHYDRADB_TEST_ROOT="$ROOT" PGHYDRADB_TEST_PG_CTL="$PG_BINDIR/pg_ctl"
"$PG_BINDIR/pg_ctl" -D "$ROOT/data" -l "$ROOT/server.log" -w start
"$PG_BINDIR/psql" -X --set=ON_ERROR_STOP=1 --file=crates/pg-hydradb/tests/fixture.sql
"$PYTHON" crates/pg-hydradb/tests/integration.py
if [[ "${PGHYDRADB_QUERY_BENCHMARK:-0}" == 1 ]]; then
  "$PYTHON" crates/pg-hydradb/tests/query_benchmark.py --iterations "${PGHYDRADB_QUERY_BENCHMARK_ITERATIONS:-7}"
fi
if [[ "${PGHYDRADB_WRITE_BENCHMARK:-0}" == 1 ]]; then
  "$PYTHON" crates/pg-hydradb/tests/write_benchmark.py
fi
"$PG_BINDIR/pg_ctl" -D "$ROOT/data" -m immediate -w stop
"$PG_BINDIR/pg_ctl" -D "$ROOT/data" -l "$ROOT/server.log" -w start
"$PYTHON" crates/pg-hydradb/tests/integration.py --recovery
"$PYTHON" crates/pg-hydradb/tests/usage_report.py
printf 'pg-hydradb integration tests passed\n'
