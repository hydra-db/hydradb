#!/bin/sh
# Runs the escalation ladder and prints a one-line verdict per run.
# Intended to be executed inside the Jepsen control container:
#   docker compose exec control /jepsen/run-suite.sh
set -u
cd /jepsen

COMMON="--nodes n1,n2,n3,n4,n5 --username root --ssh-private-key /root/.ssh/id_rsa"
TIME_LIMIT="${TIME_LIMIT:-120}"
CONCURRENCY="${CONCURRENCY:-10}"

run() {
  desc="$1"; shift
  echo ""
  echo "=================================================================="
  echo "RUN: $desc"
  echo "=================================================================="
  lein run test $COMMON --time-limit "$TIME_LIMIT" --concurrency "$CONCURRENCY" \
      --recovery-time 20 "$@" > /tmp/run.log 2>&1
  status=$?
  # -a: jepsen's log contains non-UTF8 bytes, and without it grep declares the
  # file binary and prints nothing, which reads as a crash.
  verdict=$(grep -aoE "Everything looks good|Analysis invalid" /tmp/run.log | tail -1)
  echo "verdict: ${verdict:-CRASHED (exit $status)}"
  grep -aE ":ok-count|:fail-count|:info-count|:lost-count|:stale-count|:duplicated-count|:never-read-count|:attempt-count|read-your-writes-count|monotonic-read-count|:valid\?" \
      /tmp/run.log | sed 's/^ *//' | head -30
  if [ -z "$verdict" ]; then
    echo "--- crash tail ---"
    tail -25 /tmp/run.log
  fi
}

run "2. edge-set / kill"          --workload edge-set --consistency strong --nemesis kill
run "3. edge-set / pause"         --workload edge-set --consistency strong --nemesis pause
run "4. edge-set / object-store"  --workload edge-set --consistency strong --nemesis object-store
run "5. edge-set / all faults"    --workload edge-set --consistency strong --nemesis kill,pause,partition,object-store

echo ""
echo "suite complete"
