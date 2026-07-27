#!/bin/sh
# Rungs 5-6 of the ladder: the register (linearizability) and causal
# (session-guarantee) workloads. Run after run-suite.sh is green.
set -u
cd /jepsen

COMMON="--nodes n1,n2,n3,n4,n5 --username root --ssh-private-key /root/.ssh/id_rsa"
TIME_LIMIT="${TIME_LIMIT:-120}"

run() {
  desc="$1"; shift
  echo ""
  echo "=================================================================="
  echo "RUN: $desc"
  echo "=================================================================="
  lein run test $COMMON --time-limit "$TIME_LIMIT" --recovery-time 20 "$@" \
      > /tmp/run.log 2>&1
  status=$?
  verdict=$(grep -aoE "Everything looks good|Analysis invalid" /tmp/run.log | tail -1)
  echo "verdict: ${verdict:-CRASHED (exit $status)}"
  grep -aE ":ok-count|:fail-count|:info-count|:lost-count|:stale-count|:duplicated-count|:never-read-count|:attempt-count|read-your-writes-count|monotonic-read-count|:sessions|:valid\?" \
      /tmp/run.log | sed 's/^ *//' | head -30
  if [ -z "$verdict" ]; then
    echo "--- crash tail ---"; tail -25 /tmp/run.log
  fi
}

# Knossos is exponential in per-key concurrency; keep it low and let
# jepsen.independent parallelise across keys instead.
run "5. register / kill+pause (linearizability of strong reads)" \
    --workload register --consistency strong --nemesis kill,pause \
    --concurrency 10 --concurrency-per-key 5

run "6. causal / kill+partition+object-store (bookmark contract)" \
    --workload causal --nemesis kill,partition,object-store \
    --concurrency 10

echo ""
echo "workload suite complete"
