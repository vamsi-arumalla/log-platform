#!/usr/bin/env bash
# Failover validation: kills pods/brokers under load and measures recovery time.
# Usage: ./failover-test.sh [k8s|compose]
set -euo pipefail

MODE="${1:-compose}"
QUERY_URL="${QUERY_URL:-http://localhost:8081}"
INGEST_URL="${INGEST_URL:-http://localhost:8080}"

check_health() {
  curl -s -o /dev/null -w '%{http_code}' "$1/health" 2>/dev/null || echo "000"
}

wait_for_recovery() {
  local url=$1 service=$2
  local start=$SECONDS
  echo "Waiting for $service to recover..."
  while [[ "$(check_health "$url")" != "200" ]]; do
    sleep 1
    if ((SECONDS - start > 120)); then
      echo "FAIL: $service did not recover within 120s"
      return 1
    fi
  done
  echo "PASS: $service recovered in $((SECONDS - start))s"
}

inject_failure_compose() {
  echo "--- Injecting failure: restarting kafka broker"
  docker compose restart kafka &
  sleep 2
}

inject_failure_k8s() {
  echo "--- Injecting failure: deleting random kafka pod + query pod"
  kubectl -n log-platform delete pod kafka-$((RANDOM % 3)) --grace-period=0 &
  kubectl -n log-platform delete pod -l app=query --grace-period=0 \
    --field-selector=status.phase=Running 2>/dev/null | head -1 &
  sleep 2
}

echo "=== Failover test ($MODE mode) ==="
echo "Pre-failure health: ingestor=$(check_health "$INGEST_URL") query=$(check_health "$QUERY_URL")"

# keep writing during the failure to verify at-least-once delivery
echo "Starting background write load..."
(
  for i in $(seq 1 300); do
    curl -s -o /dev/null -X POST "$INGEST_URL/ingest" \
      -H 'Content-Type: application/json' \
      -d "{\"id\":\"failover-$i\",\"source\":\"failover-test\",\"level\":\"INFO\",\"message\":\"durability probe $i\"}" || true
    sleep 0.2
  done
) &
LOAD_PID=$!

if [[ "$MODE" == "k8s" ]]; then
  inject_failure_k8s
else
  inject_failure_compose
fi

FAILOVER_START=$SECONDS
wait_for_recovery "$INGEST_URL" ingestor
wait_for_recovery "$QUERY_URL" query
TOTAL=$((SECONDS - FAILOVER_START))

wait $LOAD_PID || true

echo
echo "Verifying durability probes survived the failover..."
sleep 5
RESULT=$(curl -s -X POST "$QUERY_URL/query" \
  -H 'Content-Type: application/json' \
  -d '{"source":"failover-test","limit":500}')
COUNT=$(echo "$RESULT" | python3 -c 'import json,sys; print(json.load(sys.stdin)["total_count"])' 2>/dev/null || echo 0)

echo "Recovered probe count: $COUNT / 300 sent"
echo "Total failover recovery time: ${TOTAL}s"

if ((TOTAL < 60)); then
  echo "PASS: recovery under 60s target"
else
  echo "FAIL: recovery exceeded 60s target"
  exit 1
fi
