#!/usr/bin/env bash
# Burst load test: ramps from baseline to 10x and measures p95 query latency.
set -euo pipefail

INGEST_URL="${INGEST_URL:-http://localhost:8080}"
QUERY_URL="${QUERY_URL:-http://localhost:8081}"
BASELINE_RPS="${BASELINE_RPS:-100}"
BURST_MULTIPLIER="${BURST_MULTIPLIER:-10}"
DURATION="${DURATION:-60}"

SOURCES=(api worker scheduler auth payments)
LEVELS=(DEBUG INFO INFO INFO WARN ERROR)

gen_batch() {
  local size=$1
  local entries="["
  for ((i = 0; i < size; i++)); do
    local src=${SOURCES[$RANDOM % ${#SOURCES[@]}]}
    local lvl=${LEVELS[$RANDOM % ${#LEVELS[@]}]}
    entries+="{\"id\":\"$(uuidgen)\",\"source\":\"$src\",\"level\":\"$lvl\",\"message\":\"request handled in ${RANDOM}ms path=/api/v1/items trace=$RANDOM\"}"
    ((i < size - 1)) && entries+=","
  done
  echo "$entries]"
}

send_load() {
  local rps=$1 duration=$2 label=$3
  echo "--- $label: ${rps} entries/sec for ${duration}s"
  local batch_size=50
  local batches_per_sec=$((rps / batch_size))
  ((batches_per_sec < 1)) && batches_per_sec=1

  local end=$((SECONDS + duration))
  while ((SECONDS < end)); do
    for ((b = 0; b < batches_per_sec; b++)); do
      curl -s -o /dev/null -X POST "$INGEST_URL/ingest/batch" \
        -H 'Content-Type: application/json' \
        -d "$(gen_batch $batch_size)" &
    done
    wait
    sleep 1
  done
}

measure_query_latency() {
  local samples=${1:-20}
  local times=()
  for ((i = 0; i < samples; i++)); do
    local t
    t=$(curl -s -o /dev/null -w '%{time_total}' -X POST "$QUERY_URL/query" \
      -H 'Content-Type: application/json' \
      -d '{"source":"api","limit":100}')
    times+=("$t")
  done
  printf '%s\n' "${times[@]}" | sort -n | awk -v p=0.95 '
    { a[NR] = $1 }
    END { idx = int(NR * p); if (idx < 1) idx = 1; printf "p95: %.0fms (n=%d)\n", a[idx] * 1000, NR }'
}

echo "=== Phase 1: baseline load (${BASELINE_RPS}/s) ==="
send_load "$BASELINE_RPS" "$DURATION" "baseline"
echo "Baseline query latency:"
measure_query_latency

echo
echo "=== Phase 2: ${BURST_MULTIPLIER}x burst ($((BASELINE_RPS * BURST_MULTIPLIER))/s) ==="
send_load "$((BASELINE_RPS * BURST_MULTIPLIER))" "$DURATION" "burst"
echo "Query latency under burst:"
measure_query_latency

echo
echo "=== Phase 3: recovery ==="
sleep 10
echo "Post-burst query latency:"
measure_query_latency
