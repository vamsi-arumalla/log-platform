# Distributed Log Ingestion & Query Platform

A fault-tolerant log ingestion and query platform built on **Kafka**, **Kubernetes**, **S3**, and **Prometheus**.

## Architecture

```
                    ┌──────────────┐
   HTTP clients ──▶ │   Ingestor   │──▶ Kafka (12 partitions, RF=3)
                    │ (stateless,  │         │
                    │ backpressure)│         ▼
                    └──────────────┘   ┌──────────────┐      ┌─────────────┐
                                       │ Query nodes  │ ──▶  │  Hot store  │
                    ┌──────────────┐   │ (consumer    │      │ (in-memory, │
   Query clients ──▶│  Query API   │   │  group)      │      │  ≤6h)       │
                    └──────────────┘   └──────────────┘      └──────┬──────┘
                                                                    │ compaction
                                                                    ▼
                                                             ┌─────────────┐
                                                             │ Cold store  │
                                                             │ (S3, gzip,  │
                                                             │  date-keyed)│
                                                             └─────────────┘
```

### Key design decisions

- **Partitioned Kafka topics** — 12 partitions keyed by trace ID (falls back to source hash), replication factor 3 with `min.insync.replicas=2` and an idempotent producer with `acks=all` for at-least-once, replayable delivery.
- **Horizontally scaled stateless processors** — ingestors hold no state; consumer-group rebalancing redistributes partitions automatically when pods are added or fail.
- **Backpressure controls** — a queue-depth controller sheds load with HTTP 503 above a high-water mark and releases after a cooldown once depth halves, protecting Kafka and downstream consumers during bursts.
- **Hot/cold tiered storage** — recent logs (≤6h) are served from an indexed in-memory hot store; a compactor drains aged entries into gzip-compressed, date-partitioned S3 objects. The query engine fans out across tiers and merges results.
- **Lag-aware autoscaling** — HPA scales query/processor pods on `kafka_consumer_lag_sum` (exposed via Prometheus Adapter) and ingestors on CPU, with fast scale-up (30s window) and conservative scale-down (5m window).
- **Failure injection validated** — `scripts/failover-test.sh` kills brokers and pods under sustained write load, then verifies durability probes survived and recovery completed in under 60 seconds.

## Components

| Component | Path | Role |
|-----------|------|------|
| Ingestor | `cmd/ingestor` | HTTP → Kafka producer with backpressure shedding |
| Query | `cmd/query` | Kafka consumer + tiered query engine (hot/cold) |
| Compactor | `cmd/compactor` | Hot→cold migration on an interval |
| Backpressure | `internal/backpressure` | Queue-depth admission control |
| Hot store | `internal/storage/hot.go` | In-memory indexed store with TTL eviction |
| Cold store | `internal/storage/cold.go` | S3 gzip archives, date-partitioned keys |

## Observability

Every service exposes `/metrics` for Prometheus:

- `log_ingest_total`, `log_ingest_duration_seconds` — ingestion throughput/latency
- `kafka_consumer_lag` — per-partition lag (drives autoscaling)
- `log_query_duration_seconds{tier}` — p95 query latency per storage tier
- `log_backpressure_active`, `log_backpressure_dropped_total` — load shedding
- `log_compaction_duration_seconds` — tiering health

Alert rules fire on p95 > 500ms (5m), lag > 100k (3m), and sustained backpressure (2m).

## Quick start (local)

```bash
# infra + services
make run-local

# send a log
curl -X POST localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -d '{"source":"api","level":"INFO","message":"hello world"}'

# query it back
curl -X POST localhost:8081/query \
  -H 'Content-Type: application/json' \
  -d '{"source":"api","limit":10}'
```

- Grafana: http://localhost:3000 · Prometheus: http://localhost:9091 · MinIO console: http://localhost:9001

## Kubernetes deployment

```bash
make docker-build
make deploy
```

Deploys a 3-broker Kafka StatefulSet (KRaft, no ZooKeeper), 3+ ingestor replicas, 2+ query replicas, Prometheus with SLO recording/alerting rules, HPAs, and PodDisruptionBudgets. Anti-affinity spreads brokers and replicas across nodes.

## Testing

```bash
make test            # unit tests (race detector on)
make loadtest        # baseline → 10x burst, reports p95 query latency
make failover-test   # kills broker/pods under load, asserts <60s recovery
```
