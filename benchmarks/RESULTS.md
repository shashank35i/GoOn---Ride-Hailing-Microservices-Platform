# Recorded validation results

All results below are local development measurements. Test plans and sanitized JTLs are in [`plans/`](plans/) and [`results/`](results/); the corresponding machine-readable summaries are in [`evidence/`](evidence/).

| Test | Setup | Measured result | Status / limit | Artifacts |
|---|---|---|---|---|
| Customer active-ride claim | 1 race × 2 synchronized requests; same customer and quote, different idempotency keys | 1 HTTP 201, 1 HTTP 409; SQL: 1 ride, 1 `RIDE_REQUESTED` event, 1 active-customer claim | **PASS** | [JMX](plans/customer-double-booking.jmx) · [JTL](results/customer-double-booking-redacted.jtl) · [JSON](evidence/customer-double-booking.json) · [SQL](sql/verify-results.sql) |
| Accept versus decline | 20 rides × 50 synchronized requests (25 accept + 25 decline each) | 1,000 requests: 10 HTTP 200, 10 HTTP 204, 980 HTTP 409, 0 HTTP 500. Ten rides assigned with one accepted offer and assignment event; ten remained requested after decline won. | **PASS for exclusivity.** It does not mean all 20 rides were accepted. | [JMX](plans/accept-decline-race.jmx) · [JTL](results/accept-decline-race-redacted.jtl) · [JSON](evidence/accept-decline-race.json) · [SQL](sql/verify-results.sql) |
| One driver, two rides | 10 pairs × 2 simultaneous accept requests | 10 HTTP 200, 10 HTTP 409; no pair had both accepts succeed | **PASS** | [JMX](plans/one-driver-two-rides.jmx) · [JTL](results/one-driver-two-rides-redacted.jtl) · [JSON](evidence/one-driver-two-rides.json) · [SQL](sql/verify-results.sql) |
| Same-key idempotency | 1 run × 200 synchronized requests; same customer, signed quote, and key | 200 HTTP 201; 1 ride and 1 `RIDE_REQUESTED` row. 89.29 req/s; p50/p95/p99 1,784/2,188/2,213 ms. | **PASS for this run** | [JMX](plans/same-key-idempotency.jmx) · [JTL](results/same-key-idempotency-redacted.jtl) · [JSON](evidence/same-key-idempotency.json) · [SQL](sql/verify-results.sql) |
| GPS ingestion | 20 driver IDs; 100, 250, and 500 threads for 60 seconds each; 2-second pacing | See per-level table; 22,470 HTTP 202 responses total, 0% errors. | **PASS for local test.** CPU hit 100% at peak; not a production-capacity result. | [JMX](plans/gps-ingestion-multikey.jmx) · [JTL](results/gps-ingestion-multikey-redacted.jtl) · [JSON](evidence/gps-ingestion-multikey.json) |
| Outbox crash / restart / replay | 200 create attempts; Ride Service stopped during load and restarted; same-event replay | 194 HTTP 201, 6 HTTP 500 during forced termination. All 194 acknowledged rides had outbox rows and were published; 0 pending after 60 seconds; max 1 offer per ride. Replay kept processed-event and offer counts at 1. | **PARTIAL / FAIL** for all-request success; acknowledged writes and duplicate suppression passed. | [JMX](plans/outbox-crash-restart.jmx) · [JTL](results/outbox-crash-restart-redacted.jtl) · [JSON](evidence/outbox-crash-restart.json) · [SQL](sql/verify-results.sql) |
| InnoDB deadlock mechanism | Two independent MySQL transactions took opposing row locks | MySQL error 1213 and `LATEST DETECTED DEADLOCK` capture | **PASS as database-level demonstration only.** This did not reproduce an application endpoint deadlock. | [JSON](evidence/deadlock-mechanism.json) · [Session A SQL](sql/deadlock-session-a.sql) · [Session B SQL](sql/deadlock-session-b.sql) |

## GPS ingestion percentiles

| Threads | Samples | RPS | p50 | p95 | p99 | Errors |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 2,797 | 48.62 | 6 ms | 23 ms | 89 ms | 0% |
| 250 | 6,872 | 118.75 | 8 ms | 249 ms | 600 ms | 0% |
| 500 | 12,801 | 221.33 | 40 ms | 1,212 ms | 1,637 ms | 0% |

At 500 threads the recorded CPU average/peak was 88.76% / 100%, RAM peak was 16,073.5 MiB, and Kafka lag was 1,006 immediately after traffic, then 0 after the recorded drain wait. The 500-thread JTL is [`results/gps-ingestion-multikey-redacted.jtl`](results/gps-ingestion-multikey-redacted.jtl).

## Kafka lag investigation

A single-driver 500-thread run recorded lag 8,817 immediately after the load. In a later single-driver run after `driver-locations` was expanded to three partitions and three Location Sync consumers were present, immediate lag was 3,049 and reached zero after 30 seconds. This is a 65.4% lower immediate snapshot across separate local runs, **not a controlled causal benchmark**. Both runs used one driver ID, so its keyed events remained on one partition; adding partitions cannot parallelize that one hot key. A later 20-driver run distributed records across all three partitions and drained to zero after the load.

See [`evidence/single-key-lag-comparison.json`](evidence/single-key-lag-comparison.json). `driverId` remains the Kafka key to preserve per-driver ordering.

The SQL result summary is [`evidence/sql-check-summary.md`](evidence/sql-check-summary.md); reusable query templates are in [`sql/verify-results.sql`](sql/verify-results.sql).

## OAuth2 token burst diagnostic

During one 200-thread cold-cache burst, Keycloak logs contained 100 token-endpoint warnings in three seconds. A focused retest recorded two warnings around quote preparation and the route run. These are **log-warning counts, not a direct token-fetch counter**. The gateway code now normalizes client-credentials calls to a fixed gateway principal and serializes cache misses per client registration; internal token providers cache by registration/scope set with single-flight refresh. Each process has its own in-memory cache. See [`evidence/token-cache-diagnostic.json`](evidence/token-cache-diagnostic.json).

## Redis/Lettuce duplicate-broadcast incident

During development, approximately 10× duplicate location broadcasts were reported. The implementation was changed so `StringRedisTemplate.executePipelined` performs only `GEOADD` and `EXPIRE`, while the serialized string is sent exactly once through typed Lettuce `StatefulRedisConnection<String, String>.sync().spublish(...)`. The current source reflects this separation. The retained validation bundle does not contain a controlled before/after broadcast-count test, so the approximate multiplier is an incident report, not a benchmark result.

## UUID mapping / idempotency failure investigation

An earlier concurrent idempotency test exposed a mismatch between the ride database's `BINARY(64)` UUID columns and 16-byte UUID parameter binding. UUIDs were being stored with zero padding, and lookup/recovery did not reliably find the pre-existing ride. Ride UUID mappings and schema were corrected to `BINARY(16)`; the later 200-thread same-key run returned 200 HTTP 201 responses while SQL showed only one ride and one outbox row. This demonstrates the ORM/storage diagnosis and the corrected run, rather than just retrying the request in application code.

## Reproduction and SQL checks

The plans expect fresh valid credentials and test fixtures. Never commit generated fixture values. Run one plan at a time against a clean test environment; the raw service and Kafka state can affect the counts. SQL invariant checks are in [`sql/verify-results.sql`](sql/verify-results.sql).
