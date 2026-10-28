# GoOn validation bundle

This folder contains portable copies of the JMeter plans, redacted JTL result rows, and summarized evidence for local validation runs. The JTL copies retain timing and HTTP result columns while omitting request URLs, user IDs, ride IDs, and credentials. The `.tsv` / `.csv` fixtures contain headers only; create fresh test users, quotes, offers, and JWTs locally before running a plan. Never add real credentials to a fixture or commit.

## Run a plan

From the repository root, with the services and dependencies running and the relevant local fixture populated:

```bash
jmeter -n -t benchmarks/plans/same-key-idempotency.jmx \
  -l /tmp/goon-idempotency.jtl -e -o /tmp/goon-idempotency-report
```

The committed JMX plans reference `benchmarks/fixtures/` and contain no real JWTs. A CSV header documents each plan's required fields; the test-specific setup and assertions are described in [`RESULTS.md`](RESULTS.md). Some plans exercise failure handling that requires a separate operator action, such as stopping and restarting Ride Service during the outbox run.

## Contents

- `plans/`: JMeter 5.6.3 test plans copied from the measured runs, with machine-specific fixture paths made repository-relative.
- `fixtures/`: header-only fixture templates; values are deliberately omitted.
- `results/`: sanitized JTL samples with IDs and URLs removed.
- `evidence/`: concise result JSON and limitations, without tokens or test-user IDs.
- `sql/verify-results.sql`: reusable MySQL checks for the race, idempotency, assignment, and outbox invariants.

The figures describe local development runs, not production SLAs. The measurements were gathered on a single HP Pavilion 14 with an Intel Core i5-1135G7, 16 GiB RAM, JDK 23.0.1, Spring Boot 4.1.1, MySQL 9.1.0, Redis 7.4.10, Kafka 3.7.1, and JMeter 5.6.3.
