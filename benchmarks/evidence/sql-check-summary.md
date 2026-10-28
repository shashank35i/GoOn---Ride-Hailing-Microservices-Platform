# SQL invariants from the recorded runs

These are the aggregate SQL results captured for the runs listed in `../RESULTS.md`. Identifiers are omitted. Reusable queries are in [`../sql/verify-results.sql`](../sql/verify-results.sql).

| Run | SQL result |
|---|---|
| Customer different-key race | 1 ride for the two request keys; 1 `RIDE_REQUESTED` outbox row; 1 `customer_active_rides` row. |
| Same-key idempotency | 1 ride for the customer/key; 1 `RIDE_REQUESTED` outbox row after 200 requests. |
| Accept/decline race | 10 rides `ASSIGNED` with exactly 1 accepted offer and 1 `DRIVER_ASSIGNED` outbox row; 10 rides remained `REQUESTED` after decline won, with 0 accepted offers. |
| One driver / two rides | Across 10 pairs, exactly one ride per pair was assigned; no pair had two assigned rides. |
| Outbox kill/restart | 194 rides had 194 `RIDE_REQUESTED` outbox rows; all 194 were `PUBLISHED`; 0 remained pending after 60 seconds; maximum offer count per ride was 1. Replaying one event left its processed-event count at 1 and offer count at 1. |
| Customer active claims after backfill + race | 7 claim rows and 257 active ride rows in that local database snapshot; 0 claims without an active ride; 0 duplicate customer claims. Four customers had multiple historical active rides before backfill; the migration left ride history untouched and claimed only one current ride per customer. |

The deadlock run was a separate two-session InnoDB lock-cycle demonstration. MySQL returned error 1213 and the latest detected deadlock section was captured. It demonstrates database deadlock detection, not an application endpoint reproducing that deadlock. SQL session steps are in [`../sql/deadlock-session-a.sql`](../sql/deadlock-session-a.sql) and [`../sql/deadlock-session-b.sql`](../sql/deadlock-session-b.sql).
