-- Run against the isolated GoOn test databases after substituting fresh values.
-- UUID columns in ride_db are BINARY(16). Use UUID_TO_BIN(<uuid-string>) when filtering.

-- Customer active-ride invariant and the IDs used for one two-request race.
SELECT COUNT(*) AS active_claims_for_test_customer
FROM ride_db.customer_active_rides
WHERE customer_id = UUID_TO_BIN('<customer-uuid>');

SELECT COUNT(*) AS created_rides_for_test_keys
FROM ride_db.rides
WHERE customer_id = UUID_TO_BIN('<customer-uuid>')
  AND idempotency_key IN ('<idempotency-key-a>', '<idempotency-key-b>');

SELECT event_type, status, COUNT(*) AS event_count
FROM ride_db.outbox
WHERE aggregate_id IN (
    SELECT ride_id
    FROM ride_db.rides
    WHERE customer_id = UUID_TO_BIN('<customer-uuid>')
      AND idempotency_key IN ('<idempotency-key-a>', '<idempotency-key-b>')
)
GROUP BY event_type, status;

-- One successful offer acceptance per ride; one active assignment per driver.
SELECT ride_id, SUM(status = 'ACCEPTED') AS accepted_offer_count
FROM ride_db.ride_offers
WHERE ride_id IN (UUID_TO_BIN('<ride-uuid-1>'), UUID_TO_BIN('<ride-uuid-2>'))
GROUP BY ride_id;

SELECT driver_id, COUNT(*) AS active_assignment_count
FROM ride_db.driver_assignments
GROUP BY driver_id
HAVING COUNT(*) > 1;

-- Outbox publication and dedupe counts for an acknowledged test batch.
SELECT event_type, status, COUNT(*) AS row_count
FROM ride_db.outbox
WHERE event_type = 'RIDE_REQUESTED'
  AND aggregate_id IN (
      SELECT ride_id FROM ride_db.rides
      WHERE idempotency_key LIKE '<test-prefix>%'
  )
GROUP BY event_type, status;

SELECT COUNT(*) AS processed_event_count
FROM ride_db.processed_events
WHERE event_id = UUID_TO_BIN('<event-uuid>');
