-- Stop RideService before running this migration.
-- The current BINARY(64) values must contain exactly 16 UUID bytes
-- followed by 48 zero bytes. Do not continue unless the first query returns 0,0.
SELECT
    COALESCE(SUM(customer_id IS NOT NULL AND HEX(SUBSTRING(customer_id, 17, 48)) <> REPEAT('00', 48)), 0)
        AS customer_id_nonzero_padding,
    COALESCE(SUM(driver_id IS NOT NULL AND HEX(SUBSTRING(driver_id, 17, 48)) <> REPEAT('00', 48)), 0)
        AS driver_id_nonzero_padding
FROM ride_db.rides;

-- Copy the UUID bytes to correctly sized columns. Check the next query returns 0
-- before running the final ALTER TABLE statement.
ALTER TABLE ride_db.rides
    ADD COLUMN customer_id_v2 BINARY(16) NULL,
    ADD COLUMN driver_id_v2 BINARY(16) NULL;

UPDATE ride_db.rides
SET customer_id_v2 = LEFT(customer_id, 16),
    driver_id_v2 = LEFT(driver_id, 16);

SELECT COUNT(*) AS mismatched_copies
FROM ride_db.rides
WHERE customer_id_v2 IS NULL
   OR HEX(customer_id_v2) <> LEFT(HEX(customer_id), 32)
   OR (driver_id IS NULL AND driver_id_v2 IS NOT NULL)
   OR (driver_id IS NOT NULL AND (
       driver_id_v2 IS NULL OR HEX(driver_id_v2) <> LEFT(HEX(driver_id), 32)
   ));

-- Run only when mismatched_copies is 0.
ALTER TABLE ride_db.rides
    DROP INDEX uk_customer_idempotency,
    DROP INDEX idx_customer_status,
    DROP COLUMN customer_id,
    DROP COLUMN driver_id,
    CHANGE COLUMN customer_id_v2 customer_id BINARY(16) NOT NULL,
    CHANGE COLUMN driver_id_v2 driver_id BINARY(16) NULL,
    ADD UNIQUE KEY uk_customer_idempotency (customer_id, idempotency_key),
    ADD KEY idx_customer_status (customer_id, status);
