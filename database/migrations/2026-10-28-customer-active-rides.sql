CREATE TABLE IF NOT EXISTS ride_db.customer_active_rides (
    customer_id BINARY(16) NOT NULL,
    ride_id BINARY(16) NOT NULL,
    PRIMARY KEY (customer_id),
    UNIQUE KEY uk_customer_active_rides_ride_id (ride_id)
) ENGINE=InnoDB;

-- Backfill one claim per customer for rides already active before this table existed.
INSERT IGNORE INTO ride_db.customer_active_rides (customer_id, ride_id)
SELECT customer_id, ride_id
FROM (
    SELECT r.customer_id,
           r.ride_id,
           ROW_NUMBER() OVER (
               PARTITION BY r.customer_id
               ORDER BY r.requested_at DESC, r.ride_id DESC
           ) AS customer_ride_rank
    FROM ride_db.rides r
    WHERE r.status IN ('REQUESTED', 'ASSIGNED', 'DRIVER_ARRIVED', 'STARTED')
) ranked_active_rides
WHERE customer_ride_rank = 1;
