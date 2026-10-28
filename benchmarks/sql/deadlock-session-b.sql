-- Run in MySQL session B after session A has locked the Ride row.
START TRANSACTION;
SELECT offer_id
FROM ride_db.ride_offers
WHERE offer_id = UUID_TO_BIN('<offer-uuid>')
FOR UPDATE;

-- After session A blocks on the offer row, run this to complete the lock cycle.
SELECT ride_id
FROM ride_db.rides
WHERE ride_id = UUID_TO_BIN('<ride-uuid>')
FOR UPDATE;

ROLLBACK;
