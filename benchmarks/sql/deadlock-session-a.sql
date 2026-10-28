-- Run in MySQL session A, then follow benchmarks/RESULTS.md.
-- Use a disposable database and a Ride/Offer pair created for the experiment.
START TRANSACTION;
SELECT ride_id
FROM ride_db.rides
WHERE ride_id = UUID_TO_BIN('<ride-uuid>')
FOR UPDATE;

-- After session B locks the offer, run this and let it wait:
SELECT offer_id
FROM ride_db.ride_offers
WHERE offer_id = UUID_TO_BIN('<offer-uuid>')
FOR UPDATE;

ROLLBACK;
