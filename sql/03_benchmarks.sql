-- Test without composite index:
DROP INDEX IF EXISTS idx_bookings_segment_canceled;

EXPLAIN ANALYZE
SELECT market_segment, SUM(adr * total_stay_nights)
FROM hotel_bookings
WHERE is_canceled = 0
GROUP BY market_segment;


-- Test with composite index:
CREATE INDEX idx_bookings_segment_canceled ON hotel_bookings (market_segment, is_canceled);
EXPLAIN ANALYZE
SELECT market_segment, SUM(adr * total_stay_nights)
FROM hotel_bookings
WHERE is_canceled = 0
GROUP BY market_segment;
