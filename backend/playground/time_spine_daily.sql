-- Time spine table for date dimension
-- This table is used for time-based aggregations
SELECT date_day
FROM UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2100-12-31')) AS date_day
