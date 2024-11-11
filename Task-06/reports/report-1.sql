USE hive.divvy_trips;

-- 1. What is the average trip duration per day?

WITH t AS (
    SELECT date(start_time) AS "date", avg(tripduration) AS duration
    FROM trips 
    GROUP BY 1
)
SELECT round(avg(duration)) as avg_duration_per_day
FROM t;
