USE hive.divvy_trips;

-- 2. How many trips were taken each day?

SELECT date(start_time) AS "date", count(1) AS "count"
FROM trips 
GROUP BY 1
ORDER BY 1;