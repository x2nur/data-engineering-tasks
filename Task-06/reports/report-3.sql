USE hive.divvy_trips;

-- 3. What was the most popular starting trip station for each month?

WITH base AS (
	SELECT date_trunc('month', start_time) AS "month_round", 
		from_station_id AS station_id, -- in case if from_station_name is not accurate
		count(1) AS "trip_count",
		max(count(1)) OVER (PARTITION BY date_trunc('month', start_time)) AS max_trips
	FROM trips 
	GROUP BY 1, 2
)
SELECT "year", "month", station_name
FROM (
	SELECT YEAR(month_round) AS "year", MONTH(month_round) AS "month", station_id, trip_count
	FROM base
	WHERE trip_count = max_trips
) AS tmp LEFT JOIN (
	SELECT DISTINCT from_station_id AS station_id, from_station_name AS station_name 
	FROM trips 
) AS station_names USING (station_id)
ORDER BY 1,2;