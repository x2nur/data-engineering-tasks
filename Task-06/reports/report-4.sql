USE hive.divvy_trips;

-- 4. What were the top 3 trip stations each day for the last two weeks?

WITH base AS (
	SELECT 
		date_trunc('day',start_time) AS "day_round"
		,from_station_name AS station -- this time we assume from_station_name is accurate
		,count(1) AS "trips_count"
	FROM trips 
	WHERE start_time >= (
		SELECT date_add('week', -1, date_trunc('week',max(start_time))) 
		FROM trips)
	GROUP BY 1,2
)
SELECT 
	year(day_round) AS "year"
	, month(day_round) AS "month"
	, day(day_round) AS "day"
	, station
	, station_rank
	--, trips_count -- to debug
FROM (
	SELECT 
		day_round 
		,station
		,trips_count
		,DENSE_RANK() OVER (PARTITION BY day_round ORDER BY trips_count DESC) AS "station_rank"
	FROM base
) tmp
WHERE station_rank < 4
ORDER BY 1, 2, 3, 5;