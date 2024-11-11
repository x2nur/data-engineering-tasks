USE hive.divvy_trips;

-- 6. What is the top 10 ages of those that take the longest trips, and shortest?

WITH base AS (
	SELECT 
		YEAR(localtimestamp) - birthyear AS age
		,DENSE_RANK() OVER (ORDER BY max(tripduration)) AS longest_rank
		,DENSE_RANK() OVER (ORDER BY min(tripduration)) AS shortest_rank
	FROM trips
	WHERE birthyear IS NOT NULL 
	GROUP BY 1
)
SELECT _rank, shortest.age AS shortest_trips_ages, longest.age AS longest_trips_ages
FROM 
	UNNEST(sequence(1,10)) t(_rank) 
	LEFT JOIN 
	(
		SELECT array_agg(age ORDER BY age) AS age, shortest_rank AS _rank
		FROM base
		GROUP BY 2
	) shortest USING (_rank)
	LEFT JOIN
	(
		SELECT array_agg(age ORDER BY age) AS age, longest_rank AS _rank
		FROM base
		GROUP BY 2
	) longest USING (_rank)
ORDER BY 1;
 
