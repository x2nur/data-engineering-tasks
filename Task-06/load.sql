CREATE SCHEMA IF NOT EXISTS hive.divvy_trips WITH ( LOCATION = 's3a://dwh/divvy-trips/' );

CREATE TABLE IF NOT EXISTS hive.divvy_trips.raw_trips
(
	trip_id VARCHAR,
	start_time VARCHAR,
	end_time VARCHAR,
	bikeid VARCHAR,
	tripduration VARCHAR,
	from_station_id VARCHAR,
	from_station_name VARCHAR,
	to_station_id VARCHAR,
	to_station_name VARCHAR,
	usertype VARCHAR,
	gender VARCHAR,
	birthyear VARCHAR
) 
WITH (
	external_location = 's3a://raw/trips',
	format='CSV',
	skip_header_line_count=1
);


CREATE TABLE IF NOT EXISTS hive.divvy_trips.trips
AS
SELECT 	
	try_cast(trip_id AS BIGINT) AS trip_id,
	TRY_CAST(start_time AS TIMESTAMP) AS start_time,
	TRY_CAST(end_time AS TIMESTAMP) AS end_time,
	TRY_CAST(bikeid AS INTEGER) AS bikeid,
	TRY_CAST(REPLACE(tripduration, ',','') AS REAL) AS tripduration,
	TRY_CAST(from_station_id AS INTEGER) AS from_station_id,
	from_station_name,
	TRY_CAST(to_station_id AS INTEGER) AS to_station_id,
	to_station_name,
	usertype,
	gender,
	TRY_CAST(birthyear AS SMALLINT) AS birthyear
FROM hive.divvy_trips.raw_trips;