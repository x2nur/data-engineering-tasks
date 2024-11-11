USE hive.divvy_trips;

-- 5. Do Males or Females take longer trips on average?

SELECT if((SELECT avg(tripduration) FROM trips WHERE gender = 'Male') > 
		  (SELECT avg(tripduration) FROM trips WHERE gender = 'Female'),
		  'Males',
		  'Females') || ' take longer trips on average' AS result;