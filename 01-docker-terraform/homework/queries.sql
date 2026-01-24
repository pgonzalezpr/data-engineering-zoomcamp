/* Queries for answering homework questions */

-------------------------------------------------

/* Question 3. Counting short trips */

SELECT COUNT(*)
FROM green_trip_data
WHERE lpep_pickup_datetime >='2025-11-01'
	AND lpep_pickup_datetime < '2025-12-01'
	AND trip_distance <= 1.0

/* Question 4. Longest trip for each day */

SELECT *
FROM green_trip_data
WHERE trip_distance < 100.0
ORDER BY trip_distance DESC
LIMIT 1;

/* Question 5. Biggest pickup zone */

SELECT
  z."Zone" AS "zone",
  SUM(t."total_amount") AS "total_amount_sum"
FROM green_trip_data t
JOIN zones z
  ON t."PULocationID" = z."LocationID"
WHERE DATE(t."lpep_pickup_datetime") = '2025-11-18'
GROUP BY z."Zone"
ORDER BY "total_amount_sum" DESC
LIMIT 1;

/* Question 6. Largest tip */

SELECT
    t."lpep_pickup_datetime",
    t."tip_amount",
    pz."Zone" AS "pickup_zone",
    dz."Zone" AS "dropoff_zone"
FROM green_trip_data t
JOIN zones pz
    ON t."PULocationID" = pz."LocationID"
JOIN zones dz
    ON t."DOLocationID" = dz."LocationID"
WHERE t."lpep_pickup_datetime" >= '2025-11-01'
	AND t."lpep_pickup_datetime" < '2025-12-01'
    AND pz."Zone" = 'East Harlem North'
ORDER BY t."tip_amount" DESC
LIMIT 1;





