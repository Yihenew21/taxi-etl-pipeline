-- ============================================================================
-- NYC TAXI ETL PIPELINE - DATABASE TEST QUERIES
-- ============================================================================
-- This file contains comprehensive SQL queries to test and validate
-- the data loaded into the PostgreSQL database after ETL processing.
-- ============================================================================

-- ============================================================================
-- 1. BASIC DATA VALIDATION QUERIES
-- ============================================================================

-- Total record counts
SELECT COUNT(*) as total_trips FROM trips;
SELECT COUNT(*) as total_zones FROM zones;

-- Null value check in critical columns
SELECT 
    COUNT(*) as total_records,
    COUNT(tpep_pickup_datetime) as pickup_not_null,
    COUNT(tpep_dropoff_datetime) as dropoff_not_null,
    COUNT(fare_amount) as fare_not_null,
    COUNT(trip_distance) as distance_not_null
FROM trips;

-- ============================================================================
-- 2. DATA QUALITY VALIDATION QUERIES
-- ============================================================================

-- Check for invalid trip durations (negative or zero)
SELECT COUNT(*) as invalid_duration_trips
FROM trips 
WHERE trip_duration_minutes <= 0;

-- Check for invalid fare amounts
SELECT COUNT(*) as invalid_fare_trips
FROM trips 
WHERE fare_amount <= 0 OR fare_amount > 1000;

-- Check for invalid trip distances
SELECT COUNT(*) as invalid_distance_trips
FROM trips 
WHERE trip_distance <= 0 OR trip_distance > 500;

-- Check for trips where pickup is after dropoff
SELECT COUNT(*) as invalid_time_trips
FROM trips 
WHERE tpep_pickup_datetime >= tpep_dropoff_datetime;

-- ============================================================================
-- 3. COMPUTED FIELDS VALIDATION QUERIES
-- ============================================================================

-- Test trip duration calculation accuracy
SELECT 
    trip_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_duration_minutes,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 as calculated_duration
FROM trips 
WHERE trip_duration_minutes != EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60
LIMIT 5;

-- Test cost per mile calculation accuracy
SELECT 
    trip_id,
    fare_amount,
    trip_distance,
    cost_per_mile,
    ROUND(fare_amount / NULLIF(trip_distance, 0), 2) as calculated_cost_per_mile
FROM trips 
WHERE trip_distance > 0 
AND ABS(cost_per_mile - ROUND(fare_amount / trip_distance, 2)) > 0.01
LIMIT 5;

-- Test pickup hour extraction accuracy
SELECT 
    trip_id,
    tpep_pickup_datetime,
    pickup_hour,
    EXTRACT(HOUR FROM tpep_pickup_datetime) as calculated_hour
FROM trips 
WHERE pickup_hour != EXTRACT(HOUR FROM tpep_pickup_datetime)
LIMIT 5;

-- ============================================================================
-- 4. BUSINESS ANALYTICS QUERIES
-- ============================================================================

-- Overall trip statistics
SELECT 
    COUNT(*) as total_trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(trip_distance), 2) as avg_distance,
    ROUND(AVG(trip_duration_minutes), 1) as avg_duration,
    ROUND(AVG(cost_per_mile), 2) as avg_cost_per_mile
FROM trips;

-- Peak hours analysis
SELECT 
    pickup_hour,
    COUNT(*) as trip_count,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(trip_distance), 2) as avg_distance
FROM trips 
GROUP BY pickup_hour 
ORDER BY trip_count DESC 
LIMIT 10;

-- Payment method distribution
SELECT 
    payment_type,
    COUNT(*) as trip_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trips), 2) as percentage
FROM trips 
GROUP BY payment_type 
ORDER BY trip_count DESC;

-- Vendor distribution
SELECT 
    "VendorID",
    COUNT(*) as trip_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trips), 2) as percentage
FROM trips 
GROUP BY "VendorID" 
ORDER BY trip_count DESC;

-- ============================================================================
-- 5. GEOGRAPHIC ANALYSIS QUERIES
-- ============================================================================

-- Top pickup locations
SELECT 
    z.zone_name,
    z.borough,
    COUNT(*) as pickup_count,
    ROUND(AVG(t.fare_amount), 2) as avg_fare
FROM trips t
JOIN zones z ON t."PULocationID" = z.location_id
GROUP BY z.zone_name, z.borough
ORDER BY pickup_count DESC
LIMIT 10;

-- Top dropoff locations
SELECT 
    z.zone_name,
    z.borough,
    COUNT(*) as dropoff_count,
    ROUND(AVG(t.fare_amount), 2) as avg_fare
FROM trips t
JOIN zones z ON t."DOLocationID" = z.location_id
GROUP BY z.zone_name, z.borough
ORDER BY dropoff_count DESC
LIMIT 10;

-- Borough-to-borough trip analysis
SELECT 
    pu_borough.borough as pickup_borough,
    do_borough.borough as dropoff_borough,
    COUNT(*) as trip_count,
    ROUND(AVG(t.fare_amount), 2) as avg_fare,
    ROUND(AVG(t.trip_distance), 2) as avg_distance
FROM trips t
JOIN zones pu_borough ON t."PULocationID" = pu_borough.location_id
JOIN zones do_borough ON t."DOLocationID" = do_borough.location_id
GROUP BY pu_borough.borough, do_borough.borough
ORDER BY trip_count DESC
LIMIT 10;

-- ============================================================================
-- 6. DATA RELATIONSHIP VALIDATION QUERIES
-- ============================================================================

-- Check for orphaned pickup locations
SELECT COUNT(*) as orphaned_pickup_locations
FROM trips t
LEFT JOIN zones z ON t."PULocationID" = z.location_id
WHERE z.location_id IS NULL;

-- Check for orphaned dropoff locations
SELECT COUNT(*) as orphaned_dropoff_locations
FROM trips t
LEFT JOIN zones z ON t."DOLocationID" = z.location_id
WHERE z.location_id IS NULL;

-- Zone coverage analysis
SELECT 
    COUNT(DISTINCT "PULocationID") as unique_pickup_zones,
    COUNT(DISTINCT "DOLocationID") as unique_dropoff_zones,
    (SELECT COUNT(*) FROM zones) as total_zones
FROM trips;

-- ============================================================================
-- 7. PERFORMANCE METRICS QUERIES
-- ============================================================================

-- Trip duration distribution
SELECT 
    CASE 
        WHEN trip_duration_minutes < 5 THEN 'Very Short (<5 min)'
        WHEN trip_duration_minutes < 15 THEN 'Short (5-15 min)'
        WHEN trip_duration_minutes < 30 THEN 'Medium (15-30 min)'
        WHEN trip_duration_minutes < 60 THEN 'Long (30-60 min)'
        ELSE 'Very Long (>60 min)'
    END as duration_category,
    COUNT(*) as trip_count,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(trip_distance), 2) as avg_distance
FROM trips
GROUP BY 
    CASE 
        WHEN trip_duration_minutes < 5 THEN 'Very Short (<5 min)'
        WHEN trip_duration_minutes < 15 THEN 'Short (5-15 min)'
        WHEN trip_duration_minutes < 30 THEN 'Medium (15-30 min)'
        WHEN trip_duration_minutes < 60 THEN 'Long (30-60 min)'
        ELSE 'Very Long (>60 min)'
    END
ORDER BY trip_count DESC;

-- High-value trips analysis
SELECT 
    COUNT(*) as high_value_trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(trip_distance), 2) as avg_distance,
    ROUND(AVG(tip_amount), 2) as avg_tip
FROM trips 
WHERE fare_amount > 50;

-- Trip distance distribution
SELECT 
    CASE 
        WHEN trip_distance < 1 THEN 'Very Short (<1 mile)'
        WHEN trip_distance < 3 THEN 'Short (1-3 miles)'
        WHEN trip_distance < 10 THEN 'Medium (3-10 miles)'
        WHEN trip_distance < 25 THEN 'Long (10-25 miles)'
        ELSE 'Very Long (>25 miles)'
    END as distance_category,
    COUNT(*) as trip_count,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(cost_per_mile), 2) as avg_cost_per_mile
FROM trips
GROUP BY 
    CASE 
        WHEN trip_distance < 1 THEN 'Very Short (<1 mile)'
        WHEN trip_distance < 3 THEN 'Short (1-3 miles)'
        WHEN trip_distance < 10 THEN 'Medium (3-10 miles)'
        WHEN trip_distance < 25 THEN 'Long (10-25 miles)'
        ELSE 'Very Long (>25 miles)'
    END
ORDER BY trip_count DESC;

-- ============================================================================
-- 8. ADVANCED ANALYTICS QUERIES
-- ============================================================================

-- Daily trip patterns
SELECT 
    pickup_date,
    COUNT(*) as daily_trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(trip_distance), 2) as avg_distance
FROM trips
GROUP BY pickup_date
ORDER BY pickup_date
LIMIT 10;

-- Hourly trip patterns by day of week
SELECT 
    EXTRACT(DOW FROM tpep_pickup_datetime) as day_of_week,
    pickup_hour,
    COUNT(*) as trip_count,
    ROUND(AVG(fare_amount), 2) as avg_fare
FROM trips
GROUP BY EXTRACT(DOW FROM tpep_pickup_datetime), pickup_hour
ORDER BY day_of_week, pickup_hour
LIMIT 20;

-- Tip analysis
SELECT 
    CASE 
        WHEN tip_amount = 0 THEN 'No Tip'
        WHEN tip_amount < 2 THEN 'Small Tip (<$2)'
        WHEN tip_amount < 5 THEN 'Medium Tip ($2-5)'
        WHEN tip_amount < 10 THEN 'Large Tip ($5-10)'
        ELSE 'Very Large Tip (>$10)'
    END as tip_category,
    COUNT(*) as trip_count,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(tip_amount), 2) as avg_tip
FROM trips
GROUP BY 
    CASE 
        WHEN tip_amount = 0 THEN 'No Tip'
        WHEN tip_amount < 2 THEN 'Small Tip (<$2)'
        WHEN tip_amount < 5 THEN 'Medium Tip ($2-5)'
        WHEN tip_amount < 10 THEN 'Large Tip ($5-10)'
        ELSE 'Very Large Tip (>$10)'
    END
ORDER BY trip_count DESC;

-- ============================================================================
-- 9. DATA QUALITY SUMMARY QUERIES
-- ============================================================================

-- Data quality summary
SELECT 
    'Total Records' as metric,
    COUNT(*)::text as value
FROM trips
UNION ALL
SELECT 
    'Valid Duration Records',
    (COUNT(*) - COUNT(CASE WHEN trip_duration_minutes <= 0 THEN 1 END))::text
FROM trips
UNION ALL
SELECT 
    'Valid Fare Records',
    (COUNT(*) - COUNT(CASE WHEN fare_amount <= 0 OR fare_amount > 1000 THEN 1 END))::text
FROM trips
UNION ALL
SELECT 
    'Valid Distance Records',
    (COUNT(*) - COUNT(CASE WHEN trip_distance <= 0 OR trip_distance > 500 THEN 1 END))::text
FROM trips
UNION ALL
SELECT 
    'Valid Time Records',
    (COUNT(*) - COUNT(CASE WHEN tpep_pickup_datetime >= tpep_dropoff_datetime THEN 1 END))::text
FROM trips;

-- ============================================================================
-- 10. INDEX PERFORMANCE QUERIES
-- ============================================================================

-- Check if indexes are being used (run with EXPLAIN ANALYZE)
EXPLAIN ANALYZE SELECT * FROM trips WHERE pickup_hour = 12;
EXPLAIN ANALYZE SELECT * FROM trips WHERE pickup_date = '2019-01-01';
EXPLAIN ANALYZE SELECT * FROM trips WHERE "PULocationID" = 151;

-- ============================================================================
-- END OF DATABASE TEST QUERIES
-- ============================================================================
