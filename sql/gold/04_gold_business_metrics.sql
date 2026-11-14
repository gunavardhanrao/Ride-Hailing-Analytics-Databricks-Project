
-- 1. MONTHLY TRIP PERFORMANCE

CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_trips
AS
SELECT
    DATE_TRUNC('MONTH', trip_date) AS trip_month,
    COUNT(*) AS trips,
    SUM(distance_km) AS total_km,
    SUM(fare_amount - discount_amount) AS net_revenue,
    SUM(driver_payout) AS driver_payout,
    SUM(fare_amount - discount_amount)
        - SUM(driver_payout) AS platform_earnings
FROM silver_trips_clean
GROUP BY DATE_TRUNC('MONTH', trip_date);


-- 2. CITY AND REGION PERFORMANCE

CREATE OR REFRESH MATERIALIZED VIEW gold_city_summary
AS
SELECT
    region,
    city,
    COUNT(*) AS trips,
    SUM(fare_amount - discount_amount) AS net_revenue,
    AVG(fare_amount - discount_amount) AS avg_fare_per_trip
FROM silver_trips_clean
GROUP BY region, city;


-- 3. VEHICLE TYPE PERFORMANCE

CREATE OR REFRESH MATERIALIZED VIEW gold_vehicle_summary
AS
SELECT
    vehicle_type,
    COUNT(*) AS trips,
    SUM(distance_km) AS total_km,
    SUM(fare_amount - discount_amount) AS net_revenue,
    AVG(customer_rating) AS avg_rating
FROM silver_trips_clean
GROUP BY vehicle_type;


-- 4. PAYMENT METHOD PERFORMANCE

CREATE OR REFRESH MATERIALIZED VIEW gold_payment_summary
AS
SELECT
    payment_method,
    COUNT(*) AS trips,
    SUM(fare_amount - discount_amount) AS net_revenue,
    SUM(
        CASE
            WHEN payment_status = 'Pending'
            THEN fare_amount - discount_amount
            ELSE 0
        END
    ) AS pending_amount
FROM silver_trips_clean
GROUP BY payment_method;
