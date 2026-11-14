
CREATE OR REFRESH MATERIALIZED VIEW silver_trips_prepared
COMMENT 'Deduplicated and standardized ride-hailing trip records'
AS

WITH typed_data AS (
    SELECT
        NULLIF(TRIM(trip_id), '') AS trip_id,
        NULLIF(TRIM(customer_id), '') AS customer_id,
        NULLIF(TRIM(driver_id), '') AS driver_id,

        TRY_CAST(trip_date AS DATE) AS trip_date,
        TRY_CAST(pickup_hour AS INT) AS pickup_hour,

        NULLIF(TRIM(city), '') AS city,
        NULLIF(TRIM(pickup_zone), '') AS pickup_zone,
        NULLIF(TRIM(region), '') AS region,
        NULLIF(TRIM(vehicle_type), '') AS vehicle_type,
        NULLIF(TRIM(booking_channel), '') AS booking_channel,

        TRY_CAST(distance_km AS DOUBLE) AS distance_km,
        TRY_CAST(duration_min AS INT) AS duration_min,
        TRY_CAST(rate_per_km AS DOUBLE) AS rate_per_km,
        TRY_CAST(fare_amount AS DOUBLE) AS fare_amount,
        TRY_CAST(discount_amount AS DOUBLE) AS discount_amount,
        TRY_CAST(driver_payout AS DOUBLE) AS driver_payout,

        NULLIF(TRIM(payment_method), '') AS payment_method,
        NULLIF(TRIM(payment_status), '') AS payment_status,

        TRY_CAST(customer_rating AS DOUBLE) AS customer_rating,
        NULLIF(TRIM(trip_type), '') AS trip_type,

        source_file,
        loaded_at

    FROM bronze_ride_trips
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY loaded_at DESC, source_file DESC
        ) AS row_num
    FROM typed_data
),

standardized AS (
    SELECT
        trip_id,
        customer_id,
        driver_id,
        trip_date,
        pickup_hour,

        COALESCE(INITCAP(city), 'Unknown') AS city,
        INITCAP(pickup_zone) AS pickup_zone,
        INITCAP(region) AS region,
        INITCAP(vehicle_type) AS vehicle_type,
        INITCAP(booking_channel) AS booking_channel,

        distance_km,
        duration_min,
        rate_per_km,

        COALESCE(
            fare_amount,
            distance_km * rate_per_km
        ) AS fare_amount,

        discount_amount,
        driver_payout,

        CASE
            WHEN UPPER(payment_method) = 'UPI' THEN 'UPI'
            WHEN UPPER(payment_method) = 'CASH' THEN 'Cash'
            WHEN UPPER(payment_method) = 'CARD' THEN 'Card'
            WHEN UPPER(payment_method) = 'WALLET' THEN 'Wallet'
            ELSE COALESCE(payment_method, 'Unknown')
        END AS payment_method,

        INITCAP(payment_status) AS payment_status,
        customer_rating,
        INITCAP(trip_type) AS trip_type,

        source_file,
        loaded_at

    FROM deduplicated
    WHERE row_num = 1
)

SELECT *
FROM standardized;
