
CREATE OR REFRESH MATERIALIZED VIEW gold_data_quality
COMMENT 'Data quality metrics and end-to-end record reconciliation'
AS

WITH bronze_quality AS (
    SELECT
        COUNT(*) AS bronze_rows,

        COUNT(*) - COUNT(DISTINCT trip_id)
            AS duplicate_rows,

        COUNT_IF(TRY_CAST(distance_km AS DOUBLE) <= 0)
            AS invalid_distance_rows,

        COUNT_IF(TRY_CAST(rate_per_km AS DOUBLE) < 0)
            AS negative_rate_rows,

        COUNT_IF(city IS NULL OR TRIM(city) = '')
            AS missing_city_rows,

        COUNT_IF(payment_method IS NULL
                 OR TRIM(payment_method) = '')
            AS missing_payment_rows,

        COUNT_IF(fare_amount IS NULL)
            AS missing_fare_rows,

        COUNT_IF(customer_id IS NULL
                 OR TRIM(customer_id) = '')
            AS missing_customer_rows,

        COUNT_IF(customer_rating IS NULL)
            AS missing_rating_rows

    FROM bronze_ride_trips
),

silver_quality AS (
    SELECT
        (SELECT COUNT(*)
         FROM silver_trips_prepared) AS prepared_rows,

        (SELECT COUNT(*)
         FROM silver_trips_clean) AS clean_rows,

        (SELECT COUNT(*)
         FROM silver_trips_rejected) AS rejected_rows
),

reconciliation AS (
    SELECT
        b.*,
        s.*,

        b.bronze_rows
          - (s.clean_rows
             + s.rejected_rows
             + b.duplicate_rows)
          AS reconciliation_difference

    FROM bronze_quality b
    CROSS JOIN silver_quality s
)

SELECT
    *,
    CASE
        WHEN reconciliation_difference = 0
        THEN 'PASSED'
        ELSE 'FAILED'
    END AS reconciliation_status

FROM reconciliation;
