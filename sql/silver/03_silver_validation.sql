
-- SILVER CLEAN: Keep valid trips only

CREATE OR REFRESH MATERIALIZED VIEW silver_trips_clean (
    CONSTRAINT valid_distance
        EXPECT (distance_km > 0)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_rate
        EXPECT (rate_per_km >= 0)
        ON VIOLATION DROP ROW
)
COMMENT 'Validated ride-hailing trips for business analytics'
AS
SELECT *
FROM silver_trips_prepared
WHERE distance_km > 0
  AND rate_per_km >= 0;


-- SILVER REJECTED: Preserve invalid trips and reasons

CREATE OR REFRESH MATERIALIZED VIEW silver_trips_rejected
COMMENT 'Rejected trips with invalid distance or rate'
AS
SELECT
    *,
    CASE
        WHEN distance_km IS NULL
             OR distance_km <= 0
        THEN 'Invalid or missing distance'

        WHEN rate_per_km IS NULL
             OR rate_per_km < 0
        THEN 'Invalid or missing rate per km'
    END AS reject_reason

FROM silver_trips_prepared
WHERE distance_km IS NULL
   OR distance_km <= 0
   OR rate_per_km IS NULL
   OR rate_per_km < 0;
