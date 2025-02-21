{{
    config(
        materialized='table'
    )
}}

with target_table as (
select 
TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
PK_YEAR,
PK_MONTH,
pickup_location_id,
dropoff_location_id,
dropoff_zone,
pickup_zone
from {{ ref('dim_fhv_trips') }} --when source is already built
),
Percentiles as (
SELECT 
target_table.PK_YEAR,
target_table.PK_MONTH,
target_table.pickup_location_id,
target_table.dropoff_location_id,
target_table.dropoff_zone,
target_table.pickup_zone,
APPROX_QUANTILES(trip_duration, 100) AS duration_percentiles
FROM target_table
group by 1,2,3,4,5,6
),
final_table AS (
  SELECT
    PK_YEAR,
    PK_MONTH,
    dropoff_zone,
    pickup_zone,
    pickup_location_id,
    dropoff_location_id,
    duration_percentiles[OFFSET(90)] AS approx_percentile_value
  FROM Percentiles
  WHERE PK_YEAR = 2019 
    AND PK_MONTH = 11 
    AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
),
ranked_table AS 
(

  SELECT 
    dropoff_zone,
    pickup_zone,
    approx_percentile_value,
    DENSE_RANK() OVER (PARTITION BY PK_YEAR, PK_MONTH, pickup_zone ORDER BY approx_percentile_value DESC) AS percentile_rank
  FROM final_table
)
SELECT dropoff_zone, pickup_zone, percentile_rank
FROM ranked_table
WHERE percentile_rank = 2
