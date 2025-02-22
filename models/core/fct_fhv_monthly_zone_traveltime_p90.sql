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
PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY PK_YEAR,PK_MONTH,pickup_location_id,dropoff_location_id ) AS trip_duration_P90
FROM target_table

),
final_table AS (
  SELECT
    PK_YEAR,
    PK_MONTH,
    dropoff_zone,
    pickup_zone,
    pickup_location_id,
    dropoff_location_id,
    trip_duration_P90
  FROM Percentiles
  WHERE PK_YEAR = 2019 
    AND PK_MONTH = 11 
    AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
),
Rank_table as (

  SELECT 
    dropoff_zone,
    pickup_zone,
    trip_duration_P90,
    DENSE_RANK() OVER (PARTITION BY PK_YEAR, PK_MONTH, pickup_zone ORDER BY trip_duration_P90 DESC) AS percentile_rank
  FROM final_table
)
select distinct dropoff_zone
from Rank_table
where percentile_rank=2