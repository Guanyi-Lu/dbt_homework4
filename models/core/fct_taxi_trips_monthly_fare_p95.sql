
{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type,
        
        EXTRACT(YEAR FROM pickup_datetime) as PK_YEAR,
        EXTRACT (MONTH FROM pickup_datetime) AS PK_MONTH
        
       
    from {{ ref('stg_greentrip_data') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type,
        EXTRACT(YEAR FROM pickup_datetime) as PK_YEAR,
        EXTRACT (MONTH FROM pickup_datetime) AS PK_MONTH
    from {{ ref('stg_yellowtrip_data') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
),
target_table as (
select    
    trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.PK_YEAR,
    trips_unioned.PK_MONTH,
    --CONCAT(trips_unioned.PK_YEAR, '/Q', trips_unioned.PK_QTER) AS  Year_Quarter,
    
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    
    trips_unioned.dropoff_locationid,
   
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
from trips_unioned
where  trips_unioned.fare_amount > 0
and trips_unioned.trip_distance > 0
and trips_unioned.payment_type_description in ('Cash', 'Credit card')
),
percentiles as (
SELECT service_type, 
PK_YEAR,
PK_MONTH,
APPROX_QUANTILES(fare_amount, 100) AS fare_percentiles
--Setting it to 100 ensures that you can extract any percentile value (like p90, p95, etc.) with good precision
FROM target_table
group by 1,2,3
),
PercentileValues AS (
  SELECT
        service_type,
        PK_YEAR,
        PK_MONTH,
        percentile,
        fare_percentiles[OFFSET(percentile)] AS approx_percentile_value
    FROM Percentiles, UNNEST([90, 95, 97]) AS percentile
    WHERE PK_YEAR=2020 AND PK_MONTH=4
)
SELECT * FROM PercentileValues ORDER BY service_type, PK_YEAR, PK_MONTH, percentile
--green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}