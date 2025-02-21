
{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type,
        
        EXTRACT(YEAR FROM pickup_datetime) as PK_YEAR,
        EXTRACT (QUARTER FROM pickup_datetime) AS PK_QTER
        
       
    from {{ ref('stg_greentrip_data') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type,
        EXTRACT(YEAR FROM pickup_datetime) as PK_YEAR,
        EXTRACT (QUARTER FROM pickup_datetime) AS PK_QTER
    from {{ ref('stg_yellowtrip_data') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
),
trips_unioned_YQ as (
select    
    trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.PK_YEAR,
    trips_unioned.PK_QTER,
    CONCAT(trips_unioned.PK_YEAR, '/Q', trips_unioned.PK_QTER) AS  Year_Quarter,
    
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
),
quarterly_revenue as ( 
  SELECT
    service_type,
    PK_YEAR,
    PK_QTER,
    Year_Quarter,
    SUM(total_amount) AS quarterly_total_amount
  FROM trips_unioned_YQ
  WHERE PK_YEAR in (2019,2020)
  GROUP BY 1, 2, 3, 4
),
Prev_Revenue as(
SELECT
    service_type,
    PK_YEAR,
    PK_QTER,
    Year_Quarter,
    quarterly_total_amount,
    LAG(quarterly_total_amount, 4, 0) OVER (PARTITION BY service_type ORDER BY PK_YEAR, PK_QTER) AS PrevYearQRevenue 
FROM quarterly_revenue
ORDER BY service_type, PK_YEAR, PK_QTER
)
SELECT
    Prev_Revenue.service_type,
    Prev_Revenue.Year_Quarter,
    CASE
        WHEN PrevYearQRevenue = 0 THEN NULL  
        ELSE (quarterly_total_amount - PrevYearQRevenue) * 100.0 / PrevYearQRevenue  
    END AS change_percentage
FROM Prev_Revenue

