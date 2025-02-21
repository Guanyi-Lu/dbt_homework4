{{
    config(
        materialized='table'
    )
}}

with fhv_data as (
    select *, 
        
        EXTRACT(YEAR FROM pickup_datetime) as PK_YEAR,
        EXTRACT(MONTH FROM pickup_datetime) as PK_MONTH
    from {{ ref('stg_fhv2019_data') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
fhv_data.dispatching_base_num,
fhv_data.pickup_datetime,
fhv_data.dropoff_datetime,
fhv_data.PK_YEAR,
fhv_data.PK_MONTH,
fhv_data.sr_flag,
fhv_data.affiliated_base_number,
fhv_data.pickup_location_id,
fhv_data.dropoff_location_id,
pickup_zone.borough as pickup_borough, 
pickup_zone.zone as pickup_zone, 
dropoff_zone.borough as dropoff_borough, 
dropoff_zone.zone as dropoff_zone,  
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_location_id = dropoff_zone.locationid
