{{ config(materialized='table') }}

select 
    CAST(locationid AS INTEGER) AS locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone 
from {{ ref('taxi_zone_lookup') }} --create a table  in the dbt_glu_homework4 bq first, then this ref will work