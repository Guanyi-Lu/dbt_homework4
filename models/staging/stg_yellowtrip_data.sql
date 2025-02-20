{{ config(materialized='view') }} --by default, it is a view

WITH tripdata AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY (CAST(VendorID AS INTEGER), tpep_pickup_datetime)) AS rn
  FROM {{ source('staging', 'yellowtrip_data') }}
  WHERE VendorID IS NOT NULL 
)
--The ROW_NUMBER() function assigns a unique ranking (rn) to each row partitioned by (vendor_id, pickup_datetime, pickup_location_id). 
--This means that if there are multiple records with the same vendor_id, 
--pickup_datetime, and pickup_location_id, they will be numbered sequentially within that group.


  SELECT
    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} AS tripid,    
    {{ dbt.safe_cast("VendorID", api.Column.translate_type("integer")) }} AS vendorid,
    {{ dbt.safe_cast("RatecodeID", api.Column.translate_type("integer")) }} AS ratecodeid,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} AS pickup_locationid,
    {{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} AS dropoff_locationid,

    -- Timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- Trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    1 AS trip_type,

    -- Payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(0 AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description
  FROM tripdata
  WHERE rn = 1