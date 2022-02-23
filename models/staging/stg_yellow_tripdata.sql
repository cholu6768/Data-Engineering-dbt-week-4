{{ config(materialized='view') }}

WITH tripdata AS 
(
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY vendorid, tpep_pickup_datetime) AS rn
  FROM {{ source('staging','yellow_tripdata') }}
  WHERE vendorid IS NOT NULL 
)

SELECT 
    {{ dbt_utils.surrogate_key( ['vendorid', 'tpep_pickup_datetime'] ) }} AS tripid,
    CAST(vendorid AS integer) AS vendorid,
    CAST(ratecodeid AS integer) AS ratecodeid,
    CAST(pulocationid AS integer) AS  pickup_locationid,
    CAST(dolocationid AS integer) AS dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime AS timestamp) AS pickup_datetime,
    cast(tpep_dropoff_datetime AS timestamp) AS dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    CAST(passenger_count AS integer) AS passenger_count,
    CAST(trip_distance AS numeric) AS trip_distance,
    1 AS trip_type,
    
    -- payment info
    CAST(fare_amount AS numeric) AS fare_amount,
    CAST(extra AS numeric) AS extra,
    CAST(mta_tax AS numeric) AS mta_tax,
    CAST(tip_amount AS numeric) AS tip_amount,
    CAST(tolls_amount AS numeric) AS tolls_amount,
    0 AS ehail_fee,
    CAST(improvement_surcharge AS numeric) AS improvement_surcharge,
    CAST(total_amount AS numeric) AS total_amount,
    CAST(payment_type AS integer) AS payment_type,
    {{ get_payment_type_description('payment_type') }} AS payment_type_description,
    CAST(congestion_surcharge AS numeric) AS congestion_surcharge

FROM tripdata
WHERE rn = 1
-- dbt build -m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=FALSE) %}

    LIMIT 100

{% endif %}

