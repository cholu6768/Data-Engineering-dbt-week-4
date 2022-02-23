{{ config(materialized='view') }}

SELECT 
    
    CAST(dispatching_base_num AS string) AS dispatch_base_num,
    CAST(PULocationID AS integer) AS  pickup_locationid,
    CAST(DOLocationID AS integer) AS dropoff_locationid,
    
    -- timestamps
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropoff_datetime AS timestamp) AS dropoff_datetime,
    
    -- shared ride
    CAST(sr_flag AS integer) AS shared_ride_flag


FROM {{ source('staging','fhv_tripdata') }}
WHERE 
    EXTRACT(YEAR FROM pickup_datetime) IN (2019) 
    
-- dbt build -m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=FALSE) %}

    LIMIT 100

{% endif %}