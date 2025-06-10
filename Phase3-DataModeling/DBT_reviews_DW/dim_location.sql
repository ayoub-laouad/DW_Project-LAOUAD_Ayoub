{{ config(materialized='table') }}

SELECT DISTINCT
    location,
    city,
    MD5(location || city) AS location_id
FROM {{ source('src', 'final_reviews') }}
