{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ source('src', 'final_reviews') }}
)

SELECT
    MD5(id::text) AS review_id,
    MD5(bank_name) AS bank_id,
    MD5(branch_name) AS branch_id,
    MD5(location || city) AS location_id,
    MD5(sentiment || topic_number::text) AS sentiment_id,
    rating,
    review_date
FROM base
