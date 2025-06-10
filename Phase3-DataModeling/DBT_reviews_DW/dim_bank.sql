{{ config(materialized='table') }}

SELECT DISTINCT
    bank_name,
    MD5(bank_name) AS bank_id
FROM {{ source('src', 'final_reviews') }}

