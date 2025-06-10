{{ config(materialized='table') }}

SELECT DISTINCT
    branch_name,
    MD5(branch_name) AS branch_id
FROM {{ source('src', 'final_reviews') }}

