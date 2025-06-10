{{ config(materialized='table') }}

SELECT DISTINCT
    sentiment,
    topic_number,
    topic_words,
    topic_meaning,
    MD5(sentiment || topic_number::text) AS sentiment_id
FROM {{ source('src', 'final_reviews') }}

