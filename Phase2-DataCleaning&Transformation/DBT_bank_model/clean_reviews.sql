-- Récupération des données brutes
with raws as (
    select 
        Bank_name,
        Branch_name,
        Location,
        Review_text,
        Rating,
        Review_date
    FROM {{ source('my_source', 'reviews') }}
),

-- Suppression des doublons
duplicate as (
    select distinct *
    from raws
),

-- Normalisation : mise en minuscules et transformation des dates et des notes
lowercase as (
    select
        upper(Bank_name) as Bank_name,
        lower(Branch_name) as Branch_name,
        upper(Location) as Location,    
        lower(Review_text) as Review_text,
        case
            when Rating ~ '[0-9]+' then cast(regexp_replace(Rating, '[^\d]', '', 'g') as float)
            else null
        end as Rating,
        (case
            WHEN Review_date ~ 'un an' THEN current_date - 365 
            WHEN Review_date ~ 'un mois' THEN current_date - 30
            when Review_date ~ 'une semaine' then current_date - 7
            when Review_date ~ 'un jour' then current_date - 1
            when Review_date ~ 'heure' then current_date
            when Review_date ~ 'jour' then current_date - (nullif(regexp_replace(Review_date, '[^\d]', '', 'g'), '')::int)
            when Review_date ~ 'mois' then current_date - (nullif(regexp_replace(Review_date, '[^\d]', '', 'g'), '')::int * 30)
            when Review_date ~ 'an' then current_date - (nullif(regexp_replace(Review_date, '[^\d]', '', 'g'), '')::int * 365)
            when Review_date ~ 'semaine' then current_date - (nullif(regexp_replace(Review_date, '[^\d]', '', 'g'), '')::int * 7)
            when Review_date ~ 'heure' then current_date - (nullif(regexp_replace(Review_date, '[^\d]', '', 'g'), '')::int / 24)
            else null
        end)::DATE as Review_date
    from duplicate
),

-- Suppression de la ponctuation
punctuation as (
    select
        Bank_name,
        Branch_name,
        Location,
        regexp_replace(Review_text, '[^\w\s]', '', 'g') as Review_text,
        Rating,
        Review_date
    from lowercase
),

-- Suppression des stop words
normalized as (
    select
        Bank_name,
        Branch_name,
        Location,
        regexp_replace(
            Review_text,
            '\b(le|la|les|un|une|des|du|de|ce|cet|cette|ces|et|ou|mais|donc|or|ni|car|avec|sans|dans|sur|sous|par|pour|en|au|aux|se|son|sa|ses|mon|ma|mes|ton|ta|tes|notre|nos|votre|vos|leur|leurs|que|qui|quoi|dont|où|comme|si|lorsque|quand|alors|bien|très|plus|moins|encore|aussi|ainsi|tel|telle|tels|telles|ceci|cela|celui|celle|ceux|celles|on|nous|vous|ils|elles|il|elle|je|tu|me|te|moi|toi|soi|eux|elles)\b',
            '', 'g'
        ) as Review_text,
        Rating,
        Review_date
    from punctuation
),

-- Handle missing values
clean_data AS (
    SELECT 
        COALESCE(Bank_name, 'Unknown') AS Bank_name,
        COALESCE(Branch_name, 'Unknown') AS Branch_name,
        COALESCE(Location, 'Unknown') AS Location,
        COALESCE(NULLIF(Review_text, ''), 'Avis non disponible') AS Review_text,
        NULLIF(Rating, null) AS Rating,
        NULLIF(Review_date, null) AS Review_date
    FROM normalized
)

select * from clean_data