WITH adzuna AS (
    SELECT
        job_id,
        title,
        company_name,
        location,
        CAST(salary_min AS VARCHAR) AS salary_info,
        created_at,
        source_name
    FROM {{ ref('stg_adzuna') }}
),

france_travail AS (
    SELECT
        job_id,
        title,
        company_name,
        location,
        salary_raw AS salary_info,
        created_at,
        source_name
    FROM {{ ref('stg_france_travail') }}
),

-- 1. Unification des deux sources
unioned_jobs AS (
    SELECT * FROM adzuna
    UNION ALL
    SELECT * FROM france_travail
),

-- 2. Normalisation des données
normalized_jobs AS (
    SELECT
        job_id,
        title,
        company_name,
        salary_info,
        
        -- Nettoyage de la ville : on retire les codes postaux entre parenthèses ou à la fin
        -- Ex: "Nantes (44)" ou "44 - NANTES" devient "Nantes"
        TRIM(REGEXP_REPLACE(location, '\(?[0-9]{2,5}\)?|-', '', 'g')) AS location_clean,
        
        created_at,
        source_name
    FROM unioned_jobs
)

-- 3. Déduplication inter-sources
SELECT *
FROM normalized_jobs
-- On considère qu'il s'agit de la même offre si le titre et l'entreprise sont identiques
-- LOWER() permet d'ignorer la casse (majuscules/minuscules)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(title), LOWER(company_name) 
    ORDER BY created_at DESC
) = 1