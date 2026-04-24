{{ config(
    materialized='view',
    description='Vue finale simplifiée pour alimenter FastAPI et le Dashboard'
) }}

SELECT 
    title,
    company_name,
    location_clean AS city,      -- On renomme pour que ce soit plus parlant pour l'API
    salary_min_numeric AS salary_min,
    extracted_skills AS skills,
    TRIM(description) AS description, 
    published_at,
    matching_score,
    url AS original_url,
    source_name AS platform,
    ingestion_date
FROM {{ ref('fct_jobs') }}
WHERE matching_score >= 40       -- On cache les offres qui ne correspondent pas du tout
ORDER BY matching_score DESC, ingestion_date DESC