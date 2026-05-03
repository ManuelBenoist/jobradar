{{ config(
    materialized='view',
    description='Vue finale optimisée pour le Dashboard Streamlit et FastAPI'
) }}

WITH scored_jobs AS (
    /* 
       Récupération des offres depuis le moteur de scoring.
       Seuil de pertinence fixé à 40 pour filtrer le bruit.
    */
    SELECT * 
    FROM {{ ref('fct_jobs') }}
    WHERE matching_score >= 40
),

final AS (
    /*
       Mapping des noms de colonnes pour une consommation directe par l'API.
       On simplifie les alias pour le Dashboard (ex: location_clean -> city).
    */
    SELECT 
        title,
        company_name,
        job_location AS city,
        salary_min_numeric AS salary_min,
        extracted_skills AS skills,
        TRIM(description) AS description, 
        published_at,
        matching_score,
        semantic_score,
        rules_score,
        positive_labels,
        negative_labels,
        url AS original_url,
        source_name AS platform,
        ingestion_date
    FROM scored_jobs
)

SELECT * 
FROM final
ORDER BY matching_score DESC, ingestion_date DESC