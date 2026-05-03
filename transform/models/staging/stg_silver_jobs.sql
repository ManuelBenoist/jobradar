{{ config(materialized='view') }}

WITH source AS (
    /*
    Récupération des données brutes depuis le catalogue Glue (Bronze/Silver).
    On utilise la fonction source() pour maintenir le lineage dans dbt.
    */
    SELECT * FROM {{ source('glue_silver', 'silver_jobs') }}
),

renamed AS (
    /*
    Sélection explicite des colonnes et cast éventuel.
    Cette étape permet d'isoler le modèle des changements de schéma en amont.
    */
    SELECT
        -- Identifiants & Métadonnées
        job_id,
        source_name,
        url,
        CAST(data_quality_score AS DOUBLE) AS data_quality_score,
        ingestion_date,

        -- Informations de l'offre
        title,
        company_name,
        location_clean AS job_location,
        description,
        published_at,

        -- Caractéristiques extraites (NLP & Regex)
        extracted_skills,
        salary_min_numeric,
        exp_min_required,
        description_vector,

        -- Flags de filtrage et scoring
        is_junior,
        is_senior,
        is_red_flag,
        is_ethical,
        is_remote
    FROM source
)

SELECT * FROM renamed