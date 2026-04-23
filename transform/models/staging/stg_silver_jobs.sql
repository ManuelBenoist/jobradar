-- Ce modèle sert de "pont" propre entre Glue et dbt
SELECT
    job_id,
    title,
    company_name,
    location_clean,
    description,
    published_date,
    extracted_skills,
    salary_min_numeric,
    is_junior,
    is_senior,
    is_red_flag,
    is_ethical,
    source_name,
    url,
    ingestion_date
FROM {{ source('glue_silver', 'silver_jobs') }}