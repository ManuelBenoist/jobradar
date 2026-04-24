{{ config(
    materialized='table',
    description='Moteur de scoring V2.1 - Version explicite (Anti-Ambiguity)',
    external_location="s3://jobradar-curated-manuel-cloud/gold/fct_jobs/"
) }}

WITH jobs AS (
    SELECT * FROM {{ ref('stg_silver_jobs') }}
),

-- Calcul des scores dans une CTE isolée
pillars AS (
    SELECT
        job_id,
        (CASE WHEN is_junior = true THEN {{ var('bonus_junior_flag') }} WHEN LOWER(title) LIKE '%junior%' THEN {{ var('bonus_junior_title') }} ELSE 0 END) AS p_exp_bonus,
        (CASE WHEN LOWER(title) LIKE '%senior%' OR LOWER(title) LIKE '%expert%' OR LOWER(title) LIKE '%confirmé%' OR LOWER(title) LIKE '%experimenté%' THEN {{ var('penalty_senior') }} ELSE 0 END) AS p_exp_penalty,
        (CASE WHEN LOWER(description) LIKE '%technicien%' THEN {{ var('penalty_technician') }} ELSE 0 END) AS p_status_penalty,
        (CASE WHEN LOWER(company_name) LIKE '%banque%' OR LOWER(description) LIKE '%banque%' OR LOWER(company_name) LIKE '%finance%' OR LOWER(description) LIKE '%finance%' OR LOWER(company_name) LIKE '%assurance%' OR LOWER(description) LIKE '%assurance%' OR LOWER(company_name) LIKE '%mutuelle%' THEN {{ var('penalty_ethics_light') }} WHEN LOWER(title) LIKE '%armement%' OR LOWER(description) LIKE '%armement%' OR LOWER(description) LIKE '%missile%' OR LOWER(description) LIKE '%defense%' OR LOWER(description) LIKE '%militaire%' THEN {{ var('penalty_ethics_heavy') }} ELSE 5 END) AS p_ethics,
        (
            (CASE WHEN contains(extracted_skills, 'python') OR contains(extracted_skills, 'pandas') THEN 15 WHEN contains(extracted_skills, 'sql') THEN 10 ELSE 0 END) +
            (CASE WHEN contains(extracted_skills, 'dbt') AND (contains(extracted_skills, 'spark') OR contains(extracted_skills, 'pyspark')) THEN 15 WHEN contains(extracted_skills, 'dbt') OR contains(extracted_skills, 'spark') OR contains(extracted_skills, 'pyspark') THEN 10 ELSE 0 END) +
            (CASE WHEN contains(extracted_skills, 'aws') OR contains(extracted_skills, 'S3') OR contains(extracted_skills, 'Lambda') OR contains(extracted_skills, 'Athena') THEN {{ var('bonus_cloud_stack') }} ELSE 0 END) +
            (CASE WHEN contains(extracted_skills, 'docker') OR contains(extracted_skills, 'terraform') OR contains(extracted_skills, 'github_actions') OR contains(extracted_skills, 'cicd') THEN 8 ELSE 0 END) +
            (CASE WHEN contains(extracted_skills, 'architecture_medaillon') OR contains(extracted_skills, 'etl') OR contains(extracted_skills, 'elt') THEN 5 ELSE 0 END)
        ) AS p_tech_skills,
        (CASE WHEN is_remote = true THEN {{ var('bonus_remote') }} ELSE 0 END) AS p_remote,
        (CASE WHEN date_diff('day', published_at, current_timestamp) <= 2 THEN {{ var('bonus_freshness') }} ELSE 0 END) AS p_freshness
    FROM jobs
),

-- Assemblage final avec SELECT explicite (Pas de j.*)
final_calculation AS (
    SELECT
        -- On liste les colonnes de l'offre une par une
        j.job_id,
        j.title,
        j.company_name,
        j.location_clean,
        j.description,
        j.url,
        j.published_at,
        j.source_name,
        j.extracted_skills,
        j.salary_min_numeric,
        j.is_junior,
        j.is_senior,
        j.is_red_flag,
        j.is_ethical,
        j.is_remote,
        j.ingestion_date,
        -- On ajoute les scores calculés
        (
            {{ var('base_score') }} + p.p_exp_bonus + p.p_exp_penalty + p.p_status_penalty + 
            p.p_ethics + p.p_tech_skills + p.p_remote + p.p_freshness
        ) AS raw_score
    FROM jobs j
    LEFT JOIN pillars p ON j.job_id = p.job_id
)

SELECT 
    job_id, title, company_name, location_clean, description, url, published_at,
    source_name, extracted_skills, salary_min_numeric, is_junior, is_senior,
    is_red_flag, is_ethical, is_remote, ingestion_date,
    CASE 
        WHEN raw_score > 100 THEN 100 
        WHEN raw_score < 0 THEN 0 
        ELSE raw_score 
    END AS matching_score
FROM final_calculation