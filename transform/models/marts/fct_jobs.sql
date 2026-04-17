{{ config(
    materialized='table',
    description='Calcul du score de matching final basé sur les colonnes enrichies par Spark'
) }}

WITH jobs AS (
    SELECT * FROM {{ ref('stg_silver_jobs') }}
),

scoring AS (
    SELECT
        *,
        -- 1. Points Techniques (Basés sur l'ARRAY généré par Spark)
        -- On utilise contains() qui est la fonction native Athena pour les tableaux
        (CASE WHEN contains(extracted_skills, 'python') THEN 15 ELSE 0 END) +
        (CASE WHEN contains(extracted_skills, 'sql') THEN 10 ELSE 0 END) +
        (CASE WHEN contains(extracted_skills, 'dbt') THEN 15 ELSE 0 END) +
        (CASE WHEN contains(extracted_skills, 'spark') THEN 10 ELSE 0 END) +
        (CASE WHEN contains(extracted_skills, 'aws') THEN 10 ELSE 0 END) AS score_skills,

        -- 2. Points de Profil (Déjà calculés par Spark en Boolean)
        (CASE WHEN is_junior = true THEN 30 ELSE 0 END) AS score_junior,
        (CASE WHEN is_senior = true THEN -75 ELSE 0 END) AS score_senior, -- On évite les postes senior
        
        -- 3. Red Flags & Éthique (Déjà flagués par Spark)
        (CASE WHEN is_red_flag = true THEN -75 ELSE 0 END) AS penalty_red_flag,
        (CASE WHEN is_ethical = true THEN 15 ELSE 0 END) AS bonus_ethical,

        -- 4. Salaire (Déjà converti en nombre par Spark)
        (CASE WHEN salary_min_numeric >= 35000 THEN 15 
              WHEN salary_min_numeric IS NOT NULL THEN 5 
              ELSE 0 END) AS score_salary

    FROM jobs
),

final_calculation AS (
    SELECT
        *,
        -- Calcul du score brut (Base de 30 points)
        (30 + score_skills + score_junior + score_senior + penalty_red_flag + bonus_ethical + score_salary) AS raw_score
    FROM scoring
)

SELECT 
    job_id,
    title,
    company_name,
    location_clean,
    description,
    salary_min_numeric,
    extracted_skills,
    source_name,
    url,
    ingestion_date,
    is_junior,
    is_red_flag,
    -- Plafonnement entre 0 et 100
    CASE 
        WHEN raw_score > 100 THEN 100 
        WHEN raw_score < 0 THEN 0 
        ELSE raw_score 
    END AS matching_score
FROM final_calculation