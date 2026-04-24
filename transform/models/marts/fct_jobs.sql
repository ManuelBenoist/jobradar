{{ config(
    materialized='table',
    description='Moteur de scoring V2.1 - Configurable et Remote-aware',
    external_location="s3://jobradar-curated-manuel-cloud/gold/fct_jobs/"
) }}

WITH jobs AS (
    SELECT * FROM {{ ref('stg_silver_jobs') }}
),

scoring_pillars AS (
    SELECT
        *,
        -- PILLIER 1 : EXPÉRIENCE & STATUT
        (CASE 
            WHEN is_junior = true THEN {{ var('bonus_junior_flag') }}
            WHEN LOWER(title) LIKE '%junior%' THEN {{ var('bonus_junior_title') }}
            ELSE 0 
        END) AS p_exp_bonus,

        (CASE 
            WHEN LOWER(title) LIKE '%senior%' 
                 OR LOWER(title) LIKE '%expert%' 
                 OR LOWER(title) LIKE '%confirmé%' 
                 OR LOWER(title) LIKE '%experimenté%' THEN {{ var('penalty_senior') }} -- Mis à jour
            ELSE 0 
        END) AS p_exp_penalty,

        (CASE WHEN LOWER(description) LIKE '%technicien%' THEN {{ var('penalty_technician') }} ELSE 0 END) AS p_status_penalty,

        (CASE WHEN LOWER(description) LIKE '%technicien%' THEN {{ var('penalty_technician', -10) }} ELSE 0 END) AS p_status_penalty,

        -- PILLIER 2 : ÉTHIQUE & SECTEUR
        (CASE 
            WHEN LOWER(company_name) LIKE '%banque%' OR LOWER(description) LIKE '%banque%'
                 OR LOWER(company_name) LIKE '%finance%' OR LOWER(description) LIKE '%finance%'
                 OR LOWER(company_name) LIKE '%assurance%' OR LOWER(description) LIKE '%assurance%'
                 OR LOWER(company_name) LIKE '%mutuelle%' THEN {{ var('penalty_ethics_light') }} -- Mis à jour
            WHEN LOWER(title) LIKE '%armement%' OR LOWER(description) LIKE '%armement%'
                 OR LOWER(description) LIKE '%missile%' OR LOWER(description) LIKE '%defense%'
                 OR LOWER(description) LIKE '%militaire%' THEN {{ var('penalty_ethics_heavy') }} -- Mis à jour
            ELSE 5 
        END) AS p_ethics,

                -- PILLIER 3 : COMPÉTENCES TECHNIQUES (V2 par familles)
        (
            -- GROUPE 1 : LANGAGES (Python/Pandas/SQL) - Max 15pts
            (CASE 
                WHEN contains(extracted_skills, 'python') OR contains(extracted_skills, 'pandas') THEN 15 
                WHEN contains(extracted_skills, 'sql') THEN 10 
                ELSE 0 
            END) +

            -- GROUPE 2 : DATA STACK (dbt/Spark/Airflow) - Max 15pts
            (CASE 
                WHEN contains(extracted_skills, 'dbt') AND (contains(extracted_skills, 'spark') OR contains(extracted_skills, 'pyspark')) THEN 15
                WHEN contains(extracted_skills, 'dbt') OR contains(extracted_skills, 'spark') OR contains(extracted_skills, 'pyspark') THEN 10
                ELSE 0 
            END) +

            -- GROUPE 3 : CLOUD AWS (Bonus fixe pour la stack) - Max 12pts
            (CASE 
                WHEN contains(extracted_skills, 'aws') 
                     OR contains(extracted_skills, 'S3') 
                     OR contains(extracted_skills, 'Lambda') 
                     OR contains(extracted_skills, 'Athena') THEN {{ var('bonus_cloud_stack', 12) }}
                ELSE 0 
            END) +

            -- GROUPE 4 : DEVOPS & CI/CD - Max 8pts
            (CASE 
                WHEN contains(extracted_skills, 'docker') 
                     OR contains(extracted_skills, 'terraform') 
                     OR contains(extracted_skills, 'github_actions') 
                     OR contains(extracted_skills, 'cicd') THEN 8
                ELSE 0 
            END) +

            -- GROUPE 5 : CONCEPTS (Culture Data) - Max 5pts
            (CASE 
                WHEN contains(extracted_skills, 'architecture_medaillon') 
                     OR contains(extracted_skills, 'etl') 
                     OR contains(extracted_skills, 'elt') THEN 5
                ELSE 0 
            END)
        ) AS p_tech_skills,

        -- PILLIER 4 : REMOTE & WORK-LIFE BALANCE
        (CASE 
            WHEN is_remote = true THEN {{ var('bonus_remote', 10) }} 
            ELSE 0 
        END) AS p_remote,

        -- PILLIER 5 : FRAÎCHEUR
        (CASE WHEN date_diff('day', published_at, current_timestamp) <= 2 THEN {{ var('bonus_freshness', 5) }} ELSE 0 END) AS p_freshness

    FROM jobs
),

weighted_final AS (
    SELECT
        *,
        ({{ var('base_score', 20) }} + p_exp_bonus + p_exp_penalty + p_status_penalty + p_ethics + p_tech_skills + p_remote + p_freshness) AS raw_score
    FROM scoring_pillars
)

SELECT 
    *,
    CASE 
        WHEN raw_score > 100 THEN 100 
        WHEN raw_score < 0 THEN 0 
        ELSE raw_score 
    END AS matching_score
FROM weighted_final