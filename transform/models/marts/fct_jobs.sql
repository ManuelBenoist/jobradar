{{ config(
    materialized='table',
    description='Moteur de scoring Hybride V3.0 - Règles Métier + IA Sémantique',
    external_location="s3://jobradar-curated-manuel-cloud/gold/fct_jobs/"
) }}

WITH profile AS (
    -- On récupère la chaîne de caractères JSON du seed et on la transforme en tableau de nombres (Vecteur)
    SELECT CAST(JSON_PARSE(ideal_vector) AS ARRAY<DOUBLE>) as ideal_vec
    FROM {{ ref('ideal_profile_vector') }}
),

jobs AS (
    SELECT * FROM {{ ref('stg_silver_jobs') }}
    -- On s'assure d'avoir des offres avec un vecteur valide
    WHERE description_vector IS NOT NULL 
      AND cardinality(description_vector) = 384
),

-- --- MOTEUR IA : CALCUL DE LA SIMILARITÉ COSINUS (VERSION OPTIMISÉE ATHENA) ---
vector_math AS (
    SELECT
        j.job_id,
        -- Formule Cosinus : Somme(A[i] * B[i]) / (SQRT(Somme(A[i]^2)) * SQRT(Somme(B[i]^2)))
        -- On utilise l'index i pour accéder aux deux tableaux simultanément
        SUM(CAST(j.description_vector[i] AS DOUBLE) * p.ideal_vec[i]) / (
            SQRT(SUM(CAST(j.description_vector[i] AS DOUBLE) * CAST(j.description_vector[i] AS DOUBLE))) *
            SQRT(SUM(p.ideal_vec[i] * p.ideal_vec[i]))
        ) AS cosine_similarity
    FROM jobs j
    CROSS JOIN profile p
    -- On crée une séquence de 1 à 384 (dimensions du modèle MiniLM)
    CROSS JOIN UNNEST(SEQUENCE(1, 384)) AS t(i)
    GROUP BY j.job_id
),
-- --- MOTEUR RÈGLES : VERSION DURCIE (V3.1) ---
pillars AS (
    SELECT
        job_id,
        -- 1. Bonus Junior (Inchangé)
        (CASE 
            WHEN is_junior = true THEN {{ var('bonus_junior_flag') }} 
            WHEN LOWER(title) LIKE '%junior%' OR LOWER(title) LIKE '%débutant%' THEN {{ var('bonus_junior_title') }} 
            ELSE 0 
        END) AS p_exp_bonus,

         -- 2. Pénalité Seniorité (Dynamique via YAML)
        (CASE 
            WHEN LOWER(title) LIKE '%directeur%' OR LOWER(title) LIKE '%principal%' THEN -100 -- Veto total
            WHEN LOWER(title) LIKE '%lead%' THEN {{ var('penalty_lead') }}
            WHEN LOWER(title) LIKE '%senior%' 
                 OR LOWER(title) LIKE '%expert%' 
                 OR LOWER(title) LIKE '%confirmé%' 
                 OR LOWER(title) LIKE '%experimenté%'
            THEN {{ var('penalty_senior') }}
            WHEN LOWER(description) LIKE '%expérience significative%' 
                 OR LOWER(description) LIKE '%solide expérience%'
                 OR LOWER(description) LIKE '%expérience confirmée%'
                 OR LOWER(description) LIKE '%plusieurs années d''expérience%'
            THEN {{ var('penalty_hidden_seniority') }}
            ELSE 0 
        END) AS p_exp_penalty,

        -- 3. VÉTO NUMÉRIQUE (Dynamique via YAML)
        (CASE 
            WHEN exp_min_required > 5 THEN {{ var('penalty_high_experience') }}
            WHEN exp_min_required > 2 THEN {{ var('penalty_mid_experience') }}
            ELSE 0 
        END) AS p_years_veto,

        -- 4. Pénalité Status
        (CASE WHEN LOWER(description) LIKE '%technicien%' THEN {{ var('penalty_technician') }} ELSE 0 END) AS p_status_penalty,
        
        -- 5. Éthique
        (CASE 
            WHEN LOWER(company_name) LIKE '%banque%' OR LOWER(description) LIKE '%banque%' OR LOWER(company_name) LIKE '%finance%' OR LOWER(description) LIKE '%finance%' OR LOWER(company_name) LIKE '%assurance%' OR LOWER(description) LIKE '%assurance%' OR LOWER(company_name) LIKE '%mutuelle%' THEN {{ var('penalty_ethics_light') }} 
            WHEN LOWER(title) LIKE '%armement%' OR LOWER(description) LIKE '%armement%' OR LOWER(description) LIKE '%missile%' OR LOWER(description) LIKE '%defense%' OR LOWER(description) LIKE '%militaire%' THEN {{ var('penalty_ethics_heavy') }} 
            ELSE 5 
        END) AS p_ethics,

        -- 6. Compétences Tech
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

-- --- 🧬 ASSEMBLAGE FINAL ---
final_calculation AS (
    SELECT
        j.job_id, j.title, j.company_name, j.location_clean, j.description, j.url,
        j.published_at, j.source_name, j.extracted_skills, j.salary_min_numeric,
        j.is_junior, j.is_senior, j.is_red_flag, j.is_ethical, j.is_remote, j.ingestion_date,
        
        -- On utilise COALESCE sur chaque pilier pour sécuriser l'addition (NULL + 10 = NULL sinon)
        (
            {{ var('base_score') }} + 
            COALESCE(p.p_exp_bonus, 0) + 
            COALESCE(p.p_exp_penalty, 0) + 
            COALESCE(p.p_years_veto, 0) + 
            COALESCE(p.p_status_penalty, 0) + 
            COALESCE(p.p_ethics, 0) + 
            COALESCE(p.p_tech_skills, 0) + 
            COALESCE(p.p_remote, 0) + 
            COALESCE(p.p_freshness, 0)
        ) AS raw_rule_score,
        
        COALESCE(vm.cosine_similarity * 100, 0) AS raw_nlp_score
        
    FROM jobs j
    LEFT JOIN pillars p ON j.job_id = p.job_id
    LEFT JOIN vector_math vm ON j.job_id = vm.job_id
)

SELECT 
    job_id, title, company_name, location_clean, description, url, published_at,
    source_name, extracted_skills, salary_min_numeric, is_junior, is_senior,
    is_red_flag, is_ethical, is_remote, ingestion_date,
    
    CASE WHEN raw_rule_score > 100 THEN 100 WHEN raw_rule_score < 0 THEN 0 ELSE raw_rule_score END AS rules_score,
    CASE WHEN raw_nlp_score > 100 THEN 100 WHEN raw_nlp_score < 0 THEN 0 ELSE ROUND(raw_nlp_score) END AS semantic_score,
    
    ROUND((
        (CASE WHEN raw_rule_score > 100 THEN 100 WHEN raw_rule_score < 0 THEN 0 ELSE raw_rule_score END) * 0.4 + 
        (CASE WHEN raw_nlp_score > 100 THEN 100 WHEN raw_nlp_score < 0 THEN 0 ELSE raw_nlp_score END) * 0.6
    )) AS matching_score

FROM final_calculation