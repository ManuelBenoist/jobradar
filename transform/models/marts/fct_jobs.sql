{{ config(
    materialized='table',
    description='Moteur de scoring Hybride V3.0 - Règles Métier + IA Sémantique',
    external_location="s3://jobradar-curated-manuel-cloud/gold/fct_jobs/"
) }}

WITH profile AS (
    SELECT CAST(JSON_PARSE(ideal_vector) AS ARRAY<DOUBLE>) as ideal_vec
    FROM {{ ref('ideal_profile_vector') }}
),

jobs AS (
    SELECT * FROM {{ ref('stg_silver_jobs') }}
    WHERE description_vector IS NOT NULL 
      AND cardinality(description_vector) = 384
),

-- --- 🧠 MOTEUR IA : SIMILARITÉ COSINUS ---
vector_math AS (
    SELECT
        j.job_id,
        SUM(CAST(j.description_vector[i] AS DOUBLE) * p.ideal_vec[i]) / (
            SQRT(SUM(CAST(j.description_vector[i] AS DOUBLE) * CAST(j.description_vector[i] AS DOUBLE))) *
            SQRT(SUM(p.ideal_vec[i] * p.ideal_vec[i]))
        ) AS cosine_similarity
    FROM jobs j
    CROSS JOIN profile p
    CROSS JOIN UNNEST(SEQUENCE(1, 384)) AS t(i)
    GROUP BY j.job_id
),

-- --- 🏗️ MOTEUR RÈGLES  ---
base_jobs AS (
    SELECT *, LOWER(title) as l_title, LOWER(description) as l_desc FROM jobs
),

scoring_blocks AS (
    SELECT
        job_id,
        -- Bloc Expérience & Veto
        CASE WHEN is_junior OR regexp_like(l_title, 'junior|débutant') THEN {{ var('bonus_junior_flag') }} ELSE 0 END as score_junior,
        CASE 
            WHEN regexp_like(l_title, 'directeur|principal') THEN -100
            WHEN regexp_like(l_title, 'lead') THEN {{ var('penalty_lead') }}
            WHEN regexp_like(l_title, 'senior|expert|confirmé|experimenté') THEN {{ var('penalty_senior') }}
            WHEN regexp_like(l_desc, 'expérience significative|solide expérience|expérience confirmée') THEN {{ var('penalty_hidden_seniority') }}
            ELSE 0 
        END as score_seniority,
        CASE WHEN regexp_like(l_title, 'stage|internship|alternance|apprentissage') OR l_desc LIKE '%gratification%' THEN {{ var('penalty_internship') }} ELSE 0 END as score_veto_intern,
        CASE WHEN exp_min_required > 5 THEN {{ var('penalty_high_experience') }} WHEN exp_min_required > 2 THEN {{ var('penalty_mid_experience') }} ELSE 0 END as score_years,
        
        -- Bloc Éthique & Impact
        CASE 
            WHEN regexp_like(l_title || l_desc || company_name, 'banque|finance|assurance|mutuelle|comptable|défense') THEN {{ var('penalty_ethics_light') }}
            WHEN regexp_like(l_title || l_desc, 'armement|missile|defense|militaire') THEN {{ var('penalty_ethics_heavy') }}
            ELSE 5 
        END as score_ethics,
        CASE WHEN regexp_like(l_desc, 'b corp|b-corp|société à mission|économie sociale') THEN {{ var('bonus_ethics') }} WHEN regexp_like(l_desc, 'décarbonation|écologie|climat') THEN 15 ELSE 0 END as score_impact,
        
        -- Bloc Culture & Status
        CASE WHEN regexp_like(l_desc, 'budget formation|mentorat|pairing|pair programming') THEN {{ var('bonus_training_mentorship') }} ELSE 0 END as score_training,
        CASE WHEN regexp_like(l_desc, 'open source|contribution|contributeur') THEN {{ var('bonus_open_source') }} ELSE 0 END as score_os,
        CASE WHEN l_desc LIKE '%technicien%' THEN {{ var('penalty_technician') }} ELSE 0 END as score_status,
        
        -- Bloc Vie (Remote & Freshness)
        CASE WHEN is_remote = true THEN {{ var('bonus_remote') }} ELSE 0 END as score_remote,
        CASE WHEN date_diff('day', published_at, current_timestamp) <= 2 THEN {{ var('bonus_freshness') }} ELSE 0 END as score_freshness
    FROM base_jobs
),

pillars AS (
    SELECT
        j.job_id,
        s.score_junior as p_exp_bonus,
        s.score_seniority as p_exp_penalty,
        s.score_veto_intern as p_internship_veto,
        s.score_years as p_years_veto,
        s.score_status as p_status_penalty,
        s.score_ethics as p_ethics,
        s.score_impact as p_impact_positive,
        s.score_training as p_culture_training,
        s.score_os as p_culture_os,
        s.score_remote as p_remote,
        s.score_freshness as p_freshness,
        (
            (CASE WHEN contains(extracted_skills, 'python') THEN 15 ELSE 0 END) +
            (CASE WHEN contains(extracted_skills, 'dbt') AND contains(extracted_skills, 'spark') THEN 15 ELSE 0 END) +
            (CASE WHEN contains(extracted_skills, 'dbt') AND regexp_like(array_join(extracted_skills, ','), 'snowflake|bigquery|airflow') THEN {{ var('bonus_tech_synergy') }} ELSE 0 END) +
            (CASE WHEN contains(extracted_skills, 'aws') OR contains(extracted_skills, 'S3') OR contains(extracted_skills, 'Lambda') THEN {{ var('bonus_cloud_stack') }} ELSE 0 END)
        ) as p_tech_skills
    FROM jobs j
    LEFT JOIN scoring_blocks s ON j.job_id = s.job_id
),

-- --- 🏷️ GÉNÉRATION DES LABELS LISIBLES ---
labels_extraction AS (
    SELECT
        job_id,
        -- Points Positifs
        ARRAY_JOIN(FILTER(ARRAY[
            CASE WHEN p_exp_bonus > 0 THEN '🌱 Junior Friendly' END,
            CASE WHEN p_impact_positive > 0 THEN '🌍 Impact Positif' END,
            CASE WHEN p_culture_training > 0 THEN '🎓 Mentorat/Formation' END,
            CASE WHEN p_culture_os > 0 THEN '💻 Open Source' END,
            CASE WHEN p_remote > 0 THEN '🏠 Télétravail' END,
            CASE WHEN p_tech_skills > 40 THEN '🚀 Stack de pointe' END
        ], x -> x IS NOT NULL), ' | ') AS positive_labels,

        -- Points Négatifs
        ARRAY_JOIN(FILTER(ARRAY[
            CASE WHEN p_internship_veto < 0 THEN '🚫 Stage/Alternance' END,
            CASE WHEN p_exp_penalty <= -50 THEN '⚠️ Seniorité élevée' END,
            CASE WHEN p_exp_penalty BETWEEN -49 AND -15 THEN '⚖️ Expérience requise' END,
            CASE WHEN p_ethics < 0 THEN '🏦 Secteur Sensible' END,
            CASE WHEN p_years_veto < 0 THEN '⏳ > 2 ans exp.' END
        ], x -> x IS NOT NULL), ' | ') AS negative_labels
    FROM pillars
),

-- --- 🧬 ASSEMBLAGE FINAL ---
final_calculation AS (
    SELECT
        j.*,
        l.positive_labels,
        l.negative_labels,
        (
            {{ var('base_score') }} + 
            COALESCE(p.p_exp_bonus, 0) + 
            COALESCE(p.p_exp_penalty, 0) + 
            COALESCE(p.p_internship_veto, 0) + 
            COALESCE(p.p_years_veto, 0) + 
            COALESCE(p.p_status_penalty, 0) + 
            COALESCE(p.p_ethics, 0) + 
            COALESCE(p.p_impact_positive, 0) + 
            COALESCE(p.p_culture_training, 0) + 
            COALESCE(p.p_culture_os, 0) + 
            COALESCE(p.p_tech_skills, 0) + 
            COALESCE(p.p_remote, 0) + 
            COALESCE(p.p_freshness, 0)
        ) AS raw_rule_score,
        COALESCE(vm.cosine_similarity * 100, 0) AS raw_nlp_score
    FROM jobs j
    LEFT JOIN pillars p ON j.job_id = p.job_id
    LEFT JOIN vector_math vm ON j.job_id = vm.job_id
    LEFT JOIN labels_extraction l ON j.job_id = l.job_id
)

SELECT 
    *,
    CASE WHEN raw_rule_score > 100 THEN 100 WHEN raw_rule_score < 0 THEN 0 ELSE raw_rule_score END AS rules_score,
    CASE WHEN raw_nlp_score > 100 THEN 100 WHEN raw_nlp_score < 0 THEN 0 ELSE ROUND(raw_nlp_score) END AS semantic_score,
    ROUND((
        (CASE WHEN raw_rule_score > 100 THEN 100 WHEN raw_rule_score < 0 THEN 0 ELSE raw_rule_score END) * 0.5 + 
        (CASE WHEN raw_nlp_score > 100 THEN 100 WHEN raw_nlp_score < 0 THEN 0 ELSE raw_nlp_score END) * 0.5
    )) AS matching_score
FROM final_calculation