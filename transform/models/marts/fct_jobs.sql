WITH unified_jobs AS (
    SELECT * FROM {{ ref('int_unified_jobs') }}
),

scored_jobs AS (
    SELECT
        *,
        -- 1. Flags booléens (Utiles pour les filtres du Dashboard)
        CASE WHEN title ILIKE '%python%' THEN 1 ELSE 0 END AS req_python,
        CASE WHEN title ILIKE '%sql%' THEN 1 ELSE 0 END AS req_sql,
        CASE WHEN title ILIKE '%dbt%' THEN 1 ELSE 0 END AS req_dbt,
        CASE WHEN title ILIKE '%docker%' OR title ILIKE '%kubernetes%' OR title ILIKE '%k8s%' OR title ILIKE '%terraform%' THEN 1 ELSE 0 END AS req_devops,
        CASE WHEN title ILIKE '%spark%' OR title ILIKE '%data engineer%' THEN 1 ELSE 0 END AS req_data,

        -- 2. Modulateurs de profil (Juniorité vs Séniorité)
        CASE WHEN title ILIKE '%junior%' OR title ILIKE '%jeune diplômé%' OR title ILIKE '%débutant%' THEN 40 ELSE 0 END AS score_junior,
        CASE WHEN title ILIKE '%senior%' OR title ILIKE '%expert%' OR title ILIKE '%lead%' OR title ILIKE '%confirmé%' THEN -60 ELSE 0 END AS score_seniority,

        -- 3. Red Flags (Élimination des stages/alternances/support)
        CASE WHEN title ILIKE '%alternance%' OR title ILIKE '%stage%' OR title ILIKE '%support%' OR title ILIKE '%technicien%' OR title ILIKE '%helpdesk%' THEN -100 ELSE 0 END AS score_red_flags,

        -- 4. Boost Éthique / Impact (Recherche titre ou entreprise)
        CASE 
            WHEN title ILIKE '%impact%' OR title ILIKE '%green%' OR title ILIKE '%environnement%' OR title ILIKE '%transition%' 
              OR company_name ILIKE '%coopérative%' OR company_name ILIKE '%scic%' OR company_name ILIKE '%scop%' OR company_name ILIKE '%ess%' 
            THEN 15 ELSE 0 
        END AS score_ethical,

        -- 5. Boost Salaire (Transparence & Montant)
        -- a. +10 points de base juste pour la transparence salariale (l'info existe)
        CASE WHEN salary_info IS NOT NULL AND salary_info != '' THEN 10 ELSE 0 END AS score_salary_transparency,
        -- b. +15 points si on arrive à extraire un nombre à 4 ou 5 chiffres >= 35000
        -- On supprime les espaces (ex: 35 000 -> 35000) et on extrait via regex
        CASE 
            WHEN TRY_CAST(REGEXP_EXTRACT(REPLACE(salary_info, ' ', ''), '[0-9]{4,}') AS INTEGER) >= 35000 THEN 15
            ELSE 0
        END AS score_salary_amount

    FROM unified_jobs
),

final_calculation AS (
    SELECT
        *,
        -- Calcul du score brut
        (
            40 + -- Base abaissée à 40 pour laisser de la marge aux bonus
            (req_python * 10) + 
            (req_sql * 10) + 
            (req_dbt * 10) + 
            (req_devops * 10) + 
            (req_data * 10) + 
            score_junior + 
            score_seniority + 
            score_red_flags + 
            score_ethical +
            score_salary_transparency +
            score_salary_amount
        ) AS raw_score
    FROM scored_jobs
)

-- 3. Plafonnement et Tri final
SELECT 
    job_id,
    title,
    company_name,
    location_clean,
    salary_info,
    created_at,
    source_name,
    req_python,
    req_sql,
    req_dbt,
    req_devops,
    req_data,
    -- Plafonnement entre 0 et 100
    CASE 
        WHEN raw_score > 100 THEN 100 
        WHEN raw_score < 0 THEN 0 
        ELSE raw_score 
    END AS matching_score
FROM final_calculation
ORDER BY matching_score DESC, created_at DESC