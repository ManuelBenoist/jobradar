-- Vérifie que rules_score est toujours entre 0 et 100
SELECT job_id, rules_score
FROM {{ ref('fct_jobs') }}
WHERE rules_score < 0 OR rules_score > 100
