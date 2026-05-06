-- Vérifie que matching_score est toujours entre 0 et 100
SELECT job_id, matching_score
FROM {{ ref('fct_jobs') }}
WHERE matching_score < 0 OR matching_score > 100
