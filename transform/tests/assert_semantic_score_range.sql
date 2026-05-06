-- Vérifie que semantic_score est toujours entre 0 et 100
SELECT job_id, semantic_score
FROM {{ ref('fct_jobs') }}
WHERE semantic_score < 0 OR semantic_score > 100
