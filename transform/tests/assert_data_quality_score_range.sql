-- Vérifie que data_quality_score est entre 0.0 et 1.0
SELECT job_id, data_quality_score
FROM {{ ref('fct_jobs') }}
WHERE data_quality_score < 0.0 OR data_quality_score > 1.0
