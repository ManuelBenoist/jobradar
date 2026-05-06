-- Vérifie que url n'est jamais NULL dans la table gold
SELECT job_id, url
FROM {{ ref('fct_jobs') }}
WHERE url IS NULL OR TRIM(url) = ''
