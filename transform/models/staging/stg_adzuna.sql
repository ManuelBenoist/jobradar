WITH source AS (
    SELECT * FROM {{ source('jobradar_raw', 'raw_adzuna') }}
),

renamed AS (
    SELECT
        (job_data).id::VARCHAR AS job_id,
        (job_data).title::VARCHAR AS title,
        (job_data).company.display_name::VARCHAR AS company_name,
        (job_data).salary_min::FLOAT AS salary_min,
        (job_data).created::TIMESTAMP AS created_at,
        'Adzuna' AS source_name
    FROM source
    -- On ne garde que la version la plus récente de chaque job_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY (job_data).id ORDER BY (job_data).created DESC) = 1
)

SELECT * FROM renamed