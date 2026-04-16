WITH source AS (
    SELECT * FROM {{ source('jobradar_raw', 'raw_france_travail') }}
),

renamed AS (
    SELECT
        (job_data).id::VARCHAR AS job_id,
        (job_data).intitule::VARCHAR AS title,
        COALESCE((job_data).entreprise.nom, 'Non renseigné')::VARCHAR AS company_name,
        (job_data).salaire.libelle::VARCHAR AS salary_raw,
        (job_data).lieuTravail.libelle::VARCHAR AS location,
        (job_data).dateCreation::TIMESTAMP AS created_at,
        'France Travail' AS source_name
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY (job_data).id ORDER BY (job_data).dateCreation DESC) = 1
)

SELECT * FROM renamed