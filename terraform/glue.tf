# ============================================================================
# DATA CATALOG : GLUE DATABASE & SILVER TABLE
# ============================================================================

resource "aws_glue_catalog_database" "jobradar_db" {
  name        = "jobradar_db"
  description = "Base de données pour les couches Silver et Gold du projet JobRadar"
}

resource "aws_glue_catalog_table" "processed_jobs" {
  name          = "silver_jobs"
  database_name = aws_glue_catalog_database.jobradar_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"   = "parquet"
    "compressionType"  = "snappy"
    "typeOfData"       = "file"
  }

  storage_descriptor {
    location      = "s3://jobradar-processed-manuel-cloud/silver_jobs/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-ser-de"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Schéma des colonnes (Couche Silver enrichie par PySpark)
    columns {
      name = "job_id"
      type = "string"
    }
    columns {
      name = "title"
      type = "string"
    }
    columns {
      name = "company_name"
      type = "string"
    }
    columns {
      name = "location_clean"
      type = "string"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name = "published_at"
      type = "timestamp"
    }
    columns {
      name = "source_name"
      type = "string"
    }
    columns {
      name = "url"
      type = "string"
    }
    columns {
      name = "extracted_skills"
      type = "array<string>"
    }
    columns {
      name = "salary_min_numeric"
      type = "int"
    }
    columns {
      name = "is_junior"
      type = "boolean"
    }
    columns {
      name = "is_senior"
      type = "boolean"
    }
    columns {
      name = "is_red_flag"
      type = "boolean"
    }
    columns {
      name = "is_ethical"
      type = "boolean"
    }
    columns {
      name = "is_remote"
      type = "boolean"
    }
    columns {
      name = "exp_min_required"
      type = "double"
    }
    columns {
      name = "description_vector"
      type = "array<float>"
    }
  }

  # Partitionnement par date pour optimiser les performances de scan d'Athena
  partition_keys {
    name = "ingestion_date"
    type = "string"
  }
}

# ------ ATHENA CONFIGURATION ------

resource "aws_athena_workgroup" "jobradar_workgroup" {
  name        = "jobradar_workgroup"
  description = "Workgroup dédié à JobRadar avec limite de scan pour contrôle des coûts"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
    }

    # Limite de sécurité : 100MB par requête
    bytes_scanned_cutoff_per_query = 104857600 
  }

  force_destroy = true
}