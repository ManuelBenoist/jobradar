# 1. La bdd
resource "aws_glue_catalog_database" "jobradar_db" {
  name = "jobradar_db"
}

# 2. La Table Silver (Anciennement processed_jobs)
# On garde le nom de ressource Terraform "processed_jobs" pour éviter de tout recréer,
# mais on change le nom réel dans AWS pour "silver_jobs".
resource "aws_glue_catalog_table" "processed_jobs" {
  name          = "silver_jobs" 
  database_name = aws_glue_catalog_database.jobradar_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    # CRITIQUE : On pointe bien sur le sous-dossier défini dans ton script Spark
    location      = "s3://jobradar-processed-manuel-cloud/silver_jobs/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-ser-de"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # --- Mapping exact de sortie Spark ---
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
      type = "array<string>" # Type complexe pour Athena
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
      type = "float"
    }
    columns {
      name = "description_vector"
      type = "array<float>"
    }
  }

  partition_keys {
    name = "ingestion_date"
    type = "string"
  }
}

resource "aws_athena_workgroup" "jobradar_workgroup" {
  name = "jobradar_workgroup"

  configuration {
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
    }

    bytes_scanned_cutoff_per_query = 104857600 
  }

  force_destroy = true
}