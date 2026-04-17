# 1. La bdd
resource "aws_glue_catalog_database" "jobradar_db" {
  name = "jobradar_db"
}

# 2. La Table Processed (format Parquet)
resource "aws_glue_catalog_table" "processed_jobs" {
  name          = "processed_jobs"
  database_name = aws_glue_catalog_database.jobradar_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://jobradar-processed-manuel-cloud/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "my-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Définition des colonnes
    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "title"
      type = "string"
    }
    columns {
      name = "company"
      type = "string"
    }
    columns {
      name = "description"
      type = "string"
    }
  }

  # PARTITIONNEMENT : C'est ici qu'on optimise les coûts
  # On dit à Athena que les données sont rangées dans des dossiers par date
  partition_keys {
    name = "ingestion_date"
    type = "string"
  }
}

# Sécurisation FinOps : on limite le nombre de requêtes simultanées et la quantité de données scannées
resource "aws_athena_workgroup" "jobradar_workgroup" {
  name = "jobradar_workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
    }

    # Limite de scan par requête : 100 Mo (0.1 Go)
    # Si une requête tente de scanner plus, elle est coupée net.
    bytes_scanned_cutoff_per_query = 104857600 
  }

  force_destroy = true
}
