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
