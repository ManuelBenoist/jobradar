# Terraform zipper pour Adzuna 
data "archive_file" "adzuna_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda/adzuna" 
  output_path = "${path.module}/adzuna_lambda.zip"
}

# Création de la Lambda Adzuna
resource "aws_lambda_function" "ingest_adzuna" {
  function_name    = "jobradar-ingest-adzuna"
  filename         = data.archive_file.adzuna_zip.output_path
  source_code_hash = data.archive_file.adzuna_zip.output_base64sha256
  
  role             = aws_iam_role.lambda_ingestion_role.arn 
  handler          = "ingest_adzuna.lambda_handler"         
  runtime          = "python3.11"                           
  timeout          = 60                                     

  # 3. Les clés API sécurisées Adzuna dans les variables d'environnement
  environment {
    variables = {
      BUCKET_NAME    = aws_s3_bucket.raw.id
      ADZUNA_APP_ID  = var.adzuna_app_id
      ADZUNA_APP_KEY = var.adzuna_app_key
    }
  }
}

# Terraform zipper pour France Travail
data "archive_file" "france_travail_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda/france_travail" 
  output_path = "${path.module}/france_travail_lambda.zip"
}

# La Lambda France Travail
resource "aws_lambda_function" "ingest_france_travail" {
  function_name    = "jobradar-ingest-france-travail"
  filename         = data.archive_file.france_travail_zip.output_path
  source_code_hash = data.archive_file.france_travail_zip.output_base64sha256
  
  role             = aws_iam_role.lambda_ingestion_role.arn # On réutilise le même badge !
  handler          = "ingest_france_travail.lambda_handler"         
  runtime          = "python3.11"                           
  timeout          = 60                                     

  environment {
    variables = {
      BUCKET_NAME      = aws_s3_bucket.raw.id
      FT_CLIENT_ID     = var.ft_client_id
      FT_CLIENT_SECRET = var.ft_client_secret
    }
  }
}

# Terraform zipper pour JSEARCH
data "archive_file" "jsearch_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda/jsearch" 
  output_path = "${path.module}/jsearch_lambda.zip"
}

# La Lambda JSEARCH
resource "aws_lambda_function" "ingest_jsearch" {
  function_name    = "jobradar-ingest-jsearch"
  filename         = data.archive_file.jsearch_zip.output_path
  source_code_hash = data.archive_file.jsearch_zip.output_base64sha256
  role             = aws_iam_role.lambda_ingestion_role.arn
  handler          = "ingest_jsearch.lambda_handler"
  runtime          = "python3.11"
  timeout          = 60 # JSearch a besoin de temps

  environment {
    variables = {
      BUCKET_NAME     = aws_s3_bucket.raw.id
      JSEARCH_API_KEY = var.jsearch_api_key
    }
  }
}

# Terraform zipper pour Jooble
data "archive_file" "jooble_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda/jooble" 
  output_path = "${path.module}/jooble_lambda.zip"
}

# La Lambda Jooble
resource "aws_lambda_function" "ingest_jooble" {
  function_name    = "jobradar-ingest-jooble"
  filename         = data.archive_file.jooble_zip.output_path
  source_code_hash = data.archive_file.jooble_zip.output_base64sha256
  role             = aws_iam_role.lambda_ingestion_role.arn
  handler          = "ingest_jooble.lambda_handler"
  runtime          = "python3.11"
  timeout          = 60

  environment {
    variables = {
      BUCKET_NAME      = aws_s3_bucket.raw.id
      JOOBLE_API_KEY   = var.jooble_api_key
    }
  }
}
