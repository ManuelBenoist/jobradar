# 1. Terraform zipper pour Adzuna 
data "archive_file" "adzuna_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda/adzuna" 
  output_path = "${path.module}/adzuna_lambda.zip"
}

# 2. Création de la Lambda Adzuna
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

# 4. Terraform zipper pour France Travail
data "archive_file" "france_travail_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda/france_travail" 
  output_path = "${path.module}/france_travail_lambda.zip"
}

# 2. La Lambda France Travail
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
