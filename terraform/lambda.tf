# 1. Terraform zipper 
data "archive_file" "adzuna_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda/adzuna" 
  output_path = "${path.module}/adzuna_lambda.zip"
}

# 2. Création de la Lambda
resource "aws_lambda_function" "ingest_adzuna" {
  function_name    = "jobradar-ingest-adzuna"
  filename         = data.archive_file.adzuna_zip.output_path
  source_code_hash = data.archive_file.adzuna_zip.output_base64sha256
  
  role             = aws_iam_role.lambda_ingestion_role.arn 
  handler          = "ingest_adzuna.lambda_handler"         
  runtime          = "python3.11"                           
  timeout          = 60                                     

  # 3. Les clés API sécurisées
  environment {
    variables = {
      BUCKET_NAME    = aws_s3_bucket.raw.id
      ADZUNA_APP_ID  = var.adzuna_app_id
      ADZUNA_APP_KEY = var.adzuna_app_key
    }
  }
}
