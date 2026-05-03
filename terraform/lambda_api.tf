# ============================================================================
# API LAYER : DOCKER LAMBDA & API GATEWAY
# ============================================================================

# --- RÔLE D'EXÉCUTION (IDENTITY) ---
resource "aws_iam_role" "lambda_exec_role" {
  name = "jobradar-lambda-api-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# --- PERMISSIONS (Least Privilege mindset) ---
# Note : AmazonAthenaFullAccess et S3FullAccess sont utilisés pour garantir le bon 
# fonctionnement des requêtes vers la couche Gold.
resource "aws_iam_role_policy_attachment" "lambda_athena_access" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_s3_access" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_logging" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# --- FONCTION LAMBDA (CONTAINER BASED) ---
resource "aws_lambda_function" "jobradar_api" {
  function_name = "jobradar-api-serverless-v2"
  role          = aws_iam_role.lambda_exec_role.arn
  
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.jobradar_api.repository_url}:latest"

  timeout     = 30  # Temps nécessaire pour les requêtes Athena complexes
  memory_size = 512

  environment {
    variables = {
      ATHENA_DATABASE       = "jobradar_db"
      ATHENA_S3_STAGING_DIR = var.athena_staging_dir
      INTERNAL_API_KEY      = var.internal_api_key
      AWS_REGION_NAME       = var.aws_region
    }
  }
}

# --- API GATEWAY (HTTP API) ---
resource "aws_apigatewayv2_api" "http_api" {
  name          = "jobradar-http-api"
  protocol_type = "HTTP"
  target        = aws_lambda_function.jobradar_api.arn

  cors_configuration {
    # Harmonisé avec les réglages de main.py pour la sécurité CORS
    allow_origins = ["https://jobradar-nantes.streamlit.app/", "http://localhost:8501"]
    allow_methods = ["GET", "OPTIONS"]
    allow_headers = ["*"]
  }
}

resource "aws_lambda_permission" "apigw_lambda" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.jobradar_api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api.execution_arn}/*/*"
}