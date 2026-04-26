# 1. LE RÔLE D'EXÉCUTION
resource "aws_iam_role" "lambda_exec_role" {
  name = "jobradar-lambda-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# 2. LES PERMISSIONS
resource "aws_iam_role_policy_attachment" "lambda_athena_access" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_s3_access" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Crucial pour voir les erreurs de l'API dans CloudWatch
resource "aws_iam_role_policy_attachment" "lambda_logging" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# 3. LA FONCTION LAMBDA
resource "aws_lambda_function" "jobradar_api" {
  function_name = "jobradar-api-serverless-v2"
  role          = aws_iam_role.lambda_exec_role.arn
  
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.jobradar_api.repository_url}:latest"

  timeout     = 30  # On laisse 30s à Athena pour répondre
  memory_size = 512 # 512 Mo c'est largement assez pour FastAPI

  environment {
    variables = {
      ATHENA_DATABASE       = "jobradar_db"
      ATHENA_S3_STAGING_DIR = var.athena_staging_dir
      INTERNAL_API_KEY      = var.internal_api_key
    }
  }
}

# 4. API GATEWAY
resource "aws_apigatewayv2_api" "http_api" {
  name          = "jobradar-http-api"
  protocol_type = "HTTP"
  target        = aws_lambda_function.jobradar_api.arn

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["*"]
  }
}

# Autoriser API Gateway à exécuter la Lambda
resource "aws_lambda_permission" "apigw_lambda" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.jobradar_api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api.execution_arn}/*/*"
}

# 5. L'OUTPUT
output "api_gateway_url" {
  description = "URL publique de l'API Gateway"
  value       = aws_apigatewayv2_api.http_api.api_endpoint
}