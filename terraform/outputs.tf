# ============================================================================
# INFRASTRUCTURE OUTPUTS
# ============================================================================

output "api_gateway_url" {
  description = "Point d'entrée public de l'API JobRadar (FastAPI)"
  value       = aws_apigatewayv2_api.http_api.api_endpoint
}

output "s3_bucket_raw_id" {
  description = "Identifiant du bucket S3 contenant les données brutes (Bronze)"
  value       = aws_s3_bucket.raw.id
}

output "ecr_repository_url" {
  description = "URL du registre ECR pour le push de l'image Docker API"
  value       = aws_ecr_repository.jobradar_api.repository_url
}

output "lambda_ingestion_role_arn" {
  description = "ARN du rôle IAM utilisé par les Lambdas d'ingestion"
  value       = aws_iam_role.lambda_ingestion_role.arn
}