output "s3_bucket_raw_id" {
  value = aws_s3_bucket.raw.id
  description = "Nom du bucket pour l'ingestion de données"
}

output "iam_policy_ingestion_arn" {
  value = aws_iam_policy.lambda_ingestion_policy.arn
  description = "ARN de la politique à attacher à la future Lambda"
}
