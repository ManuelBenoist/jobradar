# ============================================================================
# SECURITY : IAM ROLES & POLICIES
# ============================================================================

# --- RÔLE D'INGESTION (LAMBDAS) ---

resource "aws_iam_role" "lambda_ingestion_role" {
  name = "jobradar-lambda-ingestion-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "lambda_ingestion_policy" {
  name        = "jobradar-lambda-ingestion-policy"
  description = "Droits d'écriture dans le Bucket RAW pour l'ingestion"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = ["s3:PutObject", "s3:ListBucket"]
      Effect   = "Allow"
      Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_ingestion_role.name
  policy_arn = aws_iam_policy.lambda_ingestion_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_ingestion_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# --- POLICY ANALYTICS (ATHENA & GLUE) ---

resource "aws_iam_policy" "athena_query_policy" {
  name        = "jobradar-athena-query-policy"
  description = "Autorise l'exécution de requêtes Athena et la gestion du catalogue Glue"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Accès aux données transformées et stockage des résultats
        Effect = "Allow"
        Action = ["s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:PutObject"]
        Resource = [
          aws_s3_bucket.processed.arn, "${aws_s3_bucket.processed.arn}/*",
          aws_s3_bucket.athena_results.arn, "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        # Interaction avec le Data Catalog (requis pour MSCK REPAIR et lecture des schémas)
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetTable",
          "glue:GetPartitions", "glue:BatchCreatePartition"
        ]
        Resource = ["*"]
      }
    ]
  })
}