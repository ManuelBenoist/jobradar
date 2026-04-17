# On crée une "Policy" : le document qui liste les droits
resource "aws_iam_policy" "lambda_ingestion_policy" {
  name        = "jobradar-lambda-ingestion-policy"
  description = "Autorise la Lambda à écrire dans le bucket RAW"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:PutObject", "s3:ListBucket"]
        Effect   = "Allow"
        Resource = [
          "${aws_s3_bucket.raw.arn}",
          "${aws_s3_bucket.raw.arn}/*"
        ]
      }
    ]
  })
}

# 1. Le "Rôle" : L'identité de la Lambda
resource "aws_iam_role" "lambda_ingestion_role" {
  name = "jobradar-lambda-ingestion-role"

  # On dit à AWS que ce rôle est réservé à une Lambda
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# 2. On attache le badge (Policy) au rôle
resource "aws_iam_role_policy_attachment" "lambda_s3_attach" {
  role       = aws_iam_role.lambda_ingestion_role.name
  policy_arn = aws_iam_policy.lambda_ingestion_policy.arn
}

# 3. On ajoute les droits de base pour que la Lambda puisse écrire des Logs (pour débugger)
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_ingestion_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# "Policy" pour Athena 
resource "aws_iam_policy" "athena_query_policy" {
  name        = "jobradar-athena-query-policy"
  description = "Autorise la lecture des données transformées et l'écriture des résultats Athena"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Droits sur les buckets S3
        Effect   = "Allow"
        Action   = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject" # Nécessaire pour écrire les fichiers de résultats (.csv)
        ]
        Resource = [
          "${aws_s3_bucket.processed.arn}",
          "${aws_s3_bucket.processed.arn}/*",
          "${aws_s3_bucket.athena_results.arn}",
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        # Droits sur le catalogue Glue (pour comprendre la structure des tables)
        Effect   = "Allow"
        Action   = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition" # Pour que le MSCK REPAIR puisse ajouter des partitions
        ]
        Resource = ["*"] # On peut restreindre à l'ARN de la DB si on veut être ultra-précis
      }
    ]
  })
}
