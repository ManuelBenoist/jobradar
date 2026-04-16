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
