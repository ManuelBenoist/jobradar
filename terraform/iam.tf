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
