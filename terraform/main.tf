# Terraform config for creating S3 buckets

resource "aws_s3_bucket" "raw" {
  bucket = "jobradar-raw-manuel-cloud" 
}

resource "aws_s3_bucket" "processed" {
  bucket = "jobradar-processed-manuel-cloud"
}

resource "aws_s3_bucket" "curated" {
  bucket = "jobradar-curated-manuel-cloud"
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "jobradar-athena-results-manuel-cloud"
  force_destroy = true
}

resource "aws_ecr_repository" "jobradar_api" {
  name                 = "jobradar-api"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true # AWS vérifie s'il y a des failles de sécurité dans le code
  }
}

# Règle de rétention de 7 jours pour ne pas payer pour rien (FinOps)
resource "aws_s3_bucket_lifecycle_configuration" "athena_results_lifecycle" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
  }
}
