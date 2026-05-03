# --- BUCKETS S3 (Architecture Médaillon) ---
resource "aws_s3_bucket" "raw"      { bucket = "jobradar-raw-manuel-cloud" }
resource "aws_s3_bucket" "processed" { bucket = "jobradar-processed-manuel-cloud" }
resource "aws_s3_bucket" "curated"   { bucket = "jobradar-curated-manuel-cloud" }

resource "aws_s3_bucket" "athena_results" {
  bucket        = "jobradar-athena-results-manuel-cloud"
  force_destroy = true # Autorise la suppression propre en phase de test
}

# Politique de rétention des résultats Athena (FinOps)
resource "aws_s3_bucket_lifecycle_configuration" "athena_results_lifecycle" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-old-results"
    status = "Enabled"
    filter {}
    expiration { days = 7 }
  }
}

# --- REGISTRE DE CONTENEURS (ECR) ---
resource "aws_ecr_repository" "jobradar_api" {
  name                 = "jobradar-api"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
}