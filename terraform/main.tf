resource "aws_s3_bucket" "raw" {
  bucket = "jobradar-raw-manuel-cloud" 
}

resource "aws_s3_bucket" "processed" {
  bucket = "jobradar-processed-manuel-cloud"
}

resource "aws_s3_bucket" "curated" {
  bucket = "jobradar-curated-manuel-cloud"
}
