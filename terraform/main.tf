provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "raw_data" {
  bucket = "crypto-raw-data-${var.env}"
  force_destroy = true
}

resource "aws_s3_bucket" "cleaned_data" {
  bucket = "crypto-cleaned-data-${var.env}"
  force_destroy = true
}

resource "aws_iam_role" "glue_role" {
  name = "crypto-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "glue.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_glue_job" "crypto_clean_job" {
  name     = "crypto-news-clean-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw_data.bucket}/scripts/glue_clean.py"
    python_version  = "3"
  }

  glue_version = "4.0"
  max_capacity = 2.0
}

output "raw_bucket" {
  value = aws_s3_bucket.raw_data.bucket
}

output "cleaned_bucket" {
  value = aws_s3_bucket.cleaned_data.bucket
}
