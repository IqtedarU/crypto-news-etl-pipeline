# outputs.tf
output "raw_bucket" {
  value = aws_s3_bucket.raw_data.bucket
}

output "cleaned_bucket" {
  value = aws_s3_bucket.cleaned_data.bucket
}
