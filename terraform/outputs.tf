output "raw_bucket_name" {
  description = "S3 bucket the CDC consumer and on-chain ingestion scripts should target (LANDING_ZONE_S3_BUCKET)."
  value       = aws_s3_bucket.raw.id
}

output "uc_access_role_arn" {
  description = "IAM role ARN Unity Catalog assumes to read/write the raw bucket."
  value       = aws_iam_role.uc_access.arn
}

output "external_location_url" {
  description = "s3:// URL registered as the Unity Catalog external location for spark/*.py to read."
  value       = databricks_external_location.raw.url
}

output "github_actions_role_arn" {
  description = "Set this as the AWS_ROLE_ARN repository variable in GitHub (Settings > Secrets and variables > Actions > Variables) for .github/workflows/onchain-ingestion.yml to assume via OIDC."
  value       = aws_iam_role.github_actions_onchain_ingestion.arn
}
