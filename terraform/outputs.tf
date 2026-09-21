output "raw_bucket_name" {
  description = "S3 bucket the CDC consumer and on-chain ingestion scripts should target (LANDING_ZONE_S3_BUCKET)."
  value       = aws_s3_bucket.raw.id
}

output "uc_access_role_arn" {
  description = "IAM role ARN Unity Catalog assumes to read/write the raw bucket. Null unless enable_unity_catalog_automation=true (see that variable's description — this doesn't exist on Databricks Free Edition)."
  value       = try(aws_iam_role.uc_access[0].arn, null)
}

output "external_location_url" {
  description = "s3:// URL registered as the Unity Catalog external location for spark/*.py to read. Null unless enable_unity_catalog_automation=true (see that variable's description)."
  value       = try(databricks_external_location.raw[0].url, null)
}

output "github_actions_role_arn" {
  description = "Set this as the AWS_ROLE_ARN repository variable in GitHub (Settings > Secrets and variables > Actions > Variables) for .github/workflows/onchain-ingestion.yml to assume via OIDC."
  value       = aws_iam_role.github_actions_onchain_ingestion.arn
}
