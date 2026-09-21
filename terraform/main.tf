# =============================================================================
# Wallet Activity Lakehouse — AWS + Unity Catalog infrastructure
#
# Provisions:
#   1. An S3 bucket for the raw landing zone (CDC events + on-chain pulls),
#      versioned, encrypted, with public access fully blocked.
#   2. A cross-account IAM role Databricks can assume to read/write that
#      bucket, using Databricks' own policy-generating data sources rather
#      than a hand-written trust policy (the two `databricks_aws_unity_*`
#      data sources below produce the exact JSON Databricks' own docs
#      specify, so this doesn't drift from what UC actually expects).
#   3. A Unity Catalog storage credential + external location wiring that
#      role to a `wallet-lakehouse-raw` path, so the Spark jobs in
#      spark/*.py can read/write via `s3://.../raw/...` under UC governance,
#      matching how serverless compute requires external data access to be
#      configured (see README: "Databricks Free Edition — what actually
#      works here").
#
# This has been written against the Databricks Terraform provider's own
# documented examples (verified against the provider's GitHub docs) but has
# NOT been run through `terraform plan`/`apply` here — this sandbox has no
# network path to releases.hashicorp.com to install the terraform CLI. Its
# HCL syntax has been checked with python-hcl2. Treat this as a reviewed,
# ready-to-run starting point, not a "confirmed working in production" claim.
# =============================================================================

locals {
  bucket_name = "${var.project_name}-raw-${var.environment}"
  role_name   = "${var.project_name}-uc-access-${var.environment}"
}

# -----------------------------------------------------------------------------
# 1. Raw landing zone bucket
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "raw" {
  bucket = local.bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Raw CDC/on-chain events are cheap to regenerate from source (Postgres /
# Etherscan) — age them out of standard storage instead of keeping them
# indefinitely at full cost. Bronze Delta tables are the durable copy.
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-raw-landing-events"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = 90
    }
  }
}

# -----------------------------------------------------------------------------
# 2. Cross-account IAM role for Unity Catalog data access
#    Uses Databricks' own policy-generator data sources so the trust policy
#    and access policy match what UC actually validates against, rather
#    than a hand-maintained copy that can drift.
# -----------------------------------------------------------------------------
data "databricks_aws_unity_catalog_assume_role_policy" "this" {
  provider       = databricks.account
  aws_account_id = var.aws_account_id
  role_name      = local.role_name
  external_id    = var.databricks_metastore_id
}

data "databricks_aws_unity_catalog_policy" "this" {
  provider       = databricks.account
  aws_account_id = var.aws_account_id
  bucket_name    = aws_s3_bucket.raw.id
  role_name      = local.role_name
}

resource "aws_iam_role" "uc_access" {
  name               = local.role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.this.json
  tags               = var.tags
}

resource "aws_iam_policy" "uc_access" {
  name   = "${local.role_name}-policy"
  policy = data.databricks_aws_unity_catalog_policy.this.json
}

resource "aws_iam_role_policy_attachment" "uc_access" {
  role       = aws_iam_role.uc_access.name
  policy_arn = aws_iam_policy.uc_access.arn
}

# -----------------------------------------------------------------------------
# 3. Unity Catalog storage credential + external location
# -----------------------------------------------------------------------------
resource "databricks_storage_credential" "raw" {
  provider     = databricks.account
  name         = "${var.project_name}-raw-credential"
  metastore_id = var.databricks_metastore_id
  aws_iam_role {
    role_arn = aws_iam_role.uc_access.arn
  }
  comment = "Managed by Terraform — wallet-activity-lakehouse raw landing zone"
}

resource "databricks_external_location" "raw" {
  provider        = databricks.account
  name            = "${var.project_name}-raw"
  url             = "s3://${aws_s3_bucket.raw.id}/raw"
  credential_name = databricks_storage_credential.raw.id
  comment         = "Managed by Terraform — CDC + on-chain landing zone read by spark/*.py"

  depends_on = [aws_iam_role_policy_attachment.uc_access]
}
