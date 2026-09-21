variable "aws_region" {
  description = "AWS region the raw/landing bucket and IAM role live in."
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Short name used as a prefix for every resource this stack creates."
  type        = string
  default     = "wallet-lakehouse"
}

variable "environment" {
  description = "Deployment environment tag (dev/staging/prod). Kept to one env (dev) for this portfolio project."
  type        = string
  default     = "dev"
}

variable "aws_account_id" {
  description = "AWS account ID that owns the raw landing zone bucket and IAM role (used to scope the Unity Catalog trust policy)."
  type        = string
}

variable "enable_unity_catalog_automation" {
  description = <<-EOT
    Gates the Unity Catalog resources in main.tf (the cross-account IAM
    role's Databricks-generated policies, the storage credential, and the
    external location). These are account-level Databricks API resources —
    Databricks Free Edition explicitly has "no access to the account console
    or account-level APIs" (confirmed against Databricks' own docs), so they
    CANNOT be applied against a Free Edition account, regardless of what
    credentials you set. Leave this false on Free Edition; the S3 bucket and
    GitHub OIDC/IAM resources apply fine either way, since they don't touch
    the databricks provider at all. Flip to true only once you're on a
    Databricks tier with Account Console access (Premium+), with
    DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET set for an account-level
    OAuth service principal.
  EOT
  type        = bool
  default     = false
}

variable "databricks_account_id" {
  description = "Databricks account ID (UUID, found under Account Console > Settings). Only needed when enable_unity_catalog_automation=true."
  type        = string
  default     = ""
}

variable "databricks_metastore_id" {
  description = "ID of the existing Unity Catalog metastore in this Databricks account. Only needed when enable_unity_catalog_automation=true."
  type        = string
  default     = ""
}

variable "github_repo" {
  description = "GitHub \"owner/repo\" this OIDC role trusts — e.g. \"Arshavin023/wallet-activity-lakehouse\". Scopes AssumeRoleWithWebIdentity to exactly this repo, no wildcard."
  type        = string
}

variable "github_default_branch" {
  description = "Branch the OIDC trust policy's `sub` claim is scoped to. Scheduled and manually-dispatched workflow runs both authenticate as this branch's ref, regardless of which branch is checked out inside the job."
  type        = string
  default     = "main"
}

variable "tags" {
  description = "Common tags applied to every AWS resource."
  type        = map(string)
  default = {
    Project   = "wallet-activity-lakehouse"
    ManagedBy = "terraform"
  }
}
