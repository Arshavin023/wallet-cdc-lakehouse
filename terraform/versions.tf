terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Configured against a Databricks *account*, not a workspace, because Free
# Edition provisioning (UC metastore, external locations) happens at the
# account level. See https://registry.terraform.io/providers/databricks/databricks/latest/docs/guides/unity-catalog
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}
