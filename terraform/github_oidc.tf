# =============================================================================
# GitHub Actions OIDC — lets the scheduled on-chain ingestion workflow
# (.github/workflows/onchain-ingestion.yml) authenticate to AWS and write
# to S3 WITHOUT a long-lived access key sitting in a repo secret. This is
# the production-grade pattern: GitHub issues a short-lived signed token
# per workflow run, AWS trusts it via this OIDC provider, and the role
# below is scoped to exactly one S3 prefix — not the whole bucket, and
# nothing else in the account.
#
# thumbprint_list is GitHub's OIDC root CA thumbprint, taken directly from
# AWS's own security blog (not derived or guessed) — verified against
# https://aws.amazon.com/blogs/security/use-iam-roles-to-connect-github-actions-to-actions-in-aws
# It's the intermediate/root CA thumbprint (stable since 2023), not tied to
# GitHub's leaf certificate, so it shouldn't need updating when GitHub
# rotates its TLS cert — but AWS's own docs note it may still need a
# manual bump if GitHub ever changes CAs; there's no auto-managed option
# on this resource.
# =============================================================================

resource "aws_iam_openid_connect_provider" "github_actions" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

# Scoped with StringLike on the `sub` claim to ONE repo and ONE ref
# (the default branch — both `schedule` and manually-dispatched runs use
# refs/heads/<default branch>). This is deliberately narrow: a wildcard
# repo or `*` ref would let any fork or branch assume this role.
resource "aws_iam_role" "github_actions_onchain_ingestion" {
  name = "${var.project_name}-gha-onchain-ingestion-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Federated = aws_iam_openid_connect_provider.github_actions.arn }
        Action    = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:ref:refs/heads/${var.github_default_branch}"
          }
        }
      }
    ]
  })

  tags = var.tags
}

# Least privilege: PutObject only, only under raw/onchain/ — this workflow
# has no business touching raw/cdc/ (that's Debezium's prefix) or anything
# else in the bucket, and it never needs to read or delete.
resource "aws_iam_policy" "github_actions_onchain_ingestion" {
  name = "${var.project_name}-gha-onchain-ingestion-policy-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.raw.arn}/raw/onchain/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_actions_onchain_ingestion" {
  role       = aws_iam_role.github_actions_onchain_ingestion.name
  policy_arn = aws_iam_policy.github_actions_onchain_ingestion.arn
}
