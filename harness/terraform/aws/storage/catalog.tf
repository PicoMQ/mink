resource "aws_s3tables_table_bucket" "external_catalog" {
  count = var.create_external_catalog ? 1 : 0

  name          = "${var.project}-external"
  force_destroy = var.force_destroy
}

resource "aws_s3tables_namespace" "kafka" {
  count = var.create_external_catalog ? 1 : 0

  namespace        = "kafka"
  table_bucket_arn = aws_s3tables_table_bucket.external_catalog[0].arn
}

data "aws_caller_identity" "this" {}

data "aws_iam_policy_document" "external_catalog" {
  for_each = try(local.external_catalog_actions[local.external_catalog_service], {})

  statement {
    actions   = each.value
    resources = local.external_catalog_resources
  }

  dynamic "statement" {
    for_each = local.external_catalog_service == "glue" ? ["lakeformation"] : []

    content {
      actions   = ["lakeformation:GetDataAccess"]
      resources = ["*"]
    }
  }
}
