resource "aws_s3tables_table_bucket" "external" {
  count = var.create_s3_tables ? 1 : 0

  name          = "${var.project}-external"
  force_destroy = var.force_destroy
}

resource "aws_s3tables_namespace" "kafka" {
  count = var.create_s3_tables ? 1 : 0

  namespace        = "kafka"
  table_bucket_arn = aws_s3tables_table_bucket.external[0].arn
}

data "aws_iam_policy_document" "tables_write" {
  count = var.create_s3_tables ? 1 : 0

  statement {
    actions = [
      "s3tables:GetTableBucket",
      "s3tables:GetNamespace",
      "s3tables:ListNamespaces",
      "s3tables:ListTables",
      "s3tables:CreateTable",
      "s3tables:DeleteTable",
      "s3tables:GetTable",
      "s3tables:GetTableMetadataLocation",
      "s3tables:UpdateTableMetadataLocation",
      "s3tables:GetTableData",
      "s3tables:PutTableData",
    ]
    resources = [
      aws_s3tables_table_bucket.external[0].arn,
      "${aws_s3tables_table_bucket.external[0].arn}/table/*",
    ]
  }
}

data "aws_iam_policy_document" "tables_read" {
  count = var.create_s3_tables ? 1 : 0

  statement {
    actions = [
      "s3tables:GetTableBucket",
      "s3tables:GetNamespace",
      "s3tables:ListNamespaces",
      "s3tables:ListTables",
      "s3tables:GetTable",
      "s3tables:GetTableMetadataLocation",
      "s3tables:GetTableData",
    ]
    resources = [
      aws_s3tables_table_bucket.external[0].arn,
      "${aws_s3tables_table_bucket.external[0].arn}/table/*",
    ]
  }
}
