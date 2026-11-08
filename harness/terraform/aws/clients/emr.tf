resource "aws_emrserverless_application" "this" {
  count = var.emr ? 1 : 0

  name          = var.project
  release_label = "emr-7.14.0"
  type          = "spark"
  architecture  = var.cpu_architecture

  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  network_configuration {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [var.emr_security_group]
  }

  tags = {
    Name = var.project
  }
}

data "aws_iam_policy_document" "emr_assume" {
  count = var.emr ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr" {
  count = var.emr ? 1 : 0

  name               = "${var.project}-emr"
  assume_role_policy = data.aws_iam_policy_document.emr_assume[0].json

  tags = {
    Name = "${var.project}-emr"
  }
}

data "aws_iam_policy_document" "emr_s3" {
  count = var.emr ? 1 : 0

  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["arn:aws:s3:::${var.bucket}/emr/*"]
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["emr/*"]
    }
  }
}

resource "aws_iam_role_policy" "emr_s3" {
  count = var.emr ? 1 : 0

  name   = "s3"
  role   = aws_iam_role.emr[0].id
  policy = data.aws_iam_policy_document.emr_s3[0].json
}

resource "aws_iam_role_policy" "emr_tables" {
  count = var.emr && var.create_s3_tables ? 1 : 0

  name   = "tables"
  role   = aws_iam_role.emr[0].id
  policy = var.external_catalog_read_policy
}
