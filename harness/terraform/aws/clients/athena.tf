resource "aws_ecr_repository" "athena" {
  count = var.create_athena ? 1 : 0

  name                 = "${var.project}-athena"
  image_tag_mutability = "MUTABLE"
  force_delete         = var.force_destroy

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "${var.project}-athena"
  }
}

data "aws_iam_policy_document" "athena_assume" {
  count = var.create_athena ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "athena" {
  count = var.create_athena ? 1 : 0

  name               = "${var.project}-athena"
  assume_role_policy = data.aws_iam_policy_document.athena_assume[0].json

  tags = {
    Name = "${var.project}-athena"
  }
}

resource "aws_iam_role_policy_attachment" "athena_vpc" {
  count = var.create_athena ? 1 : 0

  role       = aws_iam_role.athena[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "athena" {
  count = var.create_athena ? 1 : 0

  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["arn:aws:s3:::${var.bucket}/athena/*"]
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.bucket}"]
  }

  statement {
    actions   = ["athena:GetQueryExecution"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "athena" {
  count = var.create_athena ? 1 : 0

  name   = "athena"
  role   = aws_iam_role.athena[0].id
  policy = data.aws_iam_policy_document.athena[0].json
}

resource "aws_lambda_function" "athena" {
  count = var.create_athena ? 1 : 0

  function_name = "${var.project}-athena"
  role          = aws_iam_role.athena[0].arn

  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.athena[0].repository_url}:latest"
  architectures = [lower(var.cpu_architecture) == "arm64" ? "arm64" : "x86_64"]

  memory_size = 3008
  timeout     = 900

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [var.athena_security_group]
  }

  environment {
    variables = {
      catalog_uri  = var.catalog_endpoint
      spill_bucket = var.bucket
      spill_prefix = "athena/spill"
    }
  }

  depends_on = [aws_iam_role_policy_attachment.athena_vpc, aws_iam_role_policy.athena]

  tags = {
    Name = "${var.project}-athena"
  }
}

resource "aws_athena_data_catalog" "this" {
  count = var.create_athena ? 1 : 0

  name        = var.project
  description = "Mink tables through the Athena connector"
  type        = "LAMBDA"

  parameters = {
    function = aws_lambda_function.athena[0].arn
  }
}

resource "aws_athena_workgroup" "this" {
  count = var.create_athena ? 1 : 0

  name          = var.project
  force_destroy = var.force_destroy

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket}/athena/results/"
    }
  }

  tags = {
    Name = var.project
  }
}
