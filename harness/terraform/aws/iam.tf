data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.project}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json

  tags = {
    Name = "${var.project}-execution"
  }
}

resource "aws_iam_role_policy_attachment" "execution_ecs" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = concat(
      [aws_secretsmanager_secret.postgres.arn],
      aws_secretsmanager_secret.kafka[*].arn,
      aws_secretsmanager_secret.registry[*].arn,
      aws_secretsmanager_secret.registry_kafka[*].arn,
    )
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

resource "aws_iam_role" "task" {
  name               = "${var.project}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json

  tags = {
    Name = "${var.project}-task"
  }
}

data "aws_iam_policy_document" "task_s3" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      "arn:aws:s3:::${local.bucket}",
      "arn:aws:s3:::${local.bucket}/*",
    ]
  }
}

resource "aws_iam_role_policy" "task_s3" {
  name   = "s3"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_s3.json
}

data "aws_iam_policy_document" "task_exec" {
  statement {
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "task_exec" {
  name   = "exec"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_exec.json
}

data "aws_caller_identity" "this" {}

data "aws_iam_policy_document" "task_glue" {
  count = local.use_glue_registry ? 1 : 0

  statement {
    actions = ["glue:GetRegistry", "glue:GetSchemaVersion"]
    resources = [
      "arn:aws:glue:${local.glue_registry_region}:${data.aws_caller_identity.this.account_id}:registry/${local.glue_registry_name}",
      "arn:aws:glue:${local.glue_registry_region}:${data.aws_caller_identity.this.account_id}:schema/${local.glue_registry_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "task_glue" {
  count = local.use_glue_registry ? 1 : 0

  name   = "glue"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_glue[0].json
}
