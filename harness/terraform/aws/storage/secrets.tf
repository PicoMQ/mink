resource "random_password" "postgres" {
  count = var.create_postgres ? 1 : 0

  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "postgres" {
  name_prefix             = "${var.project}-postgres-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-postgres"
  }
}

resource "aws_secretsmanager_secret_version" "postgres" {
  secret_id     = aws_secretsmanager_secret.postgres.id
  secret_string = local.postgres_url
}

data "aws_iam_policy_document" "postgres" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret_version.postgres.secret_arn]
  }
}
