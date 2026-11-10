resource "random_password" "kafka" {
  count = var.create_kafka ? 1 : 0

  length  = 32
  special = false
}

resource "aws_kms_key" "scram" {
  count = var.create_kafka ? 1 : 0

  description             = "${var.project} MSK SCRAM credentials"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name = "${var.project}-scram"
  }
}

resource "aws_secretsmanager_secret" "scram" {
  count = var.create_kafka ? 1 : 0

  name_prefix             = "AmazonMSK_${var.project}-"
  kms_key_id              = aws_kms_key.scram[0].key_id
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-scram"
  }
}

resource "aws_secretsmanager_secret_version" "scram" {
  count = var.create_kafka ? 1 : 0

  secret_id = aws_secretsmanager_secret.scram[0].id
  secret_string = jsonencode({
    username = "mink"
    password = random_password.kafka[0].result
  })
}

resource "aws_secretsmanager_secret" "kafka" {
  count = !var.create_kafka && local.use_kafka_sasl ? 1 : 0

  name_prefix             = "${var.project}-kafka-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-kafka"
  }
}

resource "aws_secretsmanager_secret_version" "kafka" {
  count = !var.create_kafka && local.use_kafka_sasl ? 1 : 0

  secret_id = aws_secretsmanager_secret.kafka[0].id
  secret_string = jsonencode({
    username = local.kafka.sasl.username
    password = local.kafka.sasl.password
  })
}

resource "aws_secretsmanager_secret" "registry_password" {
  count = local.use_registry_password ? 1 : 0

  name_prefix             = "${var.project}-registry-password-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-registry-password"
  }
}

resource "aws_secretsmanager_secret_version" "registry_password" {
  count = local.use_registry_password ? 1 : 0

  secret_id     = aws_secretsmanager_secret.registry_password[0].id
  secret_string = local.registry.password
}

resource "aws_secretsmanager_secret" "registry_kafka" {
  count = var.create_confluent_registry && local.use_kafka_sasl ? 1 : 0

  name_prefix             = "${var.project}-registry-kafka-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-registry-kafka"
  }
}

resource "aws_secretsmanager_secret_version" "registry_kafka" {
  count = var.create_confluent_registry && local.use_kafka_sasl ? 1 : 0

  secret_id = aws_secretsmanager_secret.registry_kafka[0].id
  secret_string = join(" ", [
    local.kafka_login_modules[local.kafka.sasl.mechanism],
    "required",
    "username=${jsonencode(local.kafka.sasl.username)}",
    "password=${jsonencode(local.kafka.sasl.password)};",
  ])
}

data "aws_iam_policy_document" "kafka" {
  count = local.use_kafka_sasl ? 1 : 0

  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [local.kafka_secret_arn]
  }

  dynamic "statement" {
    for_each = aws_kms_key.scram

    content {
      actions   = ["kms:Decrypt"]
      resources = [statement.value.arn]
    }
  }
}

data "aws_iam_policy_document" "registry" {
  count = length(local.registry_secret_arns) > 0 ? 1 : 0

  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = local.registry_secret_arns
  }
}
