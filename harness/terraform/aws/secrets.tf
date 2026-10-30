resource "random_password" "postgres" {
  count = local.create_postgres ? 1 : 0

  length  = 32
  special = false
}

resource "random_password" "kafka" {
  count = local.create_kafka ? 1 : 0

  length  = 32
  special = false
}

resource "aws_kms_key" "scram" {
  count = local.create_kafka ? 1 : 0

  description             = "${var.project} MSK SCRAM credentials"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name = "${var.project}-scram"
  }
}

resource "aws_secretsmanager_secret" "scram" {
  count = local.create_kafka ? 1 : 0

  name_prefix             = "AmazonMSK_${var.project}-"
  kms_key_id              = aws_kms_key.scram[0].key_id
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-scram"
  }
}

resource "aws_secretsmanager_secret_version" "scram" {
  count = local.create_kafka ? 1 : 0

  secret_id = aws_secretsmanager_secret.scram[0].id
  secret_string = jsonencode({
    username = "mink"
    password = random_password.kafka[0].result
  })
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

resource "aws_secretsmanager_secret" "kafka" {
  count = local.kafka_sasl ? 1 : 0

  name_prefix             = "${var.project}-kafka-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-kafka"
  }
}

resource "aws_secretsmanager_secret_version" "kafka" {
  count = local.kafka_sasl ? 1 : 0

  secret_id     = aws_secretsmanager_secret.kafka[0].id
  secret_string = local.kafka.sasl.password
}

resource "aws_secretsmanager_secret" "registry" {
  count = local.registry_password ? 1 : 0

  name_prefix             = "${var.project}-registry-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-registry"
  }
}

resource "aws_secretsmanager_secret_version" "registry" {
  count = local.registry_password ? 1 : 0

  secret_id     = aws_secretsmanager_secret.registry[0].id
  secret_string = local.registry.password
}

resource "aws_secretsmanager_secret" "registry_kafka" {
  count = local.create_confluent_registry && local.kafka_sasl ? 1 : 0

  name_prefix             = "${var.project}-registry-kafka-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-registry-kafka"
  }
}

resource "aws_secretsmanager_secret_version" "registry_kafka" {
  count = local.create_confluent_registry && local.kafka_sasl ? 1 : 0

  secret_id = aws_secretsmanager_secret.registry_kafka[0].id
  secret_string = join(" ", [
    local.kafka_login_modules[local.kafka.sasl.mechanism],
    "required",
    "username=${jsonencode(local.kafka.sasl.username)}",
    "password=${jsonencode(local.kafka.sasl.password)};",
  ])
}
