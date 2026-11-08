resource "aws_secretsmanager_secret" "registry_kafka" {
  count = var.create_confluent_registry && var.use_kafka_sasl ? 1 : 0

  name_prefix             = "${var.project}-registry-kafka-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-registry-kafka"
  }
}

resource "aws_secretsmanager_secret_version" "registry_kafka" {
  count = var.create_confluent_registry && var.use_kafka_sasl ? 1 : 0

  secret_id = aws_secretsmanager_secret.registry_kafka[0].id
  secret_string = join(" ", [
    local.kafka_login_modules[var.kafka.sasl.mechanism],
    "required",
    "username=${jsonencode(var.kafka.sasl.username)}",
    "password=${jsonencode(var.kafka.sasl.password)};",
  ])
}
