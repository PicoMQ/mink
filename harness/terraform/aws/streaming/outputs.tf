output "kafka_bootstrap" {
  value     = local.kafka.bootstrap
  sensitive = true
}

output "scram_secret_arn" {
  value = var.create_kafka ? aws_secretsmanager_secret.scram[0].arn : null
}

output "settings" {
  value = merge(
    { MINK_KAFKA__BOOTSTRAP = jsonencode(local.kafka.bootstrap) },
    local.kafka.tls ? { MINK_KAFKA__TLS = "{}" } : {},
    local.use_kafka_sasl ? {
      MINK_KAFKA__SASL__MECHANISM = local.kafka.sasl.mechanism
      MINK_KAFKA__SASL__USERNAME  = local.kafka.sasl.username
    } : {},
    local.use_registry ? {
      for key, value in local.registry : "MINK_SCHEMA_REGISTRY__${upper(key)}" => value if value != null && key != "password"
    } : {},
  )
  sensitive = true
}

output "secrets" {
  value = merge(
    local.use_kafka_sasl ? { MINK_KAFKA__SASL__PASSWORD = "${local.kafka_secret_arn}:password::" } : {},
    local.use_registry_password ? { MINK_SCHEMA_REGISTRY__PASSWORD = aws_secretsmanager_secret_version.registry_password[0].secret_arn } : {},
  )

  depends_on = [aws_msk_single_scram_secret_association.this]
}

output "registry_settings" {
  value = merge(
    {
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS = join(",", local.kafka.bootstrap)
      SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL = local.kafka_security_protocol
    },
    local.use_kafka_sasl ? { SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM = local.kafka.sasl.mechanism } : {},
    var.create_kafka ? { SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR = tostring(aws_msk_cluster.this[0].number_of_broker_nodes) } : {},
  )
  sensitive = true
}

output "registry_secrets" {
  value = {
    for secret in aws_secretsmanager_secret_version.registry_kafka : "SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG" => secret.secret_arn
  }

  depends_on = [aws_msk_single_scram_secret_association.this]
}

output "policies" {
  value = {
    read = {
      for access, policy in data.aws_iam_policy_document.glue : "glue" => policy.json if access == "read"
    }
    write = {
      for access, policy in data.aws_iam_policy_document.glue : "glue" => policy.json if access == "write"
    }
    secrets = merge(
      { for policy in data.aws_iam_policy_document.kafka : "kafka" => policy.json },
      { for policy in data.aws_iam_policy_document.registry : "registry" => policy.json },
    )
  }
}
