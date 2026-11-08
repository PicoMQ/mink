output "kafka" {
  value     = local.kafka
  sensitive = true
}

output "use_kafka_sasl" {
  value = local.use_kafka_sasl
}

output "kafka_broker_count" {
  value = var.create_kafka ? aws_msk_cluster.this[0].number_of_broker_nodes : null
}

output "scram_secret_arn" {
  value = var.create_kafka ? aws_secretsmanager_secret.scram[0].arn : null
}

output "scram_kms_key_arn" {
  value = var.create_kafka ? aws_kms_key.scram[0].arn : null
}

output "scram_association_ids" {
  value = aws_msk_single_scram_secret_association.this[*].id
}

output "kafka_secret_arns" {
  value = aws_secretsmanager_secret.kafka[*].arn
}

output "kafka_secret_version_ids" {
  value = aws_secretsmanager_secret_version.kafka[*].id
}

output "registry_settings" {
  value     = local.registry_settings
  sensitive = true
}

output "registry_password_secret_arns" {
  value = aws_secretsmanager_secret.registry_password[*].arn
}

output "registry_password_secret_version_ids" {
  value = aws_secretsmanager_secret_version.registry_password[*].id
}

output "use_glue_registry" {
  value = local.use_glue_registry
}

output "glue_registry_arns" {
  value = local.glue_registry_arns
}
