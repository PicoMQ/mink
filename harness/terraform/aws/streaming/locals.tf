locals {
  kafka_subnet_ids = slice(var.private_subnet_ids, 0, min(3, length(var.private_subnet_ids)))

  scram = var.create_kafka ? jsondecode(aws_secretsmanager_secret_version.scram[0].secret_string) : null

  kafka = var.create_kafka ? {
    bootstrap = split(",", aws_msk_cluster.this[0].bootstrap_brokers_sasl_scram)
    tls       = true
    sasl = {
      mechanism = "SCRAM-SHA-512"
      username  = local.scram.username
      password  = local.scram.password
    }
  } : var.kafka

  use_kafka_sasl   = nonsensitive(local.kafka.sasl != null)
  kafka_secret_arn = one(concat(aws_secretsmanager_secret_version.scram[*].secret_arn, aws_secretsmanager_secret_version.kafka[*].secret_arn))

  kafka_security_protocol = "${local.use_kafka_sasl ? "SASL_" : ""}${local.kafka.tls ? "SSL" : "PLAINTEXT"}"
  kafka_login_modules = {
    PLAIN           = "org.apache.kafka.common.security.plain.PlainLoginModule"
    "SCRAM-SHA-256" = "org.apache.kafka.common.security.scram.ScramLoginModule"
    "SCRAM-SHA-512" = "org.apache.kafka.common.security.scram.ScramLoginModule"
  }

  registry = (
    var.create_confluent_registry ? {
      type     = "confluent"
      url      = "http://${var.registry.host}:${var.registry.port}"
      username = null
      password = null
      name     = null
      region   = null
    } :
    var.create_glue_registry ? {
      type     = "glue"
      url      = null
      username = null
      password = null
      name     = aws_glue_registry.this[0].registry_name
      region   = var.region
    } :
    var.schema_registry
  )

  use_registry          = nonsensitive(local.registry != null)
  use_registry_password = nonsensitive(try(local.registry.password, null) != null)
  registry_secret_arns  = concat(aws_secretsmanager_secret_version.registry_password[*].secret_arn, aws_secretsmanager_secret_version.registry_kafka[*].secret_arn)

  use_glue_registry    = nonsensitive(try(local.registry.type, null) == "glue")
  glue_registry_name   = local.use_glue_registry ? nonsensitive(local.registry.name) : null
  glue_registry_region = local.use_glue_registry ? nonsensitive(coalesce(local.registry.region, var.region)) : null
  glue_registry_arns = local.use_glue_registry ? [
    "arn:aws:glue:${local.glue_registry_region}:${data.aws_caller_identity.this.account_id}:registry/${local.glue_registry_name}",
    "arn:aws:glue:${local.glue_registry_region}:${data.aws_caller_identity.this.account_id}:schema/${local.glue_registry_name}/*",
  ] : []
  glue_registry_actions = {
    read = ["glue:GetRegistry", "glue:GetSchemaVersion"]
    write = [
      "glue:GetRegistry",
      "glue:CreateSchema",
      "glue:GetSchemaByDefinition",
      "glue:RegisterSchemaVersion",
      "glue:GetSchemaVersion",
      "glue:PutSchemaVersionMetadata",
    ]
  }
}
