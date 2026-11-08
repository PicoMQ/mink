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

  use_kafka_sasl = nonsensitive(local.kafka.sasl != null)

  registry = (
    var.create_confluent_registry ? {
      type     = "confluent"
      url      = "http://registry.${var.domain}:${var.registry_port}"
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
  registry_settings     = local.use_registry ? { for key, value in local.registry : key => value if value != null && key != "password" } : {}

  use_glue_registry    = nonsensitive(try(local.registry.type, null) == "glue")
  glue_registry_name   = local.use_glue_registry ? nonsensitive(local.registry.name) : null
  glue_registry_region = local.use_glue_registry ? nonsensitive(coalesce(local.registry.region, var.region)) : null
  glue_registry_arns = local.use_glue_registry ? [
    "arn:aws:glue:${local.glue_registry_region}:${data.aws_caller_identity.this.account_id}:registry/${local.glue_registry_name}",
    "arn:aws:glue:${local.glue_registry_region}:${data.aws_caller_identity.this.account_id}:schema/${local.glue_registry_name}/*",
  ] : []
}
