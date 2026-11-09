locals {
  create_kafka    = nonsensitive(var.kafka == null)
  create_postgres = nonsensitive(var.postgres_url == null)

  create_confluent_registry = var.create_schema_registry == "confluent"
  create_glue_registry      = var.create_schema_registry == "glue"
}

module "network" {
  source = "./network"

  project                   = var.project
  region                    = var.region
  vpc_id                    = var.vpc_id
  private_subnet_ids        = var.private_subnet_ids
  create_s3_endpoint        = var.create_s3_endpoint
  allowed_cidrs             = var.allowed_cidrs
  domain                    = var.domain
  node_count                = var.node_count
  create_kafka              = local.create_kafka
  create_postgres           = local.create_postgres
  create_confluent_registry = local.create_confluent_registry
  create_client             = var.create_client
  create_emr                = var.create_emr
  create_athena             = var.create_athena
}

module "storage" {
  source = "./storage"

  project                 = var.project
  region                  = var.region
  create_postgres         = local.create_postgres
  postgres_url            = var.postgres_url
  db_instance_class       = var.db_instance_class
  db_multi_az             = var.db_multi_az
  bucket                  = var.bucket
  create_external_catalog = var.create_external_catalog == "s3tables"
  external_catalog        = var.external_catalog
  force_destroy           = var.force_destroy
  private_subnet_ids      = module.network.private_subnet_ids
  postgres_security_group = module.network.postgres_security_group
}

module "streaming" {
  source = "./streaming"

  project                   = var.project
  region                    = var.region
  create_kafka              = local.create_kafka
  kafka                     = var.kafka
  msk_version               = var.msk_version
  msk_instance_type         = var.msk_instance_type
  msk_volume_size           = var.msk_volume_size
  create_confluent_registry = local.create_confluent_registry
  create_glue_registry      = local.create_glue_registry
  registry                  = module.network.registry
  schema_registry           = var.schema_registry
  force_destroy             = var.force_destroy
  private_subnet_ids        = module.network.private_subnet_ids
  kafka_security_group      = module.network.kafka_security_group
}

module "compute" {
  source = "./compute"

  project                   = var.project
  region                    = var.region
  domain                    = var.domain
  image                     = var.image
  create_repository         = var.create_repository
  cpu_architecture          = var.cpu_architecture
  task_cpu                  = var.task_cpu
  task_memory               = var.task_memory
  force_destroy             = var.force_destroy
  create_confluent_registry = local.create_confluent_registry
  nodes                     = module.network.nodes
  ports                     = module.network.ports
  node_ports                = module.network.node_ports
  registry                  = module.network.registry
  private_subnet_ids        = module.network.private_subnet_ids
  node_security_group       = module.network.node_security_group
  shared_target_group_arns  = module.network.shared_target_group_arns
  node_target_group_arns    = module.network.node_target_group_arns
  registry_security_group   = module.network.registry_security_group
  registry_target_group_arn = module.network.registry_target_group_arn
  rds_ca                    = module.storage.rds_ca
  settings                  = merge(module.storage.settings, module.streaming.settings)
  secrets                   = merge(module.storage.secrets, module.streaming.secrets)
  registry_settings         = module.streaming.registry_settings
  registry_secrets          = module.streaming.registry_secrets
  execution_policies        = merge(module.storage.policies.secrets, module.streaming.policies.secrets)
  task_policies             = merge(module.storage.policies.write, module.streaming.policies.read)
}

module "clients" {
  source = "./clients"

  project               = var.project
  create_client         = var.create_client
  client_instance_type  = var.client_instance_type
  create_emr            = var.create_emr
  create_athena         = var.create_athena
  force_destroy         = var.force_destroy
  catalog_endpoint      = "http://${var.domain}:${module.network.ports.catalog}"
  cpu_architecture      = var.cpu_architecture
  private_subnet_ids    = module.network.private_subnet_ids
  client_security_group = module.network.client_security_group
  emr_security_group    = module.network.emr_security_group
  athena_security_group = module.network.athena_security_group
  bucket                = module.storage.bucket
  client_policies       = merge(module.streaming.policies.write, module.streaming.policies.secrets)
  emr_policies          = module.storage.policies.read
}
