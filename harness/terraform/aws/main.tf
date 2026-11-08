locals {
  create_kafka    = nonsensitive(var.kafka == null)
  create_postgres = nonsensitive(var.postgres_url == null)

  create_confluent_registry = var.create_schema_registry == "confluent"
  create_glue_registry      = var.create_schema_registry == "glue"

  registry_port = 8081

  create_s3_tables = var.create_external_catalog == "s3tables"
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
  registry_port             = local.registry_port
  client                    = var.client
  emr                       = var.emr
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
  create_s3_tables        = local.create_s3_tables
  external_catalog        = var.external_catalog
  force_destroy           = var.force_destroy
  private_subnet_ids      = module.network.private_subnet_ids
  postgres_security_group = module.network.postgres_security_group
}

module "streaming" {
  source = "./streaming"

  project                   = var.project
  region                    = var.region
  domain                    = var.domain
  create_kafka              = local.create_kafka
  kafka                     = var.kafka
  msk_version               = var.msk_version
  msk_instance_type         = var.msk_instance_type
  msk_volume_size           = var.msk_volume_size
  create_confluent_registry = local.create_confluent_registry
  create_glue_registry      = local.create_glue_registry
  registry_port             = local.registry_port
  schema_registry           = var.schema_registry
  force_destroy             = var.force_destroy
  private_subnet_ids        = module.network.private_subnet_ids
  kafka_security_group      = module.network.kafka_security_group
}

module "compute" {
  source = "./compute"

  project                              = var.project
  region                               = var.region
  domain                               = var.domain
  image                                = var.image
  create_repository                    = var.create_repository
  cpu_architecture                     = var.cpu_architecture
  task_cpu                             = var.task_cpu
  task_memory                          = var.task_memory
  force_destroy                        = var.force_destroy
  create_kafka                         = local.create_kafka
  create_confluent_registry            = local.create_confluent_registry
  registry_port                        = local.registry_port
  nodes                                = module.network.nodes
  ports                                = module.network.ports
  node_ports                           = module.network.node_ports
  private_subnet_ids                   = module.network.private_subnet_ids
  node_security_group                  = module.network.node_security_group
  shared_target_group_arns             = module.network.shared_target_group_arns
  node_target_group_arns               = module.network.node_target_group_arns
  registry_security_group              = module.network.registry_security_group
  registry_target_group_arn            = module.network.registry_target_group_arn
  nat_route_ids                        = module.network.nat_route_ids
  s3_endpoint_ids                      = module.network.s3_endpoint_ids
  bucket                               = module.storage.bucket
  rds_ca                               = module.storage.rds_ca
  postgres_secret_arn                  = module.storage.postgres_secret_arn
  postgres_secret_version_id           = module.storage.postgres_secret_version_id
  kafka                                = module.streaming.kafka
  use_kafka_sasl                       = module.streaming.use_kafka_sasl
  kafka_broker_count                   = module.streaming.kafka_broker_count
  kafka_secret_arns                    = module.streaming.kafka_secret_arns
  kafka_secret_version_ids             = module.streaming.kafka_secret_version_ids
  scram_association_ids                = module.streaming.scram_association_ids
  registry_settings                    = module.streaming.registry_settings
  registry_password_secret_arns        = module.streaming.registry_password_secret_arns
  registry_password_secret_version_ids = module.streaming.registry_password_secret_version_ids
  use_glue_registry                    = module.streaming.use_glue_registry
  glue_registry_arns                   = module.streaming.glue_registry_arns
  create_s3_tables                     = local.create_s3_tables
  external_catalog                     = module.storage.external_catalog
  external_catalog_write_policy        = module.storage.external_catalog_write_policy
}

module "clients" {
  source = "./clients"

  project                      = var.project
  region                       = var.region
  client                       = var.client
  client_instance_type         = var.client_instance_type
  emr                          = var.emr
  cpu_architecture             = var.cpu_architecture
  create_kafka                 = local.create_kafka
  private_subnet_ids           = module.network.private_subnet_ids
  client_security_group        = module.network.client_security_group
  emr_security_group           = module.network.emr_security_group
  nat_route_ids                = module.network.nat_route_ids
  s3_endpoint_ids              = module.network.s3_endpoint_ids
  bucket                       = module.storage.bucket
  scram_secret_arn             = module.streaming.scram_secret_arn
  scram_kms_key_arn            = module.streaming.scram_kms_key_arn
  use_glue_registry            = module.streaming.use_glue_registry
  glue_registry_arns           = module.streaming.glue_registry_arns
  create_s3_tables             = local.create_s3_tables
  external_catalog_read_policy = module.storage.external_catalog_read_policy
}
