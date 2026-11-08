output "catalog_endpoint" {
  value = "http://${var.domain}:${module.network.ports.catalog}"
}

output "s3_endpoint" {
  value = "http://${var.domain}:${module.network.ports.s3}"
}

output "admin_endpoint" {
  value = "http://${var.domain}:${module.network.ports.admin}"
}

output "nodes" {
  value = { for key, node in module.network.nodes : key => node.host }
}

output "alb_dns_name" {
  value = module.network.alb_dns_name
}

output "bucket" {
  value = module.storage.bucket
}

output "kafka_bootstrap" {
  value     = module.streaming.kafka_bootstrap
  sensitive = true
}

output "scram_secret_arn" {
  value = module.streaming.scram_secret_arn
}

output "external_catalog" {
  value = module.storage.external_catalog
}

output "postgres_address" {
  value = module.storage.postgres_address
}

output "client_instance_id" {
  value = module.clients.client_instance_id
}

output "emr_application_id" {
  value = module.clients.emr_application_id
}

output "emr_role_arn" {
  value = module.clients.emr_role_arn
}

output "repository_url" {
  value = module.compute.repository_url
}

output "node_security_group" {
  value = module.network.node_security_group
}

output "vpc_id" {
  value = module.network.vpc_id
}

output "private_subnet_ids" {
  value = module.network.private_subnet_ids
}
