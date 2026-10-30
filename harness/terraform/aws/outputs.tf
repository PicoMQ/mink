output "catalog_endpoint" {
  value = "http://${var.domain}:${local.ports.catalog}"
}

output "s3_endpoint" {
  value = "http://${var.domain}:${local.ports.s3}"
}

output "admin_endpoint" {
  value = "http://${var.domain}:${local.ports.admin}"
}

output "nodes" {
  value = { for key, node in local.nodes : key => node.host }
}

output "alb_dns_name" {
  value = aws_lb.this.dns_name
}

output "bucket" {
  value = local.bucket
}

output "kafka_bootstrap" {
  value     = local.kafka.bootstrap
  sensitive = true
}

output "scram_secret_arn" {
  value = local.create_kafka ? aws_secretsmanager_secret.scram[0].arn : null
}

output "postgres_address" {
  value = local.create_postgres ? aws_db_instance.this[0].address : null
}

output "client_instance_id" {
  value = var.client ? aws_instance.client[0].id : null
}

output "repository_url" {
  value = var.create_repository ? aws_ecr_repository.mink[0].repository_url : null
}

output "node_security_group" {
  value = aws_security_group.node.id
}

output "vpc_id" {
  value = local.vpc_id
}

output "private_subnet_ids" {
  value = local.private_subnet_ids
}
