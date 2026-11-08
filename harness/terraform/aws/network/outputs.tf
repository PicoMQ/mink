output "vpc_id" {
  value = local.vpc_id
}

output "private_subnet_ids" {
  value = local.private_subnet_ids
}

output "nodes" {
  value = local.nodes
}

output "ports" {
  value = local.ports
}

output "node_ports" {
  value = local.node_ports
}

output "alb_dns_name" {
  value = aws_lb.this.dns_name
}

output "node_security_group" {
  value = aws_security_group.node.id
}

output "registry_security_group" {
  value = var.create_confluent_registry ? aws_security_group.registry[0].id : null
}

output "kafka_security_group" {
  value = var.create_kafka ? aws_security_group.kafka[0].id : null
}

output "postgres_security_group" {
  value = var.create_postgres ? aws_security_group.postgres[0].id : null
}

output "emr_security_group" {
  value = var.emr ? aws_security_group.emr[0].id : null
}

output "client_security_group" {
  value = var.client ? aws_security_group.client[0].id : null
}

output "shared_target_group_arns" {
  value = { for key, group in aws_lb_target_group.shared : key => group.arn }

  depends_on = [aws_lb_listener.this]
}

output "node_target_group_arns" {
  value = { for key, group in aws_lb_target_group.node : key => group.arn }

  depends_on = [aws_lb_listener_rule.node]
}

output "registry_target_group_arn" {
  value = var.create_confluent_registry ? aws_lb_target_group.registry[0].arn : null

  depends_on = [aws_lb_listener.registry]
}

output "nat_route_ids" {
  value = aws_route.private_nat[*].id
}

output "s3_endpoint_ids" {
  value = aws_vpc_endpoint.s3[*].id
}
