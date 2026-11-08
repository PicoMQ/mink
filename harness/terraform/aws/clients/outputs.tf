output "client_instance_id" {
  value = var.create_client ? aws_instance.client[0].id : null
}

output "emr_application_id" {
  value = var.create_emr ? aws_emrserverless_application.this[0].id : null
}

output "emr_role_arn" {
  value = var.create_emr ? aws_iam_role.emr[0].arn : null
}
