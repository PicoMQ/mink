output "client_instance_id" {
  value = var.client ? aws_instance.client[0].id : null
}

output "emr_application_id" {
  value = var.emr ? aws_emrserverless_application.this[0].id : null
}

output "emr_role_arn" {
  value = var.emr ? aws_iam_role.emr[0].arn : null
}
