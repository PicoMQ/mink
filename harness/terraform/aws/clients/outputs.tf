output "client_instance_id" {
  value = var.create_client ? aws_instance.client[0].id : null
}

output "emr_application_id" {
  value = var.create_emr ? aws_emrserverless_application.this[0].id : null
}

output "athena_repository_url" {
  value = var.create_athena ? aws_ecr_repository.athena[0].repository_url : null
}

output "athena_workgroup" {
  value = var.create_athena ? aws_athena_workgroup.this[0].name : null
}

output "emr_role_arn" {
  value = var.create_emr ? aws_iam_role.emr[0].arn : null
}
