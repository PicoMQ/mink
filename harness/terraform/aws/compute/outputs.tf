output "repository_url" {
  value = var.create_repository ? aws_ecr_repository.mink[0].repository_url : null
}
