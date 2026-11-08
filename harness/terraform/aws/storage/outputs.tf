output "bucket" {
  value = local.bucket
}

output "rds_ca" {
  value = local.rds_ca
}

output "postgres_address" {
  value = var.create_postgres ? aws_db_instance.this[0].address : null
}

output "postgres_secret_arn" {
  value = aws_secretsmanager_secret.postgres.arn
}

output "postgres_secret_version_id" {
  value = aws_secretsmanager_secret_version.postgres.id
}

output "external_catalog" {
  value = local.external_catalog
}

output "external_catalog_write_policy" {
  value = var.create_s3_tables ? data.aws_iam_policy_document.tables_write[0].json : null
}

output "external_catalog_read_policy" {
  value = var.create_s3_tables ? data.aws_iam_policy_document.tables_read[0].json : null
}
