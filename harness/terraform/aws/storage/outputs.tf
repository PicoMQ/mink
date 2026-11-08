output "bucket" {
  value = local.bucket
}

output "rds_ca" {
  value = local.rds_ca
}

output "postgres_address" {
  value = var.create_postgres ? aws_db_instance.this[0].address : null
}

output "external_catalog" {
  value = local.external_catalog == null ? null : {
    name      = local.external_catalog_name
    uri       = local.external_catalog.uri
    warehouse = local.external_catalog.warehouse
  }
}

output "settings" {
  value = merge(
    {
      MINK_WAREHOUSE       = "s3://${local.bucket}/warehouse"
      MINK_STORAGE__REGION = var.region
    },
    local.external_catalog_settings,
  )
}

output "secrets" {
  value = {
    MINK_POSTGRES = aws_secretsmanager_secret_version.postgres.secret_arn
  }
}

output "policies" {
  value = {
    read = {
      for access, policy in data.aws_iam_policy_document.external_catalog : "external-catalog" => policy.json if access == "read"
    }
    write = merge(
      { s3 = data.aws_iam_policy_document.warehouse.json },
      { for access, policy in data.aws_iam_policy_document.external_catalog : "external-catalog" => policy.json if access == "write" },
    )
    secrets = {
      postgres = data.aws_iam_policy_document.postgres.json
    }
  }
}
