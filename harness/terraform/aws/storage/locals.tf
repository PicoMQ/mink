locals {
  create_bucket = var.bucket == null

  bucket = local.create_bucket ? aws_s3_bucket.warehouse[0].bucket : var.bucket
  rds_ca = "/certs/rds.pem"

  postgres_url = var.create_postgres ? join("", [
    "postgres://${aws_db_instance.this[0].username}:${random_password.postgres[0].result}",
    "@${aws_db_instance.this[0].address}:5432/${aws_db_instance.this[0].db_name}",
    "?sslmode=verify-full&sslrootcert=${local.rds_ca}",
  ]) : var.postgres_url

  external_catalog_name = "lake"

  external_catalog = var.create_external_catalog ? {
    uri       = "https://s3tables.${var.region}.amazonaws.com/iceberg"
    warehouse = aws_s3tables_table_bucket.external_catalog[0].arn
    auth      = { type = "sigv4", service = "s3tables", region = var.region }
  } : var.external_catalog

  external_catalog_settings = local.external_catalog == null ? {} : {
    for key, value in {
      URI           = local.external_catalog.uri
      WAREHOUSE     = local.external_catalog.warehouse
      AUTH__TYPE    = try(local.external_catalog.auth.type, null)
      AUTH__SERVICE = try(local.external_catalog.auth.service, null)
      AUTH__REGION  = try(local.external_catalog.auth.region, null)
    } : "MINK_EXTERNAL_CATALOGS__${upper(local.external_catalog_name)}__${key}" => value if value != null
  }

  external_catalog_service = try(local.external_catalog.auth.service, null)

  external_catalog_glue = local.external_catalog_service == "glue" ? join(":", [
    "arn:aws:glue",
    coalesce(local.external_catalog.auth.region, var.region),
    split(":", coalesce(local.external_catalog.warehouse, data.aws_caller_identity.this.account_id))[0],
  ]) : null

  external_catalog_resources = (
    local.external_catalog_service == "s3tables" ? [
      local.external_catalog.warehouse,
      "${local.external_catalog.warehouse}/table/*",
    ] :
    local.external_catalog_service == "glue" ? [
      "${local.external_catalog_glue}:catalog",
      "${local.external_catalog_glue}:catalog/*",
      "${local.external_catalog_glue}:database/*",
      "${local.external_catalog_glue}:table/*",
    ] :
    []
  )

  external_catalog_actions = {
    s3tables = {
      read = [
        "s3tables:GetTableBucket",
        "s3tables:GetNamespace",
        "s3tables:ListNamespaces",
        "s3tables:ListTables",
        "s3tables:GetTable",
        "s3tables:GetTableMetadataLocation",
        "s3tables:GetTableData",
      ]
      write = [
        "s3tables:GetTableBucket",
        "s3tables:GetNamespace",
        "s3tables:ListNamespaces",
        "s3tables:ListTables",
        "s3tables:CreateTable",
        "s3tables:DeleteTable",
        "s3tables:GetTable",
        "s3tables:GetTableMetadataLocation",
        "s3tables:UpdateTableMetadataLocation",
        "s3tables:GetTableData",
        "s3tables:PutTableData",
      ]
    }
    glue = {
      read = [
        "glue:GetCatalog",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
      ]
      write = [
        "glue:GetCatalog",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
      ]
    }
  }
}
