locals {
  create_bucket = var.bucket == null

  bucket = local.create_bucket ? aws_s3_bucket.warehouse[0].bucket : var.bucket
  rds_ca = "/certs/rds.pem"

  external_catalog = var.create_s3_tables ? {
    uri       = "https://s3tables.${var.region}.amazonaws.com/iceberg"
    warehouse = aws_s3tables_table_bucket.external[0].arn
    auth      = { type = "sigv4", service = "s3tables", region = var.region }
  } : var.external_catalog

  postgres_url = var.create_postgres ? join("", [
    "postgres://${aws_db_instance.this[0].username}:${random_password.postgres[0].result}",
    "@${aws_db_instance.this[0].address}:5432/${aws_db_instance.this[0].db_name}",
    "?sslmode=verify-full&sslrootcert=${local.rds_ca}",
  ]) : var.postgres_url
}
