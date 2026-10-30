locals {
  create_network     = var.vpc_id == null
  create_s3_endpoint = local.create_network || var.create_s3_endpoint
  create_kafka       = nonsensitive(var.kafka == null)
  create_postgres    = nonsensitive(var.postgres_url == null)
  create_bucket      = var.bucket == null
  use_registry       = nonsensitive(var.schema_registry != null)

  vpc_id             = local.create_network ? aws_vpc.this[0].id : var.vpc_id
  private_subnet_ids = local.create_network ? aws_subnet.private[*].id : var.private_subnet_ids
  allowed_cidrs      = length(var.allowed_cidrs) > 0 ? var.allowed_cidrs : [data.aws_vpc.selected.cidr_block]

  kafka_subnet_ids = slice(local.private_subnet_ids, 0, min(3, length(local.private_subnet_ids)))

  bucket    = local.create_bucket ? aws_s3_bucket.warehouse[0].bucket : var.bucket
  rds_ca    = "/certs/rds.pem"
  cache_dir = "/cache"

  nodes = {
    for i in range(1, var.node_count + 1) : tostring(i) => {
      id   = i
      host = "node-${i}.${var.domain}"
    }
  }

  ports = {
    catalog = 8181
    s3      = 9100
    admin   = 9180
  }

  node_ports = {
    for pair in setproduct(keys(local.nodes), ["s3", "admin"]) : "${pair[1]}-${pair[0]}" => {
      node = pair[0]
      name = pair[1]
      port = local.ports[pair[1]]
    }
  }

  scram = local.create_kafka ? jsondecode(aws_secretsmanager_secret_version.scram[0].secret_string) : null

  kafka = local.create_kafka ? {
    bootstrap = split(",", aws_msk_cluster.this[0].bootstrap_brokers_sasl_scram)
    tls       = true
    sasl = {
      mechanism = "SCRAM-SHA-512"
      username  = local.scram.username
      password  = local.scram.password
    }
  } : var.kafka

  kafka_sasl        = nonsensitive(local.kafka.sasl != null)
  registry_password = nonsensitive(try(var.schema_registry.password, null) != null)

  postgres_url = local.create_postgres ? join("", [
    "postgres://${aws_db_instance.this[0].username}:${random_password.postgres[0].result}",
    "@${aws_db_instance.this[0].address}:5432/${aws_db_instance.this[0].db_name}",
    "?sslmode=verify-full&sslrootcert=${local.rds_ca}",
  ]) : var.postgres_url
}
