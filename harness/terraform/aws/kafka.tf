resource "aws_msk_configuration" "this" {
  count = local.create_kafka ? 1 : 0

  name           = var.project
  kafka_versions = [var.msk_version]

  server_properties = <<-EOT
    allow.everyone.if.no.acl.found=true
    auto.create.topics.enable=false
  EOT
}

resource "aws_msk_cluster" "this" {
  count = local.create_kafka ? 1 : 0

  cluster_name           = var.project
  kafka_version          = var.msk_version
  number_of_broker_nodes = length(local.kafka_subnet_ids)

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = local.kafka_subnet_ids
    security_groups = [aws_security_group.kafka[0].id]

    storage_info {
      ebs_storage_info {
        volume_size = var.msk_volume_size
      }
    }
  }

  client_authentication {
    unauthenticated = false

    sasl {
      scram = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this[0].arn
    revision = aws_msk_configuration.this[0].latest_revision
  }

  tags = {
    Name = var.project
  }
}

resource "aws_msk_single_scram_secret_association" "this" {
  count = local.create_kafka ? 1 : 0

  cluster_arn = aws_msk_cluster.this[0].arn
  secret_arn  = aws_secretsmanager_secret.scram[0].arn

  depends_on = [aws_secretsmanager_secret_version.scram]
}
