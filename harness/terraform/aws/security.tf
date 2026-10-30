data "aws_vpc" "selected" {
  id = local.vpc_id
}

resource "aws_security_group" "alb" {
  name_prefix = "${var.project}-alb-"
  description = "Internal ALB for the Mink catalog, S3 endpoint and admin API"
  vpc_id      = local.vpc_id

  dynamic "ingress" {
    for_each = local.alb_ports

    content {
      description     = "${ingress.key} from allowed networks, nodes, the client and EMR"
      from_port       = ingress.value
      to_port         = ingress.value
      protocol        = "tcp"
      cidr_blocks     = local.allowed_cidrs
      security_groups = concat([aws_security_group.node.id], aws_security_group.client[*].id, aws_security_group.emr[*].id)
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-alb"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "node" {
  name_prefix = "${var.project}-node-"
  description = "Mink nodes"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-node"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "node" {
  for_each = local.ports

  security_group_id            = aws_security_group.node.id
  description                  = "${each.key} from the ALB"
  from_port                    = each.value
  to_port                      = each.value
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.alb.id
}

resource "aws_security_group" "registry" {
  count = local.create_confluent_registry ? 1 : 0

  name_prefix = "${var.project}-registry-"
  description = "Confluent Schema Registry"
  vpc_id      = local.vpc_id

  ingress {
    description     = "Schema Registry from the ALB"
    from_port       = local.registry_port
    to_port         = local.registry_port
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-registry"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "kafka" {
  count = local.create_kafka ? 1 : 0

  name_prefix = "${var.project}-kafka-"
  description = "MSK brokers"
  vpc_id      = local.vpc_id

  ingress {
    description     = "SASL_SSL from nodes, the registry and the client"
    from_port       = 9096
    to_port         = 9096
    protocol        = "tcp"
    security_groups = concat([aws_security_group.node.id], aws_security_group.registry[*].id, aws_security_group.client[*].id)
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-kafka"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "postgres" {
  count = local.create_postgres ? 1 : 0

  name_prefix = "${var.project}-postgres-"
  description = "Mink Postgres"
  vpc_id      = local.vpc_id

  ingress {
    description     = "Postgres from nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.node.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-postgres"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "emr" {
  count = var.emr ? 1 : 0

  name_prefix = "${var.project}-emr-"
  description = "EMR Serverless jobs"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-emr"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "client" {
  count = var.client ? 1 : 0

  name_prefix = "${var.project}-client-"
  description = "Client instance, reached only through SSM"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-client"
  }

  lifecycle {
    create_before_destroy = true
  }
}
