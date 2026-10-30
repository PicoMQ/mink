data "aws_ec2_instance_type" "client" {
  count = var.client ? 1 : 0

  instance_type = var.client_instance_type
}

data "aws_ssm_parameter" "client_ami" {
  count = var.client ? 1 : 0

  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${data.aws_ec2_instance_type.client[0].supported_architectures[0]}"
}

data "aws_iam_policy_document" "client_assume" {
  count = var.client ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "client" {
  count = var.client ? 1 : 0

  name               = "${var.project}-client"
  assume_role_policy = data.aws_iam_policy_document.client_assume[0].json

  tags = {
    Name = "${var.project}-client"
  }
}

resource "aws_iam_role_policy_attachment" "client_ssm" {
  count = var.client ? 1 : 0

  role       = aws_iam_role.client[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "client_kafka" {
  count = var.client && local.create_kafka ? 1 : 0

  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.scram[0].arn]
  }

  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.scram[0].arn]
  }
}

resource "aws_iam_role_policy" "client_kafka" {
  count = var.client && local.create_kafka ? 1 : 0

  name   = "kafka"
  role   = aws_iam_role.client[0].id
  policy = data.aws_iam_policy_document.client_kafka[0].json
}

resource "aws_iam_instance_profile" "client" {
  count = var.client ? 1 : 0

  name = "${var.project}-client"
  role = aws_iam_role.client[0].name
}

resource "aws_instance" "client" {
  count = var.client ? 1 : 0

  ami                    = data.aws_ssm_parameter.client_ami[0].value
  instance_type          = var.client_instance_type
  subnet_id              = local.private_subnet_ids[0]
  vpc_security_group_ids = [aws_security_group.client[0].id]
  iam_instance_profile   = aws_iam_instance_profile.client[0].name

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = 20
    encrypted   = true
  }

  user_data = <<-EOT
    #!/bin/bash
    set -euo pipefail
    dnf install -y docker python3.12 jq
    systemctl enable --now docker
    echo "export AWS_REGION=${var.region}" > /etc/profile.d/region.sh
    python3.12 -m venv /opt/duckdb
    /opt/duckdb/bin/pip install duckdb==2.0.0
    docker pull apache/kafka:3.9.1
  EOT

  tags = {
    Name = "${var.project}-client"
  }

  depends_on = [aws_route.private_nat, aws_vpc_endpoint.s3]
}
