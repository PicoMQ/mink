data "aws_ec2_instance_type" "client" {
  count = var.create_client ? 1 : 0

  instance_type = var.client_instance_type
}

data "aws_ssm_parameter" "client_ami" {
  count = var.create_client ? 1 : 0

  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${data.aws_ec2_instance_type.client[0].supported_architectures[0]}"
}

data "aws_iam_policy_document" "client_assume" {
  count = var.create_client ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "client" {
  count = var.create_client ? 1 : 0

  name               = "${var.project}-client"
  assume_role_policy = data.aws_iam_policy_document.client_assume[0].json

  tags = {
    Name = "${var.project}-client"
  }
}

resource "aws_iam_role_policy_attachment" "client_ssm" {
  count = var.create_client ? 1 : 0

  role       = aws_iam_role.client[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "client" {
  for_each = var.create_client ? var.client_policies : {}

  name   = each.key
  role   = aws_iam_role.client[0].id
  policy = each.value
}

resource "aws_iam_instance_profile" "client" {
  count = var.create_client ? 1 : 0

  name = "${var.project}-client"
  role = aws_iam_role.client[0].name
}

resource "aws_instance" "client" {
  count = var.create_client ? 1 : 0

  ami                    = data.aws_ssm_parameter.client_ami[0].value
  instance_type          = var.client_instance_type
  subnet_id              = var.private_subnet_ids[0]
  vpc_security_group_ids = [var.client_security_group]
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
    python3.12 -m venv /opt/duckdb
    /opt/duckdb/bin/pip install duckdb==2.0.0
    docker pull apache/kafka:3.9.1
  EOT

  tags = {
    Name = "${var.project}-client"
  }
}
