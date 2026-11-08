resource "aws_glue_registry" "this" {
  count = var.create_glue_registry ? 1 : 0

  registry_name = var.project

  tags = {
    Name = var.project
  }
}

data "aws_caller_identity" "this" {}
