resource "aws_glue_registry" "this" {
  count = var.create_glue_registry ? 1 : 0

  registry_name = var.project

  tags = {
    Name = var.project
  }
}

data "aws_caller_identity" "this" {}

data "aws_iam_policy_document" "glue" {
  for_each = local.use_glue_registry ? local.glue_registry_actions : {}

  statement {
    actions   = each.value
    resources = local.glue_registry_arns
  }
}
