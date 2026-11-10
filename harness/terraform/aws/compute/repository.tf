resource "aws_ecr_repository" "mink" {
  count = var.create_repository ? 1 : 0

  name                 = var.project
  image_tag_mutability = "MUTABLE"
  force_delete         = var.force_destroy

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = var.project
  }
}
