resource "aws_db_subnet_group" "this" {
  count = local.create_postgres ? 1 : 0

  name       = var.project
  subnet_ids = local.private_subnet_ids

  tags = {
    Name = var.project
  }
}

resource "aws_db_instance" "this" {
  count = local.create_postgres ? 1 : 0

  identifier     = var.project
  engine         = "postgres"
  engine_version = "17"

  instance_class        = var.db_instance_class
  allocated_storage     = 20
  max_allocated_storage = 100
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = local.user
  username = local.user
  password = random_password.postgres[0].result

  db_subnet_group_name   = aws_db_subnet_group.this[0].name
  vpc_security_group_ids = [aws_security_group.postgres[0].id]
  publicly_accessible    = false
  multi_az               = var.db_multi_az

  backup_retention_period   = 7
  deletion_protection       = !var.force_destroy
  skip_final_snapshot       = var.force_destroy
  final_snapshot_identifier = var.force_destroy ? null : "${var.project}-final"

  apply_immediately = true

  tags = {
    Name = var.project
  }
}
