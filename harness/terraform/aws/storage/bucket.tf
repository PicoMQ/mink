resource "aws_s3_bucket" "warehouse" {
  count = local.create_bucket ? 1 : 0

  bucket_prefix = "${var.project}-"
  force_destroy = var.force_destroy

  tags = {
    Name = var.project
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  count = local.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.warehouse[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  count = local.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.warehouse[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "warehouse" {
  count = local.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.warehouse[0].id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
