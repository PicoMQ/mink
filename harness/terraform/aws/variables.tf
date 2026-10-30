variable "project" {
  type    = string
  default = "mink"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "vpc_id" {
  type    = string
  default = null
}

variable "private_subnet_ids" {
  type    = list(string)
  default = []

  validation {
    condition = (
      (var.vpc_id == null && length(var.private_subnet_ids) == 0) ||
      (var.vpc_id != null && length(var.private_subnet_ids) >= 2)
    )
    error_message = "Omit private_subnet_ids when vpc_id is null. When vpc_id is set provide at least two private subnet ids."
  }
}

variable "create_s3_endpoint" {
  type    = bool
  default = false
}

variable "allowed_cidrs" {
  type    = list(string)
  default = []
}

variable "domain" {
  type    = string
  default = "mink.internal"
}

variable "image" {
  type    = string
  default = "ghcr.io/picomq/mink:latest"
}

variable "create_repository" {
  type    = bool
  default = false
}

variable "node_count" {
  type    = number
  default = 2

  validation {
    condition     = var.node_count >= 1
    error_message = "node_count must be at least 1."
  }
}

variable "cpu_architecture" {
  type    = string
  default = "ARM64"

  validation {
    condition     = contains(["ARM64", "X86_64"], var.cpu_architecture)
    error_message = "cpu_architecture must be ARM64 or X86_64."
  }
}

variable "task_cpu" {
  type    = number
  default = 1024
}

variable "task_memory" {
  type    = number
  default = 2048
}

variable "kafka" {
  type = object({
    bootstrap = list(string)
    tls       = optional(bool, true)
    sasl = optional(object({
      mechanism = string
      username  = string
      password  = string
    }))
  })
  default   = null
  sensitive = true
}

variable "msk_version" {
  type    = string
  default = "3.9.x"
}

variable "msk_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "msk_volume_size" {
  type    = number
  default = 10
}

variable "postgres_url" {
  type      = string
  default   = null
  sensitive = true
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_multi_az" {
  type    = bool
  default = true
}

variable "bucket" {
  type    = string
  default = null
}

variable "schema_registry" {
  type = object({
    url      = string
    username = optional(string)
    password = optional(string)
  })
  default   = null
  sensitive = true
}

variable "client" {
  type    = bool
  default = false
}

variable "client_instance_type" {
  type    = string
  default = "t4g.small"
}

variable "force_destroy" {
  type    = bool
  default = false
}
