variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "domain" {
  type = string
}

variable "image" {
  type = string
}

variable "create_repository" {
  type = bool
}

variable "cpu_architecture" {
  type = string
}

variable "task_cpu" {
  type = number
}

variable "task_memory" {
  type = number
}

variable "force_destroy" {
  type = bool
}

variable "create_kafka" {
  type = bool
}

variable "create_confluent_registry" {
  type = bool
}

variable "registry_port" {
  type = number
}

variable "nodes" {
  type = map(object({
    id   = number
    host = string
  }))
}

variable "ports" {
  type = map(number)
}

variable "node_ports" {
  type = map(object({
    node = string
    name = string
    port = number
  }))
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "node_security_group" {
  type = string
}

variable "shared_target_group_arns" {
  type = map(string)
}

variable "node_target_group_arns" {
  type = map(string)
}

variable "registry_security_group" {
  type = string
}

variable "registry_target_group_arn" {
  type = string
}

variable "nat_route_ids" {
  type = list(string)
}

variable "s3_endpoint_ids" {
  type = list(string)
}

variable "bucket" {
  type = string
}

variable "rds_ca" {
  type = string
}

variable "postgres_secret_arn" {
  type = string
}

variable "postgres_secret_version_id" {
  type = string
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
  sensitive = true
}

variable "use_kafka_sasl" {
  type = bool
}

variable "kafka_broker_count" {
  type = number
}

variable "kafka_secret_arns" {
  type = list(string)
}

variable "kafka_secret_version_ids" {
  type = list(string)
}

variable "scram_association_ids" {
  type = list(string)
}

variable "registry_settings" {
  type      = map(string)
  sensitive = true
}

variable "registry_password_secret_arns" {
  type = list(string)
}

variable "registry_password_secret_version_ids" {
  type = list(string)
}

variable "use_glue_registry" {
  type = bool
}

variable "glue_registry_arns" {
  type = list(string)
}

variable "create_s3_tables" {
  type = bool
}

variable "external_catalog" {
  type = object({
    uri       = string
    warehouse = optional(string)
    auth = optional(object({
      type    = string
      service = optional(string)
      region  = optional(string)
    }))
  })
}

variable "external_catalog_write_policy" {
  type = string
}
