variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "client" {
  type = bool
}

variable "client_instance_type" {
  type = string
}

variable "emr" {
  type = bool
}

variable "cpu_architecture" {
  type = string
}

variable "create_kafka" {
  type = bool
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "client_security_group" {
  type = string
}

variable "emr_security_group" {
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

variable "scram_secret_arn" {
  type = string
}

variable "scram_kms_key_arn" {
  type = string
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

variable "external_catalog_read_policy" {
  type = string
}
