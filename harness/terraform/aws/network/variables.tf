variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "create_s3_endpoint" {
  type = bool
}

variable "allowed_cidrs" {
  type = list(string)
}

variable "domain" {
  type = string
}

variable "node_count" {
  type = number
}

variable "create_kafka" {
  type = bool
}

variable "create_postgres" {
  type = bool
}

variable "create_confluent_registry" {
  type = bool
}

variable "registry_port" {
  type = number
}

variable "client" {
  type = bool
}

variable "emr" {
  type = bool
}
