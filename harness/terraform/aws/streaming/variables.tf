variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "create_kafka" {
  type = bool
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

variable "msk_version" {
  type = string
}

variable "msk_instance_type" {
  type = string
}

variable "msk_volume_size" {
  type = number
}

variable "create_confluent_registry" {
  type = bool
}

variable "create_glue_registry" {
  type = bool
}

variable "registry" {
  type = object({
    host = string
    port = number
  })
}

variable "schema_registry" {
  type = object({
    type     = string
    url      = optional(string)
    username = optional(string)
    password = optional(string)
    name     = optional(string)
    region   = optional(string)
  })
  sensitive = true
}

variable "force_destroy" {
  type = bool
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "kafka_security_group" {
  type = string
}
