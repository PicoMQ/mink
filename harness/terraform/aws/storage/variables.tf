variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "create_postgres" {
  type = bool
}

variable "postgres_url" {
  type      = string
  sensitive = true
}

variable "db_instance_class" {
  type = string
}

variable "db_multi_az" {
  type = bool
}

variable "bucket" {
  type = string
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

variable "force_destroy" {
  type = bool
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "postgres_security_group" {
  type = string
}
