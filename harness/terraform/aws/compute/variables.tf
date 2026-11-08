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

variable "create_confluent_registry" {
  type = bool
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

variable "registry" {
  type = object({
    host = string
    port = number
  })
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

variable "rds_ca" {
  type = string
}

variable "settings" {
  type      = map(string)
  sensitive = true
}

variable "secrets" {
  type = map(string)
}

variable "registry_settings" {
  type      = map(string)
  sensitive = true
}

variable "registry_secrets" {
  type = map(string)
}

variable "execution_policies" {
  type = map(string)
}

variable "task_policies" {
  type = map(string)
}
