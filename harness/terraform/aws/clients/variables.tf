variable "project" {
  type = string
}

variable "create_client" {
  type = bool
}

variable "client_instance_type" {
  type = string
}

variable "create_emr" {
  type = bool
}

variable "cpu_architecture" {
  type = string
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

variable "bucket" {
  type = string
}

variable "client_policies" {
  type = map(string)
}

variable "emr_policies" {
  type = map(string)
}
