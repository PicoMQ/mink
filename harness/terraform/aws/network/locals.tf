locals {
  create_network     = var.vpc_id == null
  create_s3_endpoint = local.create_network || var.create_s3_endpoint

  vpc_id             = local.create_network ? aws_vpc.this[0].id : var.vpc_id
  private_subnet_ids = local.create_network ? aws_subnet.private[*].id : var.private_subnet_ids
  allowed_cidrs      = length(var.allowed_cidrs) > 0 ? var.allowed_cidrs : [data.aws_vpc.selected.cidr_block]

  nodes = {
    for i in range(1, var.node_count + 1) : tostring(i) => {
      id   = i
      host = "node-${i}.${var.domain}"
    }
  }

  ports = {
    catalog = 8181
    s3      = 9100
    admin   = 9180
  }

  alb_ports = merge(local.ports, var.create_confluent_registry ? { registry = var.registry_port } : {})

  node_ports = {
    for pair in setproduct(keys(local.nodes), ["s3", "admin"]) : "${pair[1]}-${pair[0]}" => {
      node = pair[0]
      name = pair[1]
      port = local.ports[pair[1]]
    }
  }
}
