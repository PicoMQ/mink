resource "aws_lb" "this" {
  name               = var.project
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = local.private_subnet_ids

  idle_timeout = 300

  tags = {
    Name = var.project
  }
}

resource "aws_lb_target_group" "shared" {
  for_each = local.ports

  name        = "${var.project}-${each.key}"
  port        = each.value
  protocol    = "HTTP"
  vpc_id      = local.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/ready"
    port                = tostring(local.ports.admin)
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  deregistration_delay = 30

  tags = {
    Name = "${var.project}-${each.key}"
  }
}

resource "aws_lb_target_group" "node" {
  for_each = local.node_ports

  name        = "${var.project}-${each.key}"
  port        = each.value.port
  protocol    = "HTTP"
  vpc_id      = local.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/ready"
    port                = tostring(local.ports.admin)
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  deregistration_delay = 30

  tags = {
    Name = "${var.project}-${each.key}"
  }
}

resource "aws_lb_listener" "this" {
  for_each = local.ports

  load_balancer_arn = aws_lb.this.arn
  port              = each.value
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.shared[each.key].arn
  }
}

resource "aws_lb_listener_rule" "node" {
  for_each = local.node_ports

  listener_arn = aws_lb_listener.this[each.value.name].arn
  priority     = local.nodes[each.value.node].id

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.node[each.key].arn
  }

  condition {
    host_header {
      values = [local.nodes[each.value.node].host]
    }
  }
}
