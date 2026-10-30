resource "aws_ecs_task_definition" "node" {
  for_each = local.nodes

  family                   = "${var.project}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.task_cpu)
  memory                   = tostring(var.task_memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  volume {
    name = "certs"
  }

  volume {
    name = "cache"
  }

  container_definitions = jsonencode([
    {
      name      = "certs"
      image     = "public.ecr.aws/amazonlinux/amazonlinux:2023"
      essential = false
      command   = ["curl", "-fsS", "-o", local.rds_ca, "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"]
      mountPoints = [
        { sourceVolume = "certs", containerPath = dirname(local.rds_ca) },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mink.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "${each.key}-certs"
        }
      }
    },
    {
      name      = "mink"
      image     = var.image
      essential = true
      dependsOn = [
        { containerName = "certs", condition = "SUCCESS" },
      ]
      portMappings = [
        for port in values(local.ports) : { containerPort = port, hostPort = port, protocol = "tcp" }
      ]
      environment = concat([
        { name = "MINK_WAREHOUSE", value = "s3://${local.bucket}/warehouse" },
        { name = "MINK_STORAGE__REGION", value = var.region },
        { name = "MINK_KAFKA__BOOTSTRAP", value = jsonencode(local.kafka.bootstrap) },
        { name = "MINK_CATALOG_ADDRESS", value = "0.0.0.0:${local.ports.catalog}" },
        { name = "MINK_S3_ADDRESS", value = "0.0.0.0:${local.ports.s3}" },
        { name = "MINK_ADMIN_ADDRESS", value = "0.0.0.0:${local.ports.admin}" },
        { name = "MINK_ADMIN_REMOTE", value = "true" },
        { name = "MINK_CATALOG_ENDPOINT", value = "http://${var.domain}:${local.ports.catalog}" },
        { name = "MINK_S3_ENDPOINT", value = "http://${var.domain}:${local.ports.s3}" },
        { name = "MINK_ADVERTISED_ADDRESS", value = "http://${each.value.host}:${local.ports.s3}" },
        { name = "MINK_ADVERTISED_ADMIN_ADDRESS", value = "http://${each.value.host}:${local.ports.admin}" },
        { name = "MINK_CACHE_DIR", value = local.cache_dir },
        { name = "MINK_CACHE_BYTES", value = tostring(var.task_memory * 1024 * 1024 / 4) },
        { name = "MINK_CACHE_DISK_BYTES", value = tostring(10 * 1024 * 1024 * 1024) },
        ],
        local.kafka.tls ? [{ name = "MINK_KAFKA__TLS", value = "{}" }] : [],
        local.kafka_sasl ? [
          { name = "MINK_KAFKA__SASL__MECHANISM", value = local.kafka.sasl.mechanism },
          { name = "MINK_KAFKA__SASL__USERNAME", value = local.kafka.sasl.username },
        ] : [],
        local.use_registry ? [
          { name = "MINK_SCHEMA_REGISTRY__TYPE", value = "confluent" },
          { name = "MINK_SCHEMA_REGISTRY__URL", value = var.schema_registry.url },
        ] : [],
        try(var.schema_registry.username, null) == null ? [] : [
          { name = "MINK_SCHEMA_REGISTRY__USERNAME", value = var.schema_registry.username },
        ],
      )
      secrets = concat(
        [{ name = "MINK_POSTGRES", valueFrom = aws_secretsmanager_secret.postgres.arn }],
        [for secret in aws_secretsmanager_secret.kafka : { name = "MINK_KAFKA__SASL__PASSWORD", valueFrom = secret.arn }],
        [for secret in aws_secretsmanager_secret.registry : { name = "MINK_SCHEMA_REGISTRY__PASSWORD", valueFrom = secret.arn }],
      )
      mountPoints = [
        { sourceVolume = "certs", containerPath = dirname(local.rds_ca), readOnly = true },
        { sourceVolume = "cache", containerPath = local.cache_dir },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mink.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = each.key
        }
      }
    },
  ])

  tags = {
    Name = "${var.project}-${each.key}"
  }
}

resource "aws_ecs_service" "node" {
  for_each = local.nodes

  name            = "${var.project}-${each.key}"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.node[each.key].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  platform_version = "LATEST"

  enable_execute_command            = true
  health_check_grace_period_seconds = 60

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = local.private_subnet_ids
    security_groups  = [aws_security_group.node.id]
    assign_public_ip = false
  }

  dynamic "load_balancer" {
    for_each = local.ports

    content {
      target_group_arn = aws_lb_target_group.shared[load_balancer.key].arn
      container_name   = "mink"
      container_port   = load_balancer.value
    }
  }

  dynamic "load_balancer" {
    for_each = { for key, node_port in local.node_ports : key => node_port if node_port.node == each.key }

    content {
      target_group_arn = aws_lb_target_group.node[load_balancer.key].arn
      container_name   = "mink"
      container_port   = load_balancer.value.port
    }
  }

  depends_on = [
    aws_lb_listener.this,
    aws_lb_listener_rule.node,
    aws_secretsmanager_secret_version.postgres,
    aws_secretsmanager_secret_version.kafka,
    aws_secretsmanager_secret_version.registry,
    aws_msk_single_scram_secret_association.this,
    aws_db_instance.this,
    aws_route.private_nat,
    aws_vpc_endpoint.s3,
  ]

  tags = {
    Name = "${var.project}-${each.key}"
  }
}
