resource "aws_ecs_task_definition" "node" {
  for_each = var.nodes

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
      command   = ["curl", "-fsS", "-o", var.rds_ca, "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"]
      mountPoints = [
        { sourceVolume = "certs", containerPath = dirname(var.rds_ca) },
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
        for port in values(var.ports) : { containerPort = port, hostPort = port, protocol = "tcp" }
      ]
      environment = concat([
        { name = "MINK_WAREHOUSE", value = "s3://${var.bucket}/warehouse" },
        { name = "MINK_STORAGE__REGION", value = var.region },
        { name = "MINK_KAFKA__BOOTSTRAP", value = jsonencode(var.kafka.bootstrap) },
        { name = "MINK_CATALOG_ADDRESS", value = "0.0.0.0:${var.ports.catalog}" },
        { name = "MINK_S3_ADDRESS", value = "0.0.0.0:${var.ports.s3}" },
        { name = "MINK_ADMIN_ADDRESS", value = "0.0.0.0:${var.ports.admin}" },
        { name = "MINK_ADMIN_REMOTE", value = "true" },
        { name = "MINK_CATALOG_ENDPOINT", value = "http://${var.domain}:${var.ports.catalog}" },
        { name = "MINK_S3_ENDPOINT", value = "http://${var.domain}:${var.ports.s3}" },
        { name = "MINK_ADVERTISED_ADDRESS", value = "http://${each.value.host}:${var.ports.s3}" },
        { name = "MINK_ADVERTISED_ADMIN_ADDRESS", value = "http://${each.value.host}:${var.ports.admin}" },
        { name = "MINK_CACHE_DIR", value = local.cache_dir },
        { name = "MINK_CACHE_BYTES", value = tostring(var.task_memory * 1024 * 1024 / 4) },
        { name = "MINK_CACHE_DISK_BYTES", value = tostring(10 * 1024 * 1024 * 1024) },
        ],
        var.kafka.tls ? [{ name = "MINK_KAFKA__TLS", value = "{}" }] : [],
        var.use_kafka_sasl ? [
          { name = "MINK_KAFKA__SASL__MECHANISM", value = var.kafka.sasl.mechanism },
          { name = "MINK_KAFKA__SASL__USERNAME", value = var.kafka.sasl.username },
        ] : [],
        [for key, value in var.registry_settings : { name = "MINK_SCHEMA_REGISTRY__${upper(key)}", value = value }],
        [for name, value in local.external_catalog_settings : { name = name, value = value }],
      )
      secrets = concat(
        [{ name = "MINK_POSTGRES", valueFrom = var.postgres_secret_arn }],
        [for arn in var.kafka_secret_arns : { name = "MINK_KAFKA__SASL__PASSWORD", valueFrom = arn }],
        [for arn in var.registry_password_secret_arns : { name = "MINK_SCHEMA_REGISTRY__PASSWORD", valueFrom = arn }],
      )
      mountPoints = [
        { sourceVolume = "certs", containerPath = dirname(var.rds_ca), readOnly = true },
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
  for_each = var.nodes

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
    subnets          = var.private_subnet_ids
    security_groups  = [var.node_security_group]
    assign_public_ip = false
  }

  dynamic "load_balancer" {
    for_each = var.ports

    content {
      target_group_arn = var.shared_target_group_arns[load_balancer.key]
      container_name   = "mink"
      container_port   = load_balancer.value
    }
  }

  dynamic "load_balancer" {
    for_each = { for key, node_port in var.node_ports : key => node_port if node_port.node == each.key }

    content {
      target_group_arn = var.node_target_group_arns[load_balancer.key]
      container_name   = "mink"
      container_port   = load_balancer.value.port
    }
  }

  depends_on = [
    var.postgres_secret_version_id,
    var.kafka_secret_version_ids,
    var.registry_password_secret_version_ids,
    var.scram_association_ids,
    var.nat_route_ids,
    var.s3_endpoint_ids,
  ]

  tags = {
    Name = "${var.project}-${each.key}"
  }
}
