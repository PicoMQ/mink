resource "aws_ecs_task_definition" "registry" {
  count = var.create_confluent_registry ? 1 : 0

  family                   = "${var.project}-registry"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.execution.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([
    {
      name      = "registry"
      image     = "confluentinc/cp-schema-registry:8.1.1"
      essential = true
      portMappings = [
        { containerPort = var.registry_port, hostPort = var.registry_port, protocol = "tcp" },
      ]
      environment = concat([
        { name = "SCHEMA_REGISTRY_HOST_NAME", value = "registry.${var.domain}" },
        { name = "SCHEMA_REGISTRY_LISTENERS", value = "http://0.0.0.0:${var.registry_port}" },
        { name = "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", value = join(",", var.kafka.bootstrap) },
        { name = "SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", value = local.kafka_security_protocol },
        ],
        var.use_kafka_sasl ? [
          { name = "SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM", value = var.kafka.sasl.mechanism },
        ] : [],
        var.create_kafka ? [
          { name = "SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", value = tostring(var.kafka_broker_count) },
        ] : [],
      )
      secrets = [
        for secret in aws_secretsmanager_secret.registry_kafka : { name = "SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG", valueFrom = secret.arn }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mink.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "registry"
        }
      }
    },
  ])

  tags = {
    Name = "${var.project}-registry"
  }
}

resource "aws_ecs_service" "registry" {
  count = var.create_confluent_registry ? 1 : 0

  name            = "${var.project}-registry"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.registry[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  platform_version = "LATEST"

  health_check_grace_period_seconds = 120

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.registry_security_group]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = var.registry_target_group_arn
    container_name   = "registry"
    container_port   = var.registry_port
  }

  depends_on = [
    aws_secretsmanager_secret_version.registry_kafka,
    var.scram_association_ids,
    var.nat_route_ids,
  ]

  tags = {
    Name = "${var.project}-registry"
  }
}
