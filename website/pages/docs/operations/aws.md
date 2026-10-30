# AWS

The Terraform stack in `harness/terraform/aws` runs Mink on ECS Fargate. It creates what a deployment needs, or takes existing pieces in their place.

## What it creates

| Piece | Created | Or existing |
| --- | --- | --- |
| Network | A VPC with two public and two private subnets, a NAT gateway and an S3 gateway endpoint | `vpc_id` and at least two `private_subnet_ids`, with outbound internet access for images, the RDS CA bundle and AWS APIs |
| Kafka | MSK, one broker in each of the first three private subnets, SASL/SCRAM-SHA-512 over TLS on port 9096. The SCRAM user lives in Secrets Manager under its own KMS key. | `kafka`: bootstrap brokers, TLS and SASL. With TLS, the brokers' certificates must chain to a public CA, as Confluent Cloud's and MSK's do. |
| Postgres | RDS PostgreSQL 17. Nodes connect with `sslmode=verify-full` against the RDS CA bundle. | `postgres_url`. For RDS, add `sslmode=verify-full&sslrootcert=/certs/rds.pem`: each node downloads the bundle there before it starts. |
| Bucket | A private, encrypted bucket for the warehouse | `bucket`, in `region` |
| Schema Registry | None | `schema_registry`: URL and credentials |
| Nodes | `node_count` ECS services, one task each. They reach S3 with their task role. | |
| Load balancer | An internal ALB on 8181 (catalog), 9100 (S3 endpoint) and 9180 (admin API and console), with a host name per node | |
| DNS | A private Route 53 zone, `domain` (default `mink.internal`), for the ALB and each node | |
| Client | With `client = true`, an EC2 instance with Docker, the Kafka CLI and DuckDB, reached only through SSM | |
| Image repository | With `create_repository = true`, an ECR repository for images built from source | |

The Postgres URL and the Kafka and Schema Registry passwords reach the nodes from Secrets Manager. Nothing in the stack accepts connections from the internet. The NAT gateway's address is for outbound traffic only.

| Reaches | From |
| --- | --- |
| The ALB | `allowed_cidrs` (the VPC's range by default), nodes and the client |
| Nodes | The ALB only |
| MSK | Nodes and the client |
| RDS | Nodes only |
| The client | Nothing. SSM Session Manager only. |

The catalog, the S3 endpoint and the admin API have no authentication. Anything that reaches the ALB reads every table. In a shared VPC, narrow `allowed_cidrs` to the networks that run engines.

An existing Kafka cluster or database in the VPC must accept connections from the nodes. The `node_security_group` output is the group to allow.

## Prerequisites

- Terraform 1.15.8 or later
- AWS credentials that can create the resources above
- The [Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html) for the AWS CLI, to reach the console and the client
- The Mink image. The default, `ghcr.io/picomq/mink:latest`, is published with each release, for `arm64` and `amd64`. Before the first release, build it from source as shown below.

## Apply

```bash
cd harness/terraform/aws
cp terraform.tfvars.example terraform.tfvars
cp backend.hcl.example backend.hcl
terraform init -backend-config=backend.hcl
```

A new stack with every piece created, a client, and nothing kept on destroy:

```hcl
region        = "us-east-1"
project       = "mink"
node_count    = 2
client        = true
force_destroy = true
db_multi_az   = false
```

```bash
terraform apply
```

MSK takes the longest to create, often more than half an hour.

To run an image built from source, set `create_repository = true`, create the repository first, push to it, then apply the rest:

```bash
terraform apply -target=aws_ecr_repository.mink
REPOSITORY=$(terraform output -raw repository_url)
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPOSITORY%%/*}"
docker buildx build --platform linux/arm64 -t "$REPOSITORY:dev" --push ../../..
terraform apply -var image="$REPOSITORY:dev"
```

## Open the console

Forward a local port to the ALB through the client:

```bash
aws ssm start-session \
  --target "$(terraform output -raw client_instance_id)" \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters host=mink.internal,portNumber=9180,localPortNumber=9180
```

`http://localhost:9180` is the console while the session is open. Its SQL page goes through the same port.

## Try it

Open a shell on the client and become root, which runs Docker:

```bash
aws ssm start-session --target "$(terraform output -raw client_instance_id)"
sudo -i
```

Write the SCRAM credentials into a Kafka client config. `SECRET` is the `scram_secret_arn` output, and `BOOTSTRAP` is the `kafka_bootstrap` output joined with commas.

```bash
CREDENTIALS=$(aws secretsmanager get-secret-value --secret-id "$SECRET" --query SecretString --output text)
cat > /tmp/client.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$(echo "$CREDENTIALS" | jq -r .username)" password="$(echo "$CREDENTIALS" | jq -r .password)";
EOF
kafka() { docker run --rm -i -v /tmp/client.properties:/client.properties apache/kafka:3.9.1 "$@"; }
```

Create a topic, write three records and add the topic as a table:

```bash
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --command-config /client.properties --create --topic payments --partitions 3
printf '%s\n' '{"id":1,"item":"tea"}' '{"id":2,"item":"bread"}' '{"id":3,"item":"milk"}' |
  kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --producer.config /client.properties --topic payments
curl -X POST http://mink.internal:9180/admin/tables \
  -H 'content-type: application/json' -d '{"topic": "payments"}'
```

Query it with DuckDB:

```bash
/opt/duckdb/bin/python - <<'EOF'
import duckdb
db = duckdb.connect()
for statement in [
    "INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs",
    "CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', "
    "ENDPOINT 'mink.internal:9100', URL_STYLE 'path', USE_SSL false)",
    "ATTACH 'warehouse' AS mink (TYPE iceberg, ENDPOINT 'http://mink.internal:8181', AUTHORIZATION_TYPE 'none')",
]:
    db.execute(statement)
db.sql("SELECT _kafka_partition, _kafka_offset, CAST(_kafka_value AS VARCHAR) FROM mink.kafka.payments").show()
EOF
```

Without a Schema Registry, each value stays one column of raw bytes.

## Destroy

```bash
terraform destroy
```

With `force_destroy = false`, the default, the database keeps a final snapshot, deletion protection stays on, and the bucket must be empty first.

## Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `project` | `mink` | Prefix for every resource name |
| `region` | `us-east-1` | AWS region |
| `vpc_id`, `private_subnet_ids` | none | An existing VPC and its private subnets |
| `create_s3_endpoint` | `false` | Add an S3 gateway endpoint to an existing VPC |
| `allowed_cidrs` | the VPC's range | Networks that reach the ALB |
| `domain` | `mink.internal` | The private zone |
| `image` | `ghcr.io/picomq/mink:latest` | The Mink image |
| `create_repository` | `false` | Create an ECR repository |
| `node_count` | `2` | Nodes, one ECS service each |
| `cpu_architecture` | `ARM64` | `ARM64` or `X86_64` |
| `task_cpu`, `task_memory` | `1024`, `2048` | Per node. A quarter of the memory goes to the cache. |
| `kafka` | none | An existing cluster: `bootstrap`, `tls` and `sasl` |
| `msk_version`, `msk_instance_type`, `msk_volume_size` | `3.9.x`, `kafka.t3.small`, `10` | The MSK cluster |
| `postgres_url` | none | An existing database |
| `db_instance_class`, `db_multi_az` | `db.t4g.micro`, `true` | The RDS instance |
| `bucket` | none | An existing bucket |
| `schema_registry` | none | `url`, `username` and `password` |
| `client`, `client_instance_type` | `false`, `t4g.small` | The client instance |
| `force_destroy` | `false` | Let destroy delete data |
