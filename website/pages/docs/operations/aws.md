# AWS

The Terraform stack in `harness/terraform/aws` runs Mink on ECS Fargate. It creates what a deployment needs, or takes existing pieces in their place.

::: info
This stack is a demo. It runs Mink on AWS end to end, and DuckDB, the console and Spark on EMR Serverless query Kafka topics through it as Iceberg tables. Mink runs next to any Kafka cluster, Postgres and S3-compatible store, on AWS or elsewhere, see [Deployment](/docs/operations/deployment).
:::

## What it creates

| Piece | Created | Or existing |
| --- | --- | --- |
| Network | A VPC with two public and two private subnets, a NAT gateway and an S3 gateway endpoint | `vpc_id` and at least two `private_subnet_ids`, with outbound internet access for images, the RDS CA bundle and AWS APIs |
| Kafka | MSK, one broker in each of the first three private subnets, SASL/SCRAM-SHA-512 over TLS on port 9096. The SCRAM user lives in Secrets Manager under its own KMS key. | `kafka`: bootstrap brokers, TLS and SASL. With TLS, the brokers' certificates must chain to a public CA, as Confluent Cloud's and MSK's do. |
| Postgres | RDS PostgreSQL 17. Nodes connect with `sslmode=verify-full` against the RDS CA bundle. | `postgres_url`. For RDS, add `sslmode=verify-full&sslrootcert=/certs/rds.pem`: each node downloads the bundle there before it starts. |
| Bucket | A private, encrypted bucket for the warehouse | `bucket`, in `region` |
| Schema Registry | With `create_schema_registry = "confluent"`, Confluent Schema Registry on ECS, storing schemas in the stack's Kafka, at `registry.<domain>:8081`. With `"glue"`, an AWS Glue registry named after `project`, which nodes read with their task role. | `schema_registry`: a Confluent URL and credentials, or a Glue registry's name and region |
| Nodes | `node_count` ECS services, one task each. They reach S3 with their task role. | |
| Load balancer | An internal ALB on 8181 (catalog), 9100 (S3 endpoint) and 9180 (admin API and console), with a host name per node | |
| DNS | A private Route 53 zone, `domain` (default `mink.internal`), for the ALB and each node | |
| Client | With `client = true`, an EC2 instance with Docker, the Kafka CLI and DuckDB, reached only through SSM | |
| Image repository | With `create_repository = true`, an ECR repository for images built from source | |
| EMR | With `emr = true`, an EMR Serverless Spark application, `emr-7.14.0`, in the private subnets. Its job role reads and writes only the bucket's `emr/` prefix. | |

The Postgres URL and the Kafka and Schema Registry passwords reach the containers from Secrets Manager. Nothing in the stack accepts connections from the internet. The NAT gateway's address is for outbound traffic only.

<div class="mink-diagram">
<svg viewBox="0 0 660 516" width="660" role="img" aria-label="allowed_cidrs reach the internal ALB on 8181, 9100, 9180 and 8081. EMR Serverless and the client reach the ALB. The ALB reaches the nodes and the Schema Registry. Nodes reach the ALB, RDS on 5432 and MSK on 9096. The Schema Registry and the client reach MSK. The client is reached only through SSM.">
  <defs>
    <marker id="aws-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="230" y="10" width="200" height="56" class="box"/>
  <text x="330" y="34" text-anchor="middle" class="label">allowed_cidrs</text>
  <text x="330" y="52" text-anchor="middle" class="sub">the VPC's range by default</text>
  <rect x="470" y="10" width="160" height="56" class="box"/>
  <text x="550" y="34" text-anchor="middle" class="label">SSM</text>
  <text x="550" y="52" text-anchor="middle" class="sub">Session Manager</text>
  <rect x="10" y="90" width="640" height="416" class="edge-soft"/>
  <text x="22" y="104" class="sub">VPC, private subnets</text>
  <rect x="30" y="110" width="160" height="56" class="box"/>
  <text x="110" y="134" text-anchor="middle" class="label">EMR Serverless</text>
  <text x="110" y="152" text-anchor="middle" class="sub">Spark, optional</text>
  <rect x="470" y="110" width="160" height="56" class="box"/>
  <text x="550" y="134" text-anchor="middle" class="label">client</text>
  <text x="550" y="152" text-anchor="middle" class="sub">EC2, optional</text>
  <rect x="30" y="200" width="400" height="56" class="box"/>
  <text x="230" y="224" text-anchor="middle" class="label">ALB</text>
  <text x="230" y="242" text-anchor="middle" class="sub">internal</text>
  <rect x="30" y="320" width="160" height="56" class="box"/>
  <text x="110" y="344" text-anchor="middle" class="label">Schema Registry</text>
  <text x="110" y="362" text-anchor="middle" class="sub">Confluent, optional</text>
  <rect x="230" y="320" width="200" height="56" class="box-accent"/>
  <text x="330" y="344" text-anchor="middle" class="label">nodes</text>
  <text x="330" y="362" text-anchor="middle" class="sub">ECS tasks</text>
  <rect x="470" y="320" width="100" height="56" class="box"/>
  <text x="520" y="344" text-anchor="middle" class="label">RDS</text>
  <text x="520" y="362" text-anchor="middle" class="sub">Postgres</text>
  <rect x="30" y="440" width="600" height="56" class="box"/>
  <text x="330" y="464" text-anchor="middle" class="label">MSK</text>
  <text x="330" y="482" text-anchor="middle" class="sub">SASL/SCRAM over TLS</text>
  <path d="M330 66 L330 196" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M550 66 L550 106" class="edge-soft" marker-end="url(#aws-arrow)"/>
  <path d="M110 166 L110 196" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M550 166 L550 228 L434 228" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M110 256 L110 316" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M370 256 L370 316" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M290 320 L290 260" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M430 348 L466 348" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M110 376 L110 436" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M330 376 L330 436" class="edge" marker-end="url(#aws-arrow)"/>
  <path d="M610 166 L610 436" class="edge" marker-end="url(#aws-arrow)"/>
  <text x="338" y="84" class="sub">8181, 9100, 9180, 8081</text>
  <text x="118" y="186" class="sub">8181, 9100</text>
  <text x="118" y="290" class="sub">8081</text>
  <text x="378" y="290" class="sub">8181, 9100, 9180</text>
  <text x="282" y="290" text-anchor="end" class="sub">forwards</text>
  <text x="448" y="340" text-anchor="middle" class="sub">5432</text>
  <text x="118" y="412" class="sub">9096</text>
  <text x="338" y="412" class="sub">9096</text>
  <text x="602" y="412" text-anchor="end" class="sub">9096</text>
</svg>
</div>

The catalog, the S3 endpoint and the admin API have no authentication. Anything that reaches the ALB reads every table. In a shared VPC, narrow `allowed_cidrs` to the networks that run engines.

An existing Kafka cluster or database in the VPC must accept connections from the nodes. The `node_security_group` output is the group to allow.

## Prerequisites

- Terraform 1.15.8 or later
- AWS credentials that can create the resources above
- The [Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html) for the AWS CLI, to reach the console and the client
- The Mink image, see [Deployment](/docs/operations/deployment#image)

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

## Load TPC-H

`harness/tpch` loads TPC-H `lineitem` into the stack through its Schema Registry and checks the result. It works with either registry type.

| Step | What it does |
| --- | --- |
| Generate | Builds `lineitem` at the given scale with DuckDB |
| Create | Creates a `lineitem` topic, which must not exist yet, and adds it as a table |
| Produce | Writes every row with the registry's own serializer: Confluent's JSON Schema serializer or AWS's Glue serializer |
| Wait | Waits until the catalog's current snapshot holds every row |
| Check | Runs TPC-H Q1 and Q6 through Mink and on DuckDB's own copy, and fails if they differ |

The table exists from the start. The console and engines show it fill as records are committed.

Prices are `double` in Mink and in DuckDB's copy. Sums agree to a relative 1e-9.

On the client, after writing the Kafka client config above:

```bash
dnf install -y git
git clone https://github.com/PicoMQ/mink /opt/mink
echo "bootstrap.servers=$BOOTSTRAP" >> /tmp/client.properties
docker run --rm --network host -e AWS_REGION \
  -v /opt/mink/harness/tpch:/src -v /tmp/client.properties:/client.properties -w /src \
  maven:3.9-eclipse-temurin-25 mvn -q compile exec:java \
  -Dexec.args="/client.properties glue mink http://mink.internal:9180 0.5"
```

With `create_schema_registry = "confluent"`, the registry arguments are `confluent http://registry.mink.internal:8081`.

| Argument | Value |
| --- | --- |
| Kafka properties | A Kafka client `.properties` file |
| Registry type | `confluent` or `glue` |
| Registry | A Confluent registry's URL, or a Glue registry's name in `AWS_REGION` |
| Mink | A node's admin URL. The tool reaches the catalog at `/catalog` and the S3 endpoint at `/s3` under it. |
| Scale | TPC-H scale, `0.5` by default |

At scale `0.5` it loads 2,999,671 rows and ends with `Q1: match` and `Q6: match`, each followed by its result rows. Through Glue on the default stack the run takes about 27 minutes, most of it producing.

## Query from EMR Serverless

With `emr = true`, Spark on EMR Serverless reads tables through the catalog, with EMR's bundled Iceberg and no other packages. `harness/emr/queries.py` runs SQL files against a table and prints the rows.

From `harness/terraform/aws`, upload the script and the TPC-H queries, then start a job:

```bash
BUCKET=$(terraform output -raw bucket)
aws s3 cp ../../emr/queries.py "s3://$BUCKET/emr/queries.py"
aws s3 cp ../../tpch/src/main/resources/queries/ "s3://$BUCKET/emr/queries/" --recursive
cat > /tmp/job.json <<EOF
{
  "sparkSubmit": {
    "entryPoint": "s3://$BUCKET/emr/queries.py",
    "entryPointArguments": ["mink.kafka.lineitem", "s3://$BUCKET/emr/queries/q1.sql", "s3://$BUCKET/emr/queries/q6.sql"],
    "sparkSubmitParameters": "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.catalog.mink=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.mink.type=rest --conf spark.sql.catalog.mink.uri=http://mink.internal:8181 --conf spark.sql.catalog.mink.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  }
}
EOF
cat > /tmp/logs.json <<EOF
{"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": "s3://$BUCKET/emr/logs/"}}}
EOF
APPLICATION=$(terraform output -raw emr_application_id)
JOB=$(aws emr-serverless start-job-run --application-id "$APPLICATION" \
  --execution-role-arn "$(terraform output -raw emr_role_arn)" --name tpch \
  --job-driver file:///tmp/job.json --configuration-overrides file:///tmp/logs.json \
  --query jobRunId --output text)
```

When `aws emr-serverless get-job-run --application-id "$APPLICATION" --job-run-id "$JOB"` reports `SUCCESS`, read the rows:

```bash
aws s3 cp "s3://$BUCKET/emr/logs/applications/$APPLICATION/jobs/$JOB/SPARK_DRIVER/stdout.gz" - | gunzip
```

The rows match the ones `tpch` prints for the same scale.

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
| `create_schema_registry` | none | `confluent` or `glue` |
| `schema_registry` | none | An existing registry: `{ type = "confluent", url, username, password }` or `{ type = "glue", name, region }` |
| `client`, `client_instance_type` | `false`, `t4g.small` | The client instance |
| `emr` | `false` | An EMR Serverless Spark application |
| `force_destroy` | `false` | Let destroy delete data |
