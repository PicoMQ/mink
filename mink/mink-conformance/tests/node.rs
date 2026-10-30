use std::fs;
use std::net::TcpListener;
use std::ops::Range;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use mink_core::{Member, Offsets, Partition};
use mink_iceberg::FIRST_TAIL_SNAPSHOT_ID;
use mink_kafka::{Cluster, Config as KafkaConfig};
use mink_metrics::Metrics;
use mink_server::{Config, Error, Node, Role};
use mink_store::{LeaseHolder, Store};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::time::Instant;
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(60);
const LOCALHOST: &str = "127.0.0.1";
const PORTS: Range<u16> = 20_000..32_000;

static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

struct TestNode {
    node: Node,
    config: Config,
    catalog: String,
    admin: String,
    s3_port: u16,
    _cache: tempfile::TempDir,
}

fn free_port() -> u16 {
    let span = u32::from(PORTS.end - PORTS.start);
    let block = std::process::id() % (span / 100) * 100;

    loop {
        let offset = (block + u32::from(NEXT_PORT.fetch_add(1, Ordering::Relaxed))) % span;
        let port = PORTS.start + offset as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

impl TestNode {
    async fn shutdown(self) {
        self.node.shutdown().await.unwrap();
    }

    fn kill(self) {
        self.node.kill();
    }
}

struct TestEnv {
    postgres: String,
    bootstrap: String,
    s3: String,
    registry: Option<String>,
    host: String,
}

async fn test_env() -> Option<TestEnv> {
    let postgres = mink_conformance::service("TEST_POSTGRES_URL")?;

    Some(TestEnv {
        postgres: mink_conformance::fresh_schema(&postgres).await,
        bootstrap: mink_conformance::service("TEST_KAFKA_BOOTSTRAP")?,
        s3: mink_conformance::service("TEST_S3_ENDPOINT")?,
        registry: mink_conformance::service("TEST_SCHEMA_REGISTRY"),
        host: mink_conformance::service("TEST_ENGINE_HOST").unwrap_or_else(|| LOCALHOST.to_owned()),
    })
}

async fn start_node(env: &TestEnv, topic: &str, roles: &[Role], settings: &str) -> TestNode {
    let node = start(env, roles, settings).await;

    match mink_server::add_table(&node.config, topic, None).await {
        Ok(_) | Err(Error::Store(mink_store::Error::AlreadyTable { .. })) => {}
        Err(error) => panic!("{error}"),
    }
    node
}

async fn start(env: &TestEnv, roles: &[Role], settings: &str) -> TestNode {
    let TestEnv {
        postgres,
        bootstrap,
        s3,
        registry,
        host,
    } = env;
    let roles: Vec<String> = roles
        .iter()
        .map(|role| format!("{:?}", role.name()))
        .collect();
    let roles = roles.join(", ");
    let (catalog_port, s3_port, admin_port) = (free_port(), free_port(), free_port());
    let bind = if host == LOCALHOST {
        LOCALHOST
    } else {
        "0.0.0.0"
    };
    let cache = tempfile::tempdir().unwrap();
    let cache_dir = cache.path().display();
    let schema_registry = registry
        .as_ref()
        .map(|url| {
            format!("[schema_registry]\n        type = \"confluent\"\n        url = \"{url}\"")
        })
        .unwrap_or_default();
    let config = format!(
        r#"
        postgres = "{postgres}"
        catalog_address = "{bind}:{catalog_port}"
        s3_address = "{bind}:{s3_port}"
        s3_endpoint = "http://{host}:{s3_port}"
        advertised_address = "http://{host}:{s3_port}"
        admin_address = "127.0.0.1:{admin_port}"
        roles = [{roles}]
        file_window = "200ms"
        snapshot_interval = "200ms"
        lease_ttl = "3s"
        cache_dir = "{cache_dir}"
        cache_disk_bytes = 268435456
        {settings}

        [kafka]
        bootstrap = ["{bootstrap}"]

        [storage]
        endpoint = "{s3}"
        region = "us-east-1"
        credentials = {{ access_key_id = "mink", secret_access_key = "minkminkmink" }}

        {schema_registry}
        "#
    );
    let path = std::env::temp_dir().join(format!("mink-{}.toml", Uuid::new_v4().simple()));
    fs::write(&path, config).unwrap();

    let config = Config::load(Some(&path)).unwrap();
    fs::remove_file(&path).unwrap();

    let node = Node::start(config.clone()).await.unwrap();
    TestNode {
        node,
        config,
        catalog: format!("http://{host}:{catalog_port}"),
        admin: format!("http://127.0.0.1:{admin_port}"),
        s3_port,
        _cache: cache,
    }
}

async fn committed(bootstrap: &str, topic: &str, partitions: i32) -> u64 {
    let mut cluster = Cluster::connect(
        KafkaConfig::new(vec![bootstrap.to_owned()]).unwrap(),
        Metrics::new(),
    )
    .await
    .unwrap();
    let mut records = 0;

    for index in 0..partitions {
        let partition = Partition::new(topic, index).unwrap();
        let stable = cluster.last_stable_offset(&partition).await.unwrap();
        let offsets = Offsets::new(partition, 0, stable).unwrap();

        records += cluster.read(&offsets).await.unwrap().len() as u64;
    }

    records
}

async fn load_table(node: &TestNode, topic: &str) -> Option<Value> {
    let url = format!("{}/v1/namespaces/kafka/tables/{topic}", node.catalog);
    let response = reqwest::get(url).await.ok()?;

    match response.status().is_success() {
        true => response.json().await.ok(),
        false => None,
    }
}

async fn wait_for_records(node: &TestNode, topic: &str, records: u64) -> Value {
    let wanted = records.to_string();

    wait_for(node, topic, |table| {
        summary(table)["total-records"] == wanted.as_str()
    })
    .await
}

async fn wait_for(node: &TestNode, topic: &str, ready: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + WAIT;

    loop {
        if let Some(table) = load_table(node, topic).await
            && ready(&table)
        {
            return table;
        }

        assert!(Instant::now() < deadline, "{topic} never became ready");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_committed(node: &TestNode, topic: &str, records: u64) -> Value {
    let wanted = records.to_string();

    wait_for(node, topic, |table| {
        let snapshots = table["metadata"]["snapshots"].as_array();
        let committed = snapshots.into_iter().flatten().rev().find(|snapshot| {
            snapshot["snapshot-id"]
                .as_i64()
                .is_some_and(|id| id < FIRST_TAIL_SNAPSHOT_ID)
        });

        committed.is_some_and(|snapshot| snapshot["summary"]["total-records"] == wanted.as_str())
    })
    .await
}

fn summary(table: &Value) -> &Value {
    let snapshots = table["metadata"]["snapshots"].as_array();

    snapshots
        .and_then(|snapshots| snapshots.last())
        .map_or(&Value::Null, |snapshot| &snapshot["summary"])
}

async fn python(python: &str, script: &str) -> Value {
    let output = Command::new(python)
        .args(["-c", script])
        .output()
        .await
        .unwrap();

    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        let (stdout, stderr) = (
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        panic!("python wrote {stdout:?} and {stderr:?}")
    })
}

async fn trino(trino: &str, sql: &str) -> Vec<Value> {
    let client = reqwest::Client::new();
    let mut page: Value = client
        .post(format!("{trino}/v1/statement"))
        .header("X-Trino-User", "mink")
        .body(sql.to_owned())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let mut rows = Vec::new();
    loop {
        if let Some(error) = page.get("error") {
            panic!("trino failed {sql:?}: {}", error["message"]);
        }
        rows.extend(page["data"].as_array().into_iter().flatten().cloned());

        let Some(next) = page["nextUri"].as_str() else {
            return rows;
        };
        page = client
            .get(next)
            .header("X-Trino-User", "mink")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }
}

async fn topic_with_records(bootstrap: &str, partitions: i32) -> String {
    let topic = format!("e2e-{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(bootstrap, &topic, partitions).await;

    mink_conformance::produce_values(bootstrap, &topic, 0, &["a", "b", "c", "d", "e"]);
    mink_conformance::produce_aborted(bootstrap, &topic, 0, &["x", "y", "z"]);
    mink_conformance::produce_values(bootstrap, &topic, partitions - 1, &["f", "g", "h", "i"]);
    topic
}

async fn duckdb_count(duckdb: &str, node: &TestNode, location: &str) -> String {
    let sql = format!(
        "INSTALL iceberg; LOAD iceberg; \
         CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', \
         ENDPOINT '127.0.0.1:{}', URL_STYLE 'path', USE_SSL false); \
         SELECT count(*) AS records FROM iceberg_scan('{location}');",
        node.s3_port
    );

    let output = Command::new(duckdb)
        .args(["-json", "-c", &sql])
        .output()
        .await
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn metric(text: &str, series: &str) -> u64 {
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("no series {series}"))
}

#[tokio::test]
async fn the_console_adds_a_table_and_duckdb_reads_it() {
    let (Some(env), Some(duckdb)) = (test_env().await, mink_conformance::service("TEST_DUCKDB"))
    else {
        return;
    };
    let topic = topic_with_records(&env.bootstrap, 2).await;
    let node = start(&env, &Role::ALL, "").await;
    let client = reqwest::Client::new();

    let added = client
        .post(format!("{}/admin/tables", node.admin))
        .json(&json!({ "topic": topic }))
        .send()
        .await
        .unwrap();
    let status = added.status();
    let table = wait_for_records(&node, &topic, 9).await;
    let location = table["metadata-location"].as_str().unwrap();
    let stdout = duckdb_count(&duckdb, &node, location).await;
    let listed: Value = client
        .get(format!("{}/admin/tables?prefix={topic}", node.admin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    node.shutdown().await;

    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert!(stdout.contains(r#""records":9"#), "{stdout}");
    assert_eq!(listed["items"][0]["state"], "indexing");
}

#[tokio::test]
async fn metrics_move_with_the_work_they_count() {
    let Some(env) = test_env().await else {
        return;
    };
    let topic = topic_with_records(&env.bootstrap, 2).await;
    let node = start_node(&env, &topic, &Role::ALL, "").await;
    wait_for_committed(&node, &topic, 9).await;
    let client = reqwest::Client::new();

    load_table(&node, &topic).await.unwrap();
    let files: Value = client
        .get(format!("{}/admin/tables/{topic}/files", node.admin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let name = files["items"][0]["name"].as_str().unwrap();
    let url = format!(
        "http://127.0.0.1:{}/mink/warehouse/{topic}/data/{name}",
        node.s3_port
    );
    for _ in 0..2 {
        let read = client.get(&url).send().await.unwrap();
        assert!(read.status().is_success(), "{}", read.status());
    }
    let text = client
        .get(format!("{}/metrics", node.admin))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    node.shutdown().await;

    let indexed = |name: &str| {
        metric(&text, &format!(r#"{name}{{source="live"}}"#))
            + metric(&text, &format!(r#"{name}{{source="backfill"}}"#))
    };
    assert_eq!(indexed("mink_indexed_records_total"), 9);
    assert!(indexed("mink_files_written_total") >= 2);
    assert!(
        metric(
            &text,
            r#"mink_kafka_requests_total{request="fetch",result="ok"}"#
        ) >= 1
    );
    assert!(metric(&text, r#"mink_commits_total{result="ok"}"#) >= 1);
    assert!(metric(&text, "mink_commit_duration_seconds_count") >= 1);
    assert!(
        metric(
            &text,
            r#"mink_catalog_requests_total{route="load",status="2xx"}"#
        ) >= 1
    );
    assert_eq!(
        metric(
            &text,
            r#"mink_s3_requests_total{method="get",object="data",status="2xx"}"#
        ),
        2
    );
    assert!(metric(&text, "mink_s3_sent_bytes_total") > 0);
    assert_eq!(
        metric(&text, r#"nestor_blocks_miss_total{namespace="files"}"#),
        1
    );
    assert_eq!(
        metric(&text, r#"nestor_blocks_hit_total{namespace="files"}"#),
        1
    );
    assert_eq!(metric(&text, r#"mink_builds_total{result="ok"}"#), 1);
    assert!(metric(&text, "mink_cache_bytes") > 0);
    assert!(metric(&text, r#"mink_leases{scope="partition"}"#) >= 1);
}

#[tokio::test]
async fn duckdb_reads_a_mink_table() {
    let (Some(env), Some(duckdb)) = (test_env().await, mink_conformance::service("TEST_DUCKDB"))
    else {
        return;
    };
    let bootstrap = env.bootstrap.clone();
    let topic = topic_with_records(&bootstrap, 2).await;
    let node = start_node(&env, &topic, &Role::ALL, "").await;
    wait_for_records(&node, &topic, 9).await;

    mink_conformance::produce_values(&bootstrap, &topic, 1, &["j", "k"]);
    let table = load_table(&node, &topic).await.unwrap();
    let location = table["metadata-location"].as_str().unwrap();

    let stdout = duckdb_count(&duckdb, &node, location).await;

    assert!(stdout.contains(r#""records":11"#), "{stdout}");
    assert_eq!(committed(&bootstrap, &topic, 2).await, 11);
    node.shutdown().await;
}

#[tokio::test]
async fn pyiceberg_reads_every_committed_record() {
    let (Some(env), Some(python)) = (
        test_env().await,
        mink_conformance::service("TEST_PYICEBERG"),
    ) else {
        return;
    };
    let bootstrap = env.bootstrap.clone();
    let topic = topic_with_records(&bootstrap, 2).await;
    let node = start_node(&env, &topic, &Role::ALL, "").await;
    wait_for_records(&node, &topic, 9).await;

    mink_conformance::produce_values(&bootstrap, &topic, 0, &["j", "k", "l"]);
    let script = format!(
        "from pyiceberg.catalog.rest import RestCatalog\n\
         catalog = RestCatalog('mink', uri='{}', **{{'s3.access-key-id': 'mink', 's3.secret-access-key': 'mink'}})\n\
         table = catalog.load_table(('kafka', '{topic}'))\n\
         print(table.scan().to_arrow().num_rows)\n",
        node.catalog
    );
    let output = Command::new(&python)
        .args(["-c", &script])
        .output()
        .await
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "12");
    node.shutdown().await;
}

#[tokio::test]
async fn indexed_files_hold_every_committed_record_once() {
    let Some(env) = test_env().await else {
        return;
    };
    let bootstrap = env.bootstrap.clone();
    let topic = topic_with_records(&bootstrap, 3).await;
    let first = start_node(&env, &topic, &Role::ALL, "").await;
    let second = start_node(&env, &topic, &[Role::Indexer], "").await;

    wait_for_records(&first, &topic, 9).await;
    first.shutdown().await;
    mink_conformance::produce_values(&bootstrap, &topic, 1, &["l", "m"]);
    let remaining = start_node(&env, &topic, &[Role::Catalog, Role::S3], "").await;
    let table = wait_for_records(&remaining, &topic, 11).await;

    let summary = &table["metadata"]["snapshots"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()["summary"];
    assert_eq!(summary["total-records"], "11");
    assert_eq!(committed(&bootstrap, &topic, 3).await, 11);
    second.shutdown().await;
    remaining.shutdown().await;
}

async fn produce_version(env: &TestEnv, topic: &str, schema: Value, payload: &[u8]) -> i32 {
    let subject = format!("{topic}-value");
    let body = json!({ "schema": schema.to_string() });
    let registry = env.registry.as_ref().unwrap();

    let id = mink_conformance::register_schema(registry, &subject, &body).await;
    mink_conformance::produce_bytes(
        &env.bootstrap,
        topic,
        0,
        &[&mink_conformance::prefixed(id, payload)],
    );
    id
}

#[tokio::test]
async fn engines_read_typed_columns_across_schema_versions() {
    let (Some(env), Some(duckdb), Some(interpreter), Some(trino_url), Some(spark)) = (
        test_env().await,
        mink_conformance::service("TEST_DUCKDB"),
        mink_conformance::service("TEST_PYICEBERG"),
        mink_conformance::service("TEST_TRINO"),
        mink_conformance::service("TEST_SPARK"),
    ) else {
        return;
    };
    let Some(registry) = env.registry.clone() else {
        return;
    };
    let topic = format!("typed-{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 1).await;
    let node = start_node(&env, &topic, &Role::ALL, "").await;
    let id = json!({"name": "id", "type": "long"});
    let note = json!({"name": "note", "type": ["null", "string"], "default": null});
    let amount = json!({"name": "amount", "type": ["null", "double"], "default": null});

    let first = produce_version(
        &env,
        &topic,
        json!({"type": "record", "name": "Order", "fields": [id]}),
        &[2],
    )
    .await;
    wait_for_records(&node, &topic, 1).await;
    let second = json!({"type": "record", "name": "Order", "fields": [id, note]});
    produce_version(&env, &topic, second, &[4, 2, 2, b'b']).await;
    wait_for_records(&node, &topic, 2).await;
    let third = json!({"type": "record", "name": "Order", "fields": [id, note, amount]});
    let third_payload: Vec<u8> = [6, 2, 2, b'c', 2]
        .into_iter()
        .chain(2.5f64.to_le_bytes())
        .collect();
    produce_version(&env, &topic, third, &third_payload).await;
    mink_conformance::produce_values(&env.bootstrap, &topic, 0, &["not avro"]);
    let guid = mink_conformance::schema_guid(&registry, first).await;
    let header: Vec<u8> = [1].into_iter().chain(guid.into_bytes()).collect();
    mink_conformance::produce_with_header(
        &env.bootstrap,
        &topic,
        0,
        &[8],
        ("__value_schema_id", &header),
    );
    let table = wait_for_records(&node, &topic, 5).await;

    assert_eq!(table["metadata"]["current-schema-id"], 3);
    let location = table["metadata-location"].as_str().unwrap();
    let sql = format!(
        "INSTALL iceberg; LOAD iceberg; \
         CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', \
         ENDPOINT '127.0.0.1:{}', URL_STYLE 'path', USE_SSL false); \
         SELECT id, note, amount, _kafka_value IS NOT NULL AS raw FROM iceberg_scan('{location}') ORDER BY _kafka_offset;",
        node.s3_port
    );
    let duckdb = Command::new(&duckdb)
        .args(["-json", "-c", &sql])
        .output()
        .await
        .unwrap();

    let script = format!(
        "import json\n\
         from pyiceberg.catalog.rest import RestCatalog\n\
         catalog = RestCatalog('mink', uri='{}', **{{'s3.access-key-id': 'mink', 's3.secret-access-key': 'mink'}})\n\
         rows = catalog.load_table(('kafka', '{topic}')).scan().to_arrow().sort_by('_kafka_offset')\n\
         print(json.dumps(rows.select(['id', 'note', 'amount']).to_pylist()))\n",
        node.catalog
    );
    let pyiceberg = Command::new(&interpreter)
        .args(["-c", &script])
        .output()
        .await
        .unwrap();

    let expected = json!([
        {"id": 1, "note": null, "amount": null, "raw": false},
        {"id": 2, "note": "b", "amount": null, "raw": false},
        {"id": 3, "note": "c", "amount": 2.5, "raw": false},
        {"id": null, "note": null, "amount": null, "raw": true},
        {"id": 4, "note": null, "amount": null, "raw": false},
    ]);
    let read = serde_json::Deserializer::from_slice(&duckdb.stdout)
        .into_iter::<Value>()
        .filter_map(Result::ok)
        .last()
        .unwrap_or_else(|| panic!("{}", String::from_utf8_lossy(&duckdb.stderr)));
    assert_eq!(read, expected);
    let scanned: Value = serde_json::from_slice(&pyiceberg.stdout).unwrap_or_else(|_| {
        let (stdout, stderr) = (
            String::from_utf8_lossy(&pyiceberg.stdout),
            String::from_utf8_lossy(&pyiceberg.stderr),
        );

        panic!("pyiceberg wrote {stdout:?} and {stderr:?}")
    });
    let without_raw: Vec<Value> = expected
        .as_array()
        .unwrap()
        .iter()
        .map(|row| json!({"id": row["id"], "note": row["note"], "amount": row["amount"]}))
        .collect();
    assert_eq!(scanned, Value::Array(without_raw));

    let catalog = format!("mink_{}", Uuid::new_v4().simple());
    let s3 = format!("http://{}:{}", env.host, node.s3_port);
    let select = |table: String| {
        format!(
            "SELECT id, note, amount, _kafka_value IS NOT NULL AS raw FROM {catalog}.kafka.{table} ORDER BY _kafka_offset"
        )
    };

    trino(
        &trino_url,
        &format!(
            "CREATE CATALOG {catalog} USING iceberg WITH (\
             \"iceberg.catalog.type\" = 'rest', \"iceberg.rest-catalog.uri\" = '{}', \
             \"fs.native-s3.enabled\" = 'true', \"s3.endpoint\" = '{s3}', \"s3.path-style-access\" = 'true', \
             \"s3.region\" = 'us-east-1', \"s3.aws-access-key\" = 'mink', \"s3.aws-secret-key\" = 'mink')",
            node.catalog
        ),
    )
    .await;
    let trino_rows = trino(&trino_url, &select(format!("\"{topic}\""))).await;
    trino(&trino_url, &format!("DROP CATALOG {catalog}")).await;

    let spark_script = format!(
        "import json\n\
         from pyspark.sql import SparkSession\n\
         spark = SparkSession.builder.remote('{spark}').getOrCreate()\n\
         settings = {{'': 'org.apache.iceberg.spark.SparkCatalog', '.type': 'rest', '.uri': '{}', \
         '.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO', '.s3.endpoint': '{s3}', '.s3.path-style-access': 'true', \
         '.s3.access-key-id': 'mink', '.s3.secret-access-key': 'mink'}}\n\
         for key, value in settings.items():\n    spark.conf.set('spark.sql.catalog.{catalog}' + key, value)\n\
         rows = spark.sql(\"{}\").collect()\n\
         print(json.dumps([row.asDict() for row in rows]))\n",
        node.catalog,
        select(format!("`{topic}`"))
    );
    let spark_rows = python(&interpreter, &spark_script).await;

    let as_rows = |rows: Vec<Value>| -> Value {
        rows.into_iter()
            .map(|row| json!({"id": row[0], "note": row[1], "amount": row[2], "raw": row[3]}))
            .collect()
    };
    assert_eq!(as_rows(trino_rows), expected);
    assert_eq!(spark_rows, expected);
    node.shutdown().await;
}

#[tokio::test]
async fn engines_read_only_the_queried_hour() {
    let (Some(env), Some(interpreter)) = (
        test_env().await,
        mink_conformance::service("TEST_PYICEBERG"),
    ) else {
        return;
    };
    let topic = format!("hours-{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 1).await;
    let node = start_node(&env, &topic, &Role::ALL, r#"assembly_delay = "100ms""#).await;
    let first_hour = mink_core::now_ms() / 3_600_000 - 4;

    for hour in first_hour..first_hour + 3 {
        for batch in 0..3 {
            let timestamp = hour * 3_600_000 + batch * 1000;

            mink_conformance::produce_at(&env.bootstrap, &topic, 0, &["a", "b"], timestamp);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    let table = wait_for(&node, &topic, |table| {
        summary(table)["total-records"] == "18" && summary(table)["total-data-files"] == "3"
    })
    .await;
    mink_conformance::delete_records(&env.bootstrap, &topic, 0, 12).await;

    let last_hour = first_hour + 2;
    let location = table["metadata-location"].as_str().unwrap();
    let script = format!(
        "import datetime, json\n\
         import duckdb\n\
         from pyiceberg.expressions import GreaterThanOrEqual\n\
         from pyiceberg.io.pyarrow import PyArrowFileIO\n\
         from pyiceberg.table import StaticTable\n\
         mink = {{'s3.endpoint': 'http://127.0.0.1:{port}', 's3.access-key-id': 'mink', 's3.secret-access-key': 'mink', 's3.region': 'us-east-1'}}\n\
         bucket = PyArrowFileIO({{'s3.endpoint': '{s3}', 's3.access-key-id': 'mink', 's3.secret-access-key': 'minkminkmink', 's3.region': 'us-east-1'}})\n\
         table = StaticTable.from_metadata('{location}', mink)\n\
         for manifest in table.current_snapshot().manifests(table.io):\n\
         \x20   if int.from_bytes(manifest.partitions[1].lower_bound, 'little', signed=True) != {last_hour}:\n\
         \x20       bucket.delete(manifest.manifest_path)\n\
         start = datetime.datetime.fromtimestamp({last_hour} * 3600, datetime.timezone.utc)\n\
         scanned = table.scan(row_filter=GreaterThanOrEqual('_kafka_timestamp', start.isoformat())).to_arrow().num_rows\n\
         connection = duckdb.connect()\n\
         connection.execute(\"INSTALL iceberg; LOAD iceberg; CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', ENDPOINT '127.0.0.1:{port}', URL_STYLE 'path', USE_SSL false)\")\n\
         read = connection.execute(\"SELECT count(*) FROM iceberg_scan('{location}') WHERE _kafka_timestamp >= ?\", [start]).fetchone()[0]\n\
         print(json.dumps({{'pyiceberg': scanned, 'duckdb': read}}))\n",
        port = node.s3_port,
        s3 = env.s3,
    );

    let read = python(&interpreter, &script).await;

    assert_eq!(read, json!({"pyiceberg": 6, "duckdb": 6}));
    node.shutdown().await;
}

#[tokio::test]
async fn pyiceberg_reads_the_statistics_file() {
    let (Some(env), Some(interpreter)) = (
        test_env().await,
        mink_conformance::service("TEST_PYICEBERG"),
    ) else {
        return;
    };
    let topic = format!("statistics-{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 1).await;
    let node = start_node(&env, &topic, &Role::ALL, r#"statistics_interval = "200ms""#).await;

    mink_conformance::produce_values(&env.bootstrap, &topic, 0, &["a", "b", "c", "a"]);
    wait_for(&node, &topic, |table| {
        let statistics = &table["metadata"]["statistics"][0]["blob-metadata"];
        let offsets = statistics
            .as_array()
            .into_iter()
            .flatten()
            .find(|blob| blob["fields"] == json!([2]));

        offsets.is_some_and(|blob| blob["properties"]["ndv"] == "4")
    })
    .await;

    let script = format!(
        "import json\n\
         import datasketches\n\
         from pyiceberg.catalog.rest import RestCatalog\n\
         catalog = RestCatalog('mink', uri='{catalog}', **{{'s3.access-key-id': 'mink', 's3.secret-access-key': 'mink'}})\n\
         table = catalog.load_table(('kafka', '{topic}'))\n\
         statistics = table.metadata.statistics[0]\n\
         data = table.io.new_input(statistics.statistics_path).open().read()\n\
         size = int.from_bytes(data[-12:-8], 'little')\n\
         footer = json.loads(data[-12 - size:-12])\n\
         sketches = {{}}\n\
         for blob in footer['blobs']:\n\
         \x20   sketch = datasketches.compact_theta_sketch.deserialize(data[blob['offset']:blob['offset'] + blob['length']])\n\
         \x20   sketches[blob['fields'][0]] = [round(sketch.get_estimate()), int(blob['properties']['ndv'])]\n\
         print(json.dumps(sketches))\n",
        catalog = node.catalog,
    );

    let sketches = python(&interpreter, &script).await;

    assert_eq!(sketches["2"], json!([4, 4]));
    assert_eq!(sketches["9"], json!([3, 3]));
    for (field, estimates) in sketches.as_object().unwrap() {
        assert_eq!(estimates[0], estimates[1], "field {field}");
    }
    node.shutdown().await;
}

async fn produce_round(bootstrap: &str, topic: &str, round: usize) {
    for partition in 0..3 {
        let values: Vec<String> = (0..10).map(|index| format!("r{round}-{index}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();

        mink_conformance::produce_values(bootstrap, topic, partition, &values);
    }
}

async fn holders(postgres: &str, topic: &str) -> Vec<Uuid> {
    let store = Store::connect(postgres).await.unwrap();
    let holders = store.leases().holders(topic).await.unwrap();

    store.close().await;
    holders.iter().map(LeaseHolder::holder).collect()
}

async fn live_members(postgres: &str) -> Vec<Uuid> {
    let store = Store::connect(postgres).await.unwrap();
    let live = store.members().live().await.unwrap();

    store.close().await;
    live.iter().map(Member::id).collect()
}

#[tokio::test]
async fn three_nodes_survive_a_crash_and_a_postgres_restart() {
    let (Some(env), Some(duckdb)) = (test_env().await, mink_conformance::service("TEST_DUCKDB"))
    else {
        return;
    };
    let topic = format!("cluster-{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 3).await;

    let crashed = start_node(&env, &topic, &Role::ALL, "").await;
    produce_round(&env.bootstrap, &topic, 0).await;
    wait_for_committed(&crashed, &topic, 30).await;
    let alone = holders(&env.postgres, &topic).await;

    let first = start_node(&env, &topic, &Role::ALL, "").await;
    let second = start_node(&env, &topic, &Role::ALL, "").await;
    crashed.kill();
    let killed_at = Instant::now();
    let after_kill = holders(&env.postgres, &topic).await;

    let deadline = killed_at + WAIT;
    let mut taken = holders(&env.postgres, &topic).await;
    while (taken.len() < 4 || taken.contains(&alone[0])) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        taken = holders(&env.postgres, &topic).await;
    }
    let taken_after = killed_at.elapsed();
    let live_holders: Vec<Uuid> = live_members(&env.postgres)
        .await
        .into_iter()
        .filter(|id| taken.contains(id))
        .collect();

    produce_round(&env.bootstrap, &topic, 1).await;
    let mut expected = 60;
    wait_for_committed(&first, &topic, expected).await;

    assert_eq!(alone.len(), 4);
    assert!(alone.iter().all(|holder| *holder == alone[0]), "{alone:?}");
    assert!(after_kill.contains(&alone[0]), "{after_kill:?}");
    assert_eq!(taken.len(), 4);
    assert!(!taken.contains(&alone[0]), "{taken:?}");
    assert!(taken_after >= Duration::from_secs(1), "{taken_after:?}");
    assert_eq!(live_holders.len(), 2, "{taken:?}");
    assert!(!live_members(&env.postgres).await.contains(&alone[0]));

    if let Ok(container) = std::env::var("TEST_POSTGRES_CONTAINER") {
        let restarted = Command::new("docker")
            .args(["restart", "--time", "5", &container])
            .output()
            .await
            .unwrap();
        assert!(
            restarted.status.success(),
            "{}",
            String::from_utf8_lossy(&restarted.stderr)
        );

        produce_round(&env.bootstrap, &topic, 2).await;
        expected = 90;
        wait_for_committed(&second, &topic, expected).await;

        let live = live_members(&env.postgres).await;
        assert!(
            live_holders
                .iter()
                .all(|remaining| live.contains(remaining)),
            "{live:?}"
        );
    }

    let table = load_table(&first, &topic).await.unwrap();
    let location = table["metadata-location"].as_str().unwrap();
    let sql = format!(
        "INSTALL iceberg; LOAD iceberg; \
         CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', \
         ENDPOINT '127.0.0.1:{}', URL_STYLE 'path', USE_SSL false); \
         SELECT count(*) AS records, count(DISTINCT (_kafka_partition, _kafka_offset)) AS distinct_records \
         FROM iceberg_scan('{location}');",
        first.s3_port
    );
    let output = Command::new(&duckdb)
        .args(["-json", "-c", &sql])
        .output()
        .await
        .unwrap();

    let read = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<Value>()
        .filter_map(Result::ok)
        .last()
        .unwrap_or_else(|| panic!("{}", String::from_utf8_lossy(&output.stderr)));
    assert_eq!(
        read,
        json!([{"records": expected, "distinct_records": expected}])
    );
    assert_eq!(committed(&env.bootstrap, &topic, 3).await, expected);
    first.shutdown().await;
    second.shutdown().await;
}
