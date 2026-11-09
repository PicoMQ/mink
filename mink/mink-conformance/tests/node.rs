use std::fs;
use std::net::TcpListener;
use std::ops::Range;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use mink_core::{Member, Offsets, Partition, Scope, View};
use mink_iceberg::FIRST_TAIL_SNAPSHOT_ID;
use mink_kafka::{Cluster, Config as KafkaConfig};
use mink_metrics::Metrics;
use mink_server::{
    Attachment, CatalogTable, Config, Error, Node, Partitioning, Role, TableSource, TierSchedule,
};
use mink_store::{LeaseHolder, Store};
use serde_json::{Map, Value, json};
use tokio::process::Command;
use tokio::time::Instant;
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(60);
const LOCALHOST: &str = "127.0.0.1";
const PORTS: Range<u16> = 20_000..32_000;
const TIER_INTERVAL: Duration = Duration::from_secs(2);
const TARGET_FILE_BYTES: u64 = 524_288;

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

async fn load_table(node: &TestNode, view: View, topic: &str) -> Option<Value> {
    let url = format!(
        "{}/v1/namespaces/{}/tables/{topic}",
        node.catalog,
        view.name()
    );
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
        if let Some(table) = load_table(node, View::Merged, topic).await
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
        committed_snapshot(table)["summary"]["total-records"] == wanted.as_str()
    })
    .await
}

fn committed_snapshot(table: &Value) -> &Value {
    let snapshots = table["metadata"]["snapshots"].as_array();

    snapshots
        .into_iter()
        .flatten()
        .rev()
        .find(|snapshot| {
            snapshot["snapshot-id"]
                .as_i64()
                .is_some_and(|id| id < FIRST_TAIL_SNAPSHOT_ID)
        })
        .unwrap_or(&Value::Null)
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

fn records_sql(table: &str) -> String {
    format!(
        "SELECT (SELECT count(*) FROM {table}) AS records, \
         (SELECT count(*) FROM (SELECT DISTINCT _kafka_partition, _kafka_offset FROM {table}) AS pairs) AS distinct_records"
    )
}

fn records(count: u64) -> Value {
    json!([{"records": count, "distinct_records": count}])
}

async fn duckdb_records(duckdb: &str, node: &TestNode, location: &str) -> Value {
    let sql = format!(
        "INSTALL iceberg; LOAD iceberg; \
         CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', \
         ENDPOINT '127.0.0.1:{}', URL_STYLE 'path', USE_SSL false); {};",
        node.s3_port,
        records_sql(&format!("iceberg_scan('{location}')"))
    );
    let output = Command::new(duckdb)
        .args(["-json", "-c", &sql])
        .output()
        .await
        .unwrap();

    serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<Value>()
        .filter_map(Result::ok)
        .last()
        .unwrap_or_else(|| panic!("{}", String::from_utf8_lossy(&output.stderr)))
}

fn mink_catalog(node: &TestNode) -> String {
    format!(
        "from pyiceberg.catalog.rest import RestCatalog\n\
         catalog = RestCatalog('mink', uri='{}', **{{'s3.access-key-id': 'mink', 's3.secret-access-key': 'mink'}})\n",
        node.catalog
    )
}

fn fixture_catalog(uri: &str, s3: &str) -> String {
    format!(
        "from pyiceberg.catalog.rest import RestCatalog\n\
         catalog = RestCatalog('fixture', uri='{uri}', **{{'s3.endpoint': '{s3}', 's3.access-key-id': 'mink', \
         's3.secret-access-key': 'minkminkmink', 's3.region': 'us-east-1'}})\n"
    )
}

async fn pyiceberg_records(
    interpreter: &str,
    catalog: &str,
    namespace: &str,
    topic: &str,
) -> Value {
    let script = format!(
        "import json\n\
         {catalog}\
         rows = catalog.load_table(('{namespace}', '{topic}')).scan(selected_fields=('_kafka_partition', '_kafka_offset')).to_arrow()\n\
         pairs = set(zip(rows['_kafka_partition'].to_pylist(), rows['_kafka_offset'].to_pylist()))\n\
         print(json.dumps([{{'records': rows.num_rows, 'distinct_records': len(pairs)}}]))\n"
    );

    python(interpreter, &script).await
}

async fn trino_catalog(trino_url: &str, node: &TestNode, host: &str) -> String {
    let catalog = format!("mink_{}", Uuid::new_v4().simple());

    trino(
        trino_url,
        &format!(
            "CREATE CATALOG {catalog} USING iceberg WITH (\
             \"iceberg.catalog.type\" = 'rest', \"iceberg.rest-catalog.uri\" = '{}', \
             \"fs.native-s3.enabled\" = 'true', \"s3.endpoint\" = 'http://{host}:{}', \"s3.path-style-access\" = 'true', \
             \"s3.region\" = 'us-east-1', \"s3.aws-access-key\" = 'mink', \"s3.aws-secret-key\" = 'mink')",
            node.catalog, node.s3_port
        ),
    )
    .await;
    catalog
}

fn spark_catalog(uri: &str, s3: &str, secret: &str) -> Value {
    json!({
        "": "org.apache.iceberg.spark.SparkCatalog",
        ".type": "rest",
        ".uri": uri,
        ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        ".s3.endpoint": s3,
        ".s3.path-style-access": "true",
        ".s3.access-key-id": "mink",
        ".s3.secret-access-key": secret,
        ".client.region": "us-east-1",
    })
}

async fn spark_sql(
    interpreter: &str,
    spark: &str,
    catalogs: &[(&str, Value)],
    statements: &[String],
) -> Value {
    let catalogs: Map<String, Value> = catalogs
        .iter()
        .map(|(name, settings)| (name.to_string(), settings.clone()))
        .collect();
    let script = format!(
        "import json\n\
         from pyspark.sql import SparkSession\n\
         spark = SparkSession.builder.remote('{spark}').getOrCreate()\n\
         for catalog, settings in json.loads({catalogs}).items():\n\
         \x20   for key, value in settings.items():\n\
         \x20       spark.conf.set('spark.sql.catalog.' + catalog + key, value)\n\
         rows = []\n\
         for statement in json.loads({statements}):\n\
         \x20   rows = [row.asDict() for row in spark.sql(statement).collect()]\n\
         print(json.dumps(rows))\n",
        catalogs = serde_json::to_string(&Value::Object(catalogs).to_string()).unwrap(),
        statements = serde_json::to_string(&json!(statements).to_string()).unwrap(),
    );

    python(interpreter, &script).await
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

    load_table(&node, View::Merged, &topic).await.unwrap();
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
    assert!(metric(&text, r#"mink_leases{worker="indexer"}"#) >= 1);
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
    let table = load_table(&node, View::Merged, &topic).await.unwrap();
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
        "{}\
         table = catalog.load_table(('merged', '{topic}'))\n\
         print(table.scan().to_arrow().num_rows)\n",
        mink_catalog(&node)
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
         {}\
         rows = catalog.load_table(('merged', '{topic}')).scan().to_arrow().sort_by('_kafka_offset')\n\
         print(json.dumps(rows.select(['id', 'note', 'amount']).to_pylist()))\n",
        mink_catalog(&node)
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

    let catalog = trino_catalog(&trino_url, &node, &env.host).await;
    let s3 = format!("http://{}:{}", env.host, node.s3_port);
    let select = |table: String| {
        format!(
            "SELECT id, note, amount, _kafka_value IS NOT NULL AS raw FROM {catalog}.merged.{table} ORDER BY _kafka_offset"
        )
    };

    let trino_rows = trino(&trino_url, &select(format!("\"{topic}\""))).await;
    trino(&trino_url, &format!("DROP CATALOG {catalog}")).await;

    let spark_rows = spark_sql(
        &interpreter,
        &spark,
        &[(&catalog, spark_catalog(&node.catalog, &s3, "mink"))],
        &[select(format!("`{topic}`"))],
    )
    .await;

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

async fn admin(node: &TestNode, path: &str) -> Value {
    reqwest::get(format!("{}/admin/{path}", node.admin))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn noise(length: usize) -> String {
    (0..length / 32)
        .map(|_| Uuid::new_v4().simple().to_string())
        .collect()
}

#[tokio::test]
async fn a_footer_read_needs_no_build() {
    let Some(env) = test_env().await else {
        return;
    };
    let topic = format!("footer-{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 1).await;
    mink_conformance::produce_values(&env.bootstrap, &topic, 0, &["a", "b", "c"]);
    let node = start_node(&env, &topic, &Role::ALL, "").await;
    wait_for_committed(&node, &topic, 3).await;
    mink_conformance::delete_records(&env.bootstrap, &topic, 0, 3).await;

    let files = admin(&node, &format!("tables/{topic}/files")).await;
    let name = files["items"][0]["name"].as_str().unwrap();
    let url = format!(
        "http://127.0.0.1:{}/mink/warehouse/{topic}/data/{name}",
        node.s3_port
    );
    let client = reqwest::Client::new();
    let read = client
        .get(&url)
        .header("range", "bytes=-65536")
        .send()
        .await
        .unwrap();
    let status = read.status();
    let body = read.bytes().await.unwrap();
    let rows = client
        .get(&url)
        .header("range", "bytes=4-13")
        .send()
        .await
        .unwrap();
    node.shutdown().await;

    assert_eq!(status, reqwest::StatusCode::PARTIAL_CONTENT);
    assert!(!rows.status().is_success(), "{}", rows.status());
    assert_eq!(body.len(), 65_536);
    assert!(body.ends_with(b"PAR1"));
    assert!(body[..1024].iter().all(|byte| *byte == 0));
}

#[tokio::test]
async fn assembled_files_build_only_the_parts_a_query_reads() {
    let (Some(env), Some(interpreter), Some(trino_url)) = (
        test_env().await,
        mink_conformance::service("TEST_PYICEBERG"),
        mink_conformance::service("TEST_TRINO"),
    ) else {
        return;
    };
    let topic = format!("parts-{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 1).await;
    let node = start_node(&env, &topic, &Role::ALL, r#"assembly_delay = "100ms""#).await;
    let hour = mink_core::now_ms() / 3_600_000 - 2;

    for batch in 0..3 {
        let values: Vec<String> = (0..80).map(|_| noise(100_000)).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();

        mink_conformance::produce_at(
            &env.bootstrap,
            &topic,
            0,
            &values,
            hour * 3_600_000 + batch * 1000,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let table = wait_for(&node, &topic, |table| {
        summary(table)["total-records"] == "240" && summary(table)["total-data-files"] == "1"
    })
    .await;
    let files = admin(&node, &format!("tables/{topic}/files")).await;
    let name = files["items"][0]["name"].as_str().unwrap();
    let detail = admin(&node, &format!("tables/{topic}/files/{name}")).await;
    let spans = detail["spans"].as_array().unwrap();
    assert!(spans.len() >= 2, "{detail}");
    let second = spans[1]["startOffset"].as_i64().unwrap();
    mink_conformance::delete_records(&env.bootstrap, &topic, 0, second).await;

    let location = table["metadata-location"].as_str().unwrap();
    let script = format!(
        "import json\n\
         import duckdb\n\
         from pyiceberg.expressions import GreaterThanOrEqual\n\
         from pyiceberg.table import StaticTable\n\
         mink = {{'s3.endpoint': 'http://127.0.0.1:{port}', 's3.access-key-id': 'mink', 's3.secret-access-key': 'mink', 's3.region': 'us-east-1'}}\n\
         table = StaticTable.from_metadata('{location}', mink)\n\
         scanned = table.scan(row_filter=GreaterThanOrEqual('_kafka_offset', {second})).to_arrow().num_rows\n\
         connection = duckdb.connect()\n\
         connection.execute(\"INSTALL iceberg; LOAD iceberg; CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', ENDPOINT '127.0.0.1:{port}', URL_STYLE 'path', USE_SSL false)\")\n\
         read = connection.execute(\"SELECT count(*) FROM iceberg_scan('{location}') WHERE _kafka_offset >= {second}\").fetchone()[0]\n\
         print(json.dumps({{'pyiceberg': scanned, 'duckdb': read}}))\n",
        port = node.s3_port,
    );
    let read = python(&interpreter, &script).await;

    let catalog = trino_catalog(&trino_url, &node, &env.host).await;
    let counted = trino(
        &trino_url,
        &format!(
            "SELECT count(*) FROM {catalog}.merged.\"{topic}\" WHERE _kafka_offset >= {second}"
        ),
    )
    .await;
    trino(&trino_url, &format!("DROP CATALOG {catalog}")).await;
    node.shutdown().await;

    let expected = 240 - second;
    assert_eq!(read, json!({"pyiceberg": expected, "duckdb": expected}));
    assert_eq!(counted, [json!([expected])]);
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
         {catalog}\
         table = catalog.load_table(('merged', '{topic}'))\n\
         statistics = table.metadata.statistics[0]\n\
         data = table.io.new_input(statistics.statistics_path).open().read()\n\
         size = int.from_bytes(data[-12:-8], 'little')\n\
         footer = json.loads(data[-12 - size:-12])\n\
         sketches = {{}}\n\
         for blob in footer['blobs']:\n\
         \x20   sketch = datasketches.compact_theta_sketch.deserialize(data[blob['offset']:blob['offset'] + blob['length']])\n\
         \x20   sketches[blob['fields'][0]] = [round(sketch.get_estimate()), int(blob['properties']['ndv'])]\n\
         print(json.dumps(sketches))\n",
        catalog = mink_catalog(&node),
    );

    let sketches = python(&interpreter, &script).await;

    assert_eq!(sketches["2"], json!([4, 4]));
    assert_eq!(sketches["6"], json!([3, 3]));
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

    let table = load_table(&first, View::Merged, &topic).await.unwrap();
    let location = table["metadata-location"].as_str().unwrap();

    let read = duckdb_records(&duckdb, &first, location).await;
    assert_eq!(read, records(expected));
    assert_eq!(committed(&env.bootstrap, &topic, 3).await, expected);
    first.shutdown().await;
    second.shutdown().await;
}

fn external_catalogs(uri: &str) -> String {
    format!(r#"external_catalogs = {{ fixture = {{ uri = "{uri}" }} }}"#)
}

async fn attach(node: &TestNode, uri: &str, topic: &str, after: Duration, source: TableSource) {
    reqwest::Client::new()
        .post(format!("{uri}/v1/namespaces"))
        .json(&json!({ "namespace": ["mink"] }))
        .send()
        .await
        .unwrap();
    let attachment = Attachment {
        topic: topic.to_owned(),
        table: CatalogTable::new("fixture", "mink", topic),
        schedule: TierSchedule::new(after, TIER_INTERVAL).unwrap(),
        source,
    };

    mink_server::attach_external_table(&node.config, attachment)
        .await
        .unwrap();
}

fn new_table(properties: &[(&str, String)]) -> TableSource {
    TableSource::New {
        partitioning: Partitioning::Fields(vec!["day(_kafka_timestamp)".parse().unwrap()]),
        sort: vec!["_kafka_offset desc".parse().unwrap()],
        properties: properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect(),
        location: None,
    }
}

async fn external_metadata(uri: &str, topic: &str) -> Value {
    let table: Value = reqwest::get(format!("{uri}/v1/namespaces/mink/tables/{topic}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    table["metadata"].clone()
}

fn tiered(snapshot: &Value) -> i64 {
    snapshot["summary"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| {
            key.starts_with("mink.external.partition.") && key.ends_with(".end-offset")
        })
        .filter_map(|(_, offset)| offset.as_str()?.parse::<i64>().ok())
        .sum()
}

async fn wait_for_tiered(node: &TestNode, topic: &str, tiered_records: i64, records: u64) -> Value {
    let wanted = records.to_string();

    wait_for(node, topic, |table| {
        let snapshot = committed_snapshot(table);

        tiered(snapshot) == tiered_records
            && snapshot["summary"]["total-records"] == wanted.as_str()
    })
    .await
}

async fn tier_holders(postgres: &str, topic: &str) -> Vec<Uuid> {
    let store = Store::connect(postgres).await.unwrap();
    let holders = store.leases().holders(topic).await.unwrap();

    store.close().await;
    holders
        .iter()
        .filter(|holder| matches!(holder.scope(), Scope::Tier(_)))
        .map(LeaseHolder::holder)
        .collect()
}

#[tokio::test]
async fn tiered_records_read_once_through_every_engine() {
    let (Some(env), Some(uri), Some(duckdb), Some(interpreter), Some(trino_url), Some(spark)) = (
        test_env().await,
        mink_conformance::service("TEST_ICEBERG_REST"),
        mink_conformance::service("TEST_DUCKDB"),
        mink_conformance::service("TEST_PYICEBERG"),
        mink_conformance::service("TEST_TRINO"),
        mink_conformance::service("TEST_SPARK"),
    ) else {
        return;
    };
    let topic = format!("tiered_{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 2).await;
    let node = start_node(&env, &topic, &Role::ALL, &external_catalogs(&uri)).await;
    let old_ms = mink_core::now_ms() - 2 * 3_600_000;

    for partition in 0..2 {
        let values: Vec<String> = (0..30).map(|_| noise(100_000)).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();

        mink_conformance::produce_at(&env.bootstrap, &topic, partition, &values, old_ms);
    }
    wait_for_committed(&node, &topic, 60).await;
    for partition in 0..2 {
        mink_conformance::produce_values(
            &env.bootstrap,
            &topic,
            partition,
            &["a", "b", "c", "d", "e"],
        );
    }
    let target = [(
        "write.target-file-size-bytes",
        TARGET_FILE_BYTES.to_string(),
    )];
    attach(
        &node,
        &uri,
        &topic,
        Duration::from_secs(3600),
        new_table(&target),
    )
    .await;
    let table = wait_for_tiered(&node, &topic, 60, 70).await;

    let location = table["metadata-location"].as_str().unwrap();
    let duckdb_read = duckdb_records(&duckdb, &node, location).await;
    let pyiceberg_read =
        pyiceberg_records(&interpreter, &mink_catalog(&node), "merged", &topic).await;
    let catalog = trino_catalog(&trino_url, &node, &env.host).await;
    let trino_read = trino(
        &trino_url,
        &records_sql(&format!("{catalog}.merged.\"{topic}\"")),
    )
    .await;
    trino(&trino_url, &format!("DROP CATALOG {catalog}")).await;
    let s3 = format!("http://{}:{}", env.host, node.s3_port);
    let spark_read = spark_sql(
        &interpreter,
        &spark,
        &[(&catalog, spark_catalog(&node.catalog, &s3, "mink"))],
        &[records_sql(&format!("{catalog}.merged.{topic}"))],
    )
    .await;

    let fixture = fixture_catalog(&uri, &env.s3);
    let script = format!(
        "import json\n\
         import pyarrow.parquet as pq\n\
         {fixture}\
         table = catalog.load_table(('mink', '{topic}'))\n\
         files = []\n\
         for task in table.scan().plan_files():\n\
         \x20   parquet = pq.ParquetFile(table.io.new_input(task.file.file_path).open())\n\
         \x20   rows = parquet.read(columns=['_kafka_partition', '_kafka_offset'])\n\
         \x20   offsets = rows['_kafka_offset'].to_pylist()\n\
         \x20   files.append({{'partitions': sorted(set(rows['_kafka_partition'].to_pylist())), \
         'descending': offsets == sorted(offsets, reverse=True), 'bytes': task.file.file_size_in_bytes, \
         'codec': parquet.metadata.row_group(0).column(0).compression, 'day': task.file.partition[0]}})\n\
         print(json.dumps(files))\n"
    );
    let files = python(&interpreter, &script).await;
    let external = pyiceberg_records(&interpreter, &fixture, "mink", &topic).await;
    node.shutdown().await;

    assert_eq!(duckdb_read, records(70));
    assert_eq!(pyiceberg_read, records(70));
    assert_eq!(trino_read, [json!([70, 70])]);
    assert_eq!(spark_read, records(70));
    assert_eq!(external, records(60));
    let files = files.as_array().unwrap();
    for partition in 0..2 {
        let written = files
            .iter()
            .filter(|file| file["partitions"] == json!([partition]))
            .count();
        assert!(written >= 2, "{files:?}");
    }
    for file in files {
        assert_eq!(file["partitions"].as_array().unwrap().len(), 1, "{file}");
        assert_eq!(file["descending"], true, "{file}");
        assert_eq!(file["codec"], "ZSTD", "{file}");
        assert_eq!(file["day"], old_ms / 86_400_000, "{file}");
        assert!(
            file["bytes"].as_u64().unwrap() <= 2 * TARGET_FILE_BYTES,
            "{file}"
        );
    }
}

#[tokio::test]
async fn the_stream_and_lake_namespaces_split_the_merged_table() {
    let (Some(env), Some(uri), Some(duckdb), Some(interpreter)) = (
        test_env().await,
        mink_conformance::service("TEST_ICEBERG_REST"),
        mink_conformance::service("TEST_DUCKDB"),
        mink_conformance::service("TEST_PYICEBERG"),
    ) else {
        return;
    };
    let topic = format!("split_{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 2).await;
    let node = start_node(&env, &topic, &Role::ALL, &external_catalogs(&uri)).await;
    let old_ms = mink_core::now_ms() - 2 * 3_600_000;

    for partition in 0..2 {
        let values = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
        mink_conformance::produce_at(&env.bootstrap, &topic, partition, &values, old_ms);
    }
    wait_for_committed(&node, &topic, 20).await;
    for partition in 0..2 {
        mink_conformance::produce_values(&env.bootstrap, &topic, partition, &["k", "l", "m"]);
    }
    attach(
        &node,
        &uri,
        &topic,
        Duration::from_secs(3600),
        new_table(&[]),
    )
    .await;
    wait_for_tiered(&node, &topic, 20, 26).await;

    let catalog = mink_catalog(&node);
    let mut pyiceberg = Vec::new();
    let mut duckdb_reads = Vec::new();
    for view in View::ALL {
        pyiceberg.push(pyiceberg_records(&interpreter, &catalog, view.name(), &topic).await);
        let table = load_table(&node, view, &topic).await.unwrap();
        let location = table["metadata-location"].as_str().unwrap();
        duckdb_reads.push(duckdb_records(&duckdb, &node, location).await);
    }
    node.shutdown().await;

    let expected = [records(26), records(6), records(20)];
    assert_eq!(pyiceberg, expected);
    assert_eq!(duckdb_reads, expected);
}

#[tokio::test]
async fn external_changes_show_in_the_merged_table() {
    let (Some(env), Some(uri), Some(duckdb), Some(interpreter), Some(spark)) = (
        test_env().await,
        mink_conformance::service("TEST_ICEBERG_REST"),
        mink_conformance::service("TEST_DUCKDB"),
        mink_conformance::service("TEST_PYICEBERG"),
        mink_conformance::service("TEST_SPARK"),
    ) else {
        return;
    };
    let topic = format!("changed_{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 2).await;
    let settings = format!("{}\nretention_check = \"200ms\"", external_catalogs(&uri));
    let node = start_node(&env, &topic, &Role::ALL, &settings).await;
    for partition in 0..2 {
        let values = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

        mink_conformance::produce_values(&env.bootstrap, &topic, partition, &values);
    }
    attach(
        &node,
        &uri,
        &topic,
        Duration::from_millis(1),
        new_table(&[]),
    )
    .await;
    wait_for_tiered(&node, &topic, 20, 20).await;

    let fixture = format!("fixture_{}", Uuid::new_v4().simple());
    let table = format!("{fixture}.mink.{topic}");
    spark_sql(
        &interpreter,
        &spark,
        &[(&fixture, spark_catalog(&uri, &env.s3, "minkminkmink"))],
        &[
            format!("DELETE FROM {table} WHERE _kafka_partition = 0 AND _kafka_offset < 4"),
            format!(
                "CALL {fixture}.system.rewrite_data_files(table => '{table}', options => map('rewrite-all', 'true'))"
            ),
            format!("CALL {fixture}.system.rewrite_manifests('{table}')"),
        ],
    )
    .await;
    let changed = external_metadata(&uri, &topic).await;
    let snapshot_id = changed["current-snapshot-id"].to_string();
    let listed = wait_for(&node, &topic, |table| {
        committed_snapshot(table)["summary"]["mink.external.snapshot-id"] == snapshot_id.as_str()
    })
    .await;
    let after_changes = duckdb_records(
        &duckdb,
        &node,
        listed["metadata-location"].as_str().unwrap(),
    )
    .await;

    for partition in 0..2 {
        mink_conformance::produce_values(
            &env.bootstrap,
            &topic,
            partition,
            &["k", "l", "m", "n", "o"],
        );
    }
    wait_for_tiered(&node, &topic, 30, 26).await;
    for partition in 0..2 {
        mink_conformance::delete_records(&env.bootstrap, &topic, partition, 15).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let table = load_table(&node, View::Merged, &topic).await.unwrap();
    let after_kafka =
        duckdb_records(&duckdb, &node, table["metadata-location"].as_str().unwrap()).await;
    node.shutdown().await;

    assert_eq!(after_changes, records(16));
    assert_eq!(changed["properties"]["mink.partition.0.end-offset"], "10");
    assert_eq!(changed["properties"]["mink.partition.1.end-offset"], "10");
    assert_eq!(after_kafka, records(26));
}

#[tokio::test]
async fn a_tier_lease_moves_without_tiering_twice() {
    let (Some(env), Some(uri), Some(duckdb), Some(interpreter)) = (
        test_env().await,
        mink_conformance::service("TEST_ICEBERG_REST"),
        mink_conformance::service("TEST_DUCKDB"),
        mink_conformance::service("TEST_PYICEBERG"),
    ) else {
        return;
    };
    let topic = format!("moved_{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 3).await;
    let settings = external_catalogs(&uri);
    let first = start_node(&env, &topic, &Role::ALL, &settings).await;
    attach(
        &first,
        &uri,
        &topic,
        Duration::from_millis(1),
        new_table(&[]),
    )
    .await;
    let alone = live_members(&env.postgres).await;
    let second = start_node(&env, &topic, &Role::ALL, &settings).await;

    let deadline = Instant::now() + WAIT;
    let mut tiering = tier_holders(&env.postgres, &topic).await;
    while tiering.iter().all(|holder| alone.contains(holder)) {
        assert!(Instant::now() < deadline, "{tiering:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
        tiering = tier_holders(&env.postgres, &topic).await;
    }
    produce_round(&env.bootstrap, &topic, 0).await;
    wait_for_tiered(&first, &topic, 30, 30).await;
    second.kill();
    produce_round(&env.bootstrap, &topic, 1).await;
    let table = wait_for_tiered(&first, &topic, 60, 60).await;

    let location = table["metadata-location"].as_str().unwrap();
    let merged = duckdb_records(&duckdb, &first, location).await;
    let external = pyiceberg_records(
        &interpreter,
        &fixture_catalog(&uri, &env.s3),
        "mink",
        &topic,
    )
    .await;
    let remaining = tier_holders(&env.postgres, &topic).await;
    first.shutdown().await;

    assert_eq!(merged, records(60));
    assert_eq!(external, records(60));
    assert_eq!(remaining.len(), 3);
    assert!(
        remaining.iter().all(|holder| alone.contains(holder)),
        "{remaining:?}"
    );
}

#[tokio::test]
async fn detach_keeps_time_travel_and_reattaching_resumes() {
    let (Some(env), Some(uri), Some(duckdb), Some(interpreter), Some(trino_url)) = (
        test_env().await,
        mink_conformance::service("TEST_ICEBERG_REST"),
        mink_conformance::service("TEST_DUCKDB"),
        mink_conformance::service("TEST_PYICEBERG"),
        mink_conformance::service("TEST_TRINO"),
    ) else {
        return;
    };
    let topic = format!("detached_{}", Uuid::new_v4().simple());
    mink_conformance::create_topic(&env.bootstrap, &topic, 2).await;
    let node = start_node(&env, &topic, &Role::ALL, &external_catalogs(&uri)).await;
    for partition in 0..2 {
        let values = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

        mink_conformance::produce_values(&env.bootstrap, &topic, partition, &values);
    }
    attach(
        &node,
        &uri,
        &topic,
        Duration::from_millis(1),
        new_table(&[]),
    )
    .await;
    let table = wait_for_tiered(&node, &topic, 20, 20).await;
    let listed = committed_snapshot(&table)["snapshot-id"].as_i64().unwrap();

    mink_server::detach_external_table(&node.config, &topic)
        .await
        .unwrap();
    let detached = wait_for(&node, &topic, |table| {
        let summary = &committed_snapshot(table)["summary"];

        summary["mink.external.generation"].is_null() && summary["total-records"] == "20"
    })
    .await;
    let unlisted = duckdb_records(
        &duckdb,
        &node,
        detached["metadata-location"].as_str().unwrap(),
    )
    .await;
    let catalog = trino_catalog(&trino_url, &node, &env.host).await;
    let travelled = trino(
        &trino_url,
        &records_sql(&format!(
            "{catalog}.merged.\"{topic}\" FOR VERSION AS OF BIGINT '{listed}'"
        )),
    )
    .await;
    trino(&trino_url, &format!("DROP CATALOG {catalog}")).await;

    let properties = &external_metadata(&uri, &topic).await["properties"];
    let end_offsets = (0..2)
        .map(|partition| {
            let offset = &properties[format!("mink.partition.{partition}.end-offset")];

            (partition, offset.as_str().unwrap().parse().unwrap())
        })
        .collect();
    let existing = TableSource::Existing { end_offsets };
    attach(&node, &uri, &topic, Duration::from_millis(1), existing).await;
    for partition in 0..2 {
        mink_conformance::produce_values(
            &env.bootstrap,
            &topic,
            partition,
            &["k", "l", "m", "n", "o"],
        );
    }
    let resumed = wait_for_tiered(&node, &topic, 30, 30).await;
    let merged = duckdb_records(
        &duckdb,
        &node,
        resumed["metadata-location"].as_str().unwrap(),
    )
    .await;
    let external = pyiceberg_records(
        &interpreter,
        &fixture_catalog(&uri, &env.s3),
        "mink",
        &topic,
    )
    .await;
    node.shutdown().await;

    assert_eq!(unlisted, records(20));
    assert_eq!(travelled, [json!([20, 20])]);
    assert_eq!(
        committed_snapshot(&resumed)["summary"]["mink.external.generation"],
        "2"
    );
    assert_eq!(merged, records(30));
    assert_eq!(external, records(30));
}
