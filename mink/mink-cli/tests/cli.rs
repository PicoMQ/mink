//! Runs the built `mink` binary against a live node and checks each subcommand's output.

use std::io::Write as _;
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;
use std::{fs, path};

use mink_runtime::{ServerConfig, start};
use serde_json::Value;
use tokio::time::Instant;

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(dir: &path::Path, port: u16) -> ServerConfig {
    ServerConfig {
        node_id: 1,
        cluster_id: "cli-test".into(),
        meta_url: format!("sqlite:{}", dir.join("meta.db").display()),
        storage_uri: format!("-2@file://{}", dir.join("objects").display()),
        wal_uri: None,
        data_dir: dir.join("data"),
        listen: format!("127.0.0.1:{port}").parse().unwrap(),
        advertise: format!("grpc://127.0.0.1:{port}"),
        lease_ttl: Duration::from_secs(2),
        default_bucket_count: 2,
        ..ServerConfig::default()
    }
}

struct Bin {
    bootstrap: String,
}

impl Bin {
    fn run(&self, args: &[&str], stdin: &str) -> String {
        let (code, out, err) = self.try_run(args, stdin);
        assert_eq!(code, 0, "mink {} failed:\n{err}", args.join(" "));
        out
    }

    fn try_run(&self, args: &[&str], stdin: &str) -> (i32, String, String) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mink"))
            .arg("--bootstrap")
            .arg(&self.bootstrap)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8(output.stdout).unwrap(),
            String::from_utf8(output.stderr).unwrap(),
        )
    }

    fn text(&self, args: &[&str]) -> String {
        self.run(args, "")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn json(&self, args: &[&str]) -> Value {
        let mut full = vec!["--json"];
        full.extend_from_slice(args);
        let out = self.run(&full, "");
        serde_json::from_str(&out).unwrap_or_else(|e| panic!("{e}: {out}"))
    }
}

// The binary is a child process; the in-process node needs its own worker threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_administers_writes_and_reads() {
    let dir = tempfile::tempdir().unwrap();
    let port = reserve_port();
    let server = start(config(dir.path(), port)).await.unwrap();
    let coordinator = server.coordinator().clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !coordinator.is_leader() {
        assert!(Instant::now() < deadline, "no lease");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mink = Bin {
        bootstrap: format!("grpc://127.0.0.1:{port}"),
    };

    mink.run(&["db", "create", "shop", "--comment", "orders"], "");
    assert_eq!(mink.run(&["db", "list"], "").trim(), "shop");
    let (code, _, err) = mink.try_run(&["db", "create", "shop"], "");
    assert_ne!(code, 0);
    assert!(err.contains("exists"), "{err}");
    mink.run(&["db", "create", "shop", "--if-not-exists"], "");

    mink.run(
        &[
            "table",
            "create",
            "shop.orders",
            "-C",
            "id BIGINT NOT NULL",
            "-C",
            "region STRING NOT NULL",
            "-C",
            "amount DECIMAL(10,2)",
            "--primary-key",
            "id,region",
            "--partition-by",
            "region",
            "--bucket-count",
            "2",
            "--log-ttl",
            "2d",
            "--property",
            "owner=ops",
        ],
        "",
    );
    mink.run(
        &[
            "table",
            "create",
            "shop.events",
            "-C",
            "id BIGINT NOT NULL",
            "-C",
            "note STRING",
            "--bucket-key",
            "id",
        ],
        "",
    );
    assert_eq!(
        mink.run(&["table", "list", "shop"], "").trim(),
        "events\norders"
    );

    let described = mink.json(&["table", "describe", "shop.orders"]);
    assert_eq!(
        described["descriptor"]["partition_keys"],
        serde_json::json!(["region"])
    );
    assert_eq!(
        described["descriptor"]["options"]["log_ttl_ms"],
        2 * 24 * 3600 * 1000
    );
    assert_eq!(described["descriptor"]["custom"]["owner"], "ops");
    let text = mink.text(&["table", "describe", "shop.orders"]);
    assert!(text.contains("primary key id, region"), "{text}");
    assert!(text.contains("amount DECIMAL(10, 2)"), "{text}");
    assert!(text.contains("log ttl 2days"), "{text}");

    let file = dir.path().join("orders.json");
    fs::write(&file, described["descriptor"].to_string()).unwrap();
    mink.run(
        &[
            "table",
            "create",
            "shop.orders_copy",
            "--descriptor",
            file.to_str().unwrap(),
        ],
        "",
    );
    mink.run(&["table", "drop", "shop.orders_copy"], "");

    mink.run(&["partition", "create", "shop.orders", "region=eu"], "");
    let partitions = mink.json(&["partition", "list", "shop.orders"]);
    assert_eq!(partitions.as_array().unwrap().len(), 1);

    let wrote = mink.run(
        &["write", "shop.orders"],
        r#"{"id": 1, "region": "us", "amount": "10.50"}
{"id": 2, "region": "eu", "amount": "3.00"}
{"id": 1, "region": "us", "amount": "11.00"}
"#,
    );
    assert!(wrote.starts_with("wrote 3 rows to shop.orders"), "{wrote}");
    assert_eq!(
        mink.json(&["partition", "list", "shop.orders"])
            .as_array()
            .unwrap()
            .len(),
        2
    );
    mink.run(
        &["write", "shop.events"],
        "{\"id\": 7, \"note\": \"a\"}\n{\"id\": 8, \"note\": \"b\"}\n{\"id\": 9}\n",
    );

    let snapshot = mink.run(&["read", "shop.orders", "--mode", "snapshot"], "");
    let mut rows: Vec<Value> = snapshot
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    rows.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["amount"].as_f64(),
        Some(11.0),
        "decimals print as numbers"
    );
    assert_eq!(rows[1]["region"], "eu");

    let changelog = mink.run(&["read", "shop.orders", "--meta", "--partition", "us"], "");
    let changes: Vec<String> = changelog
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["__change"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(changes, ["+I", "-U", "+U"]);

    let events = mink.run(&["read", "shop.events", "-n", "2"], "");
    assert_eq!(events.lines().count(), 2);
    let all_events = mink.run(&["read", "shop.events"], "");
    assert_eq!(all_events.lines().count(), 3);
    assert!(
        all_events.contains("{\"id\":9}"),
        "nulls are omitted: {all_events}"
    );

    mink.run(
        &["write", "shop.orders", "--delete"],
        "{\"id\": 2, \"region\": \"eu\"}\n",
    );
    mink.run(
        &["write", "shop.orders", "--columns", "id,region,amount"],
        "{\"id\": 1, \"region\": \"us\", \"amount\": \"12.00\"}\n",
    );
    let snapshot = mink.run(&["read", "shop.orders", "--mode", "snapshot"], "");
    assert_eq!(snapshot.lines().count(), 1);
    assert!(snapshot.contains("\"amount\":12.0"), "{snapshot}");

    let offsets = mink.json(&["table", "offsets", "shop.events"]);
    let total: i64 = offsets
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["offset"].as_i64().unwrap())
        .sum();
    assert_eq!(total, 3);
    let earliest = mink.json(&["table", "offsets", "shop.events", "--at", "earliest"]);
    assert!(
        earliest
            .as_array()
            .unwrap()
            .iter()
            .all(|o| o["offset"] == 0)
    );
    let by_time = mink.json(&["table", "offsets", "shop.events", "--at", "0"]);
    assert!(by_time.as_array().unwrap().iter().all(|o| o["offset"] == 0));

    let altered = mink.run(
        &[
            "table",
            "alter",
            "shop.events",
            "--add-column",
            "source STRING",
            "--set",
            "team=data",
        ],
        "",
    );
    assert!(altered.contains("schema id is now 1"), "{altered}");
    let described = mink.json(&["table", "describe", "shop.events"]);
    assert_eq!(described["schemas"].as_array().unwrap().len(), 2);
    assert_eq!(described["descriptor"]["custom"]["team"], "data");
    let (code, _, err) = mink.try_run(
        &[
            "table",
            "alter",
            "shop.events",
            "--set",
            "table.log.format=indexed",
        ],
        "",
    );
    assert_ne!(code, 0, "table.log.format cannot be altered");
    assert!(!err.is_empty());

    let kv = mink.run(
        &[
            "table",
            "kv-snapshot",
            "shop.orders",
            "--bucket",
            "0",
            "--partition",
            "us",
        ],
        "",
    );
    assert!(kv.contains("no KV snapshot yet"), "{kv}");
    let lake = mink.run(&["table", "lake-snapshot", "shop.orders"], "");
    assert!(lake.contains("no lake snapshot"), "{lake}");

    let none = mink.run(&["producer-offsets", "get", "job-1"], "");
    assert!(none.contains("nothing registered"), "{none}");

    let cluster = mink.json(&["cluster", "describe"]);
    assert_eq!(cluster["tables"], 2);
    assert_eq!(cluster["nodes"][0]["live"], true);
    assert_eq!(cluster["coordinator"]["node_id"], 1);
    let text = mink.text(&["cluster", "describe"]);
    assert!(text.contains("node address live leading"), "{text}");
    let health = mink.text(&["cluster", "health"]);
    assert!(health.contains("registered yes"), "{health}");
    let config = mink.run(&["cluster", "config"], "");
    assert!(config.contains("cluster_id = cli-test"), "{config}");
    assert!(config.contains("default_bucket_count = 2"), "{config}");
    let stats = mink.json(&["cluster", "stats"]);
    assert_eq!(stats["node_id"], 1);
    assert!(stats["buckets"].as_array().unwrap().len() >= 4, "{stats}");
    let stats = mink.run(&["cluster", "stats", "--all"], "");
    assert!(stats.contains("== node 1"), "{stats}");
    assert!(stats.contains("shop.orders"), "{stats}");
    let rebalance = mink.run(&["cluster", "rebalance"], "");
    assert!(rebalance.contains("nothing moved"), "{rebalance}");

    mink.run(&["table", "drop", "shop.events"], "");
    mink.run(&["partition", "drop", "shop.orders", "region=eu"], "");
    mink.run(&["db", "drop", "shop", "--cascade"], "");
    assert_eq!(mink.json(&["db", "list"]), serde_json::json!([]));

    let (code, _, err) = Bin {
        bootstrap: "grpc://127.0.0.1:1".into(),
    }
    .try_run(&["db", "list"], "");
    assert_ne!(code, 0);
    assert!(!err.contains("panicked"), "{err}");
    server.shutdown().await;
}
