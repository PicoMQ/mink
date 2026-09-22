//! Chaos through the docker CLI: containers are addressed by their fixed compose names.

use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;
use tokio::time::Instant;

pub const POSTGRES: &str = "mink-postgres";
pub const RUSTFS: &str = "mink-rustfs";
pub const ICEBERG_REST: &str = "mink-iceberg-rest";

pub fn node(node_id: i32) -> String {
    format!("mink{node_id}")
}

async fn docker(args: &[&str]) -> String {
    let output = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap_or_else(|e| panic!("docker {}: {e}", args.join(" ")));
    assert!(
        output.status.success(),
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

pub async fn available() -> bool {
    Command::new("docker")
        .args(["version", "--format", "{{.Server.Version}}"])
        .stdin(Stdio::null())
        .output()
        .await
        .is_ok_and(|o| o.status.success())
}

pub async fn kill(container: &str) {
    tracing::info!(container, "docker kill");
    docker(&["kill", "--signal", "SIGKILL", container]).await;
}

pub async fn start(container: &str) {
    tracing::info!(container, "docker start");
    docker(&["start", container]).await;
}

pub async fn restart(container: &str) {
    tracing::info!(container, "docker restart");
    docker(&["restart", "-t", "1", container]).await;
}

pub async fn pause(container: &str) {
    tracing::info!(container, "docker pause");
    docker(&["pause", container]).await;
}

pub async fn unpause(container: &str) {
    tracing::info!(container, "docker unpause");
    docker(&["unpause", container]).await;
}

pub async fn status(container: &str) -> String {
    docker(&[
        "inspect",
        "--format",
        "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}",
        container,
    ])
    .await
}

pub async fn wait_healthy(container: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let status = status(container).await;
        if status == "running healthy" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{container} never became healthy, last status {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn wait_stopped(container: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = status(container).await;
        if status.starts_with("exited") || status.starts_with("dead") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{container} did not stop, last status {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn logs_tail(container: &str, lines: usize) -> String {
    let output = Command::new("docker")
        .args(["logs", "--tail", &lines.to_string(), container])
        .stdin(Stdio::null())
        .output()
        .await
        .expect("docker logs");
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
