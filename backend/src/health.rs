use serde::Serialize;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub commit: String,
    pub uptime_seconds: u64,
    pub features: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct HealthState {
    version: String,
    commit: String,
    features: Vec<String>,
    started_at: Instant,
}

impl HealthState {
    pub fn new() -> Self {
        Self::with_commit(std::env::var("GIT_COMMIT").unwrap_or_else(|_| "unknown".to_string()))
    }

    pub fn with_commit(commit: impl Into<String>) -> Self {
        Self {
            version: crate::VERSION.to_string(),
            commit: commit.into(),
            features: backend_features(),
            started_at: Instant::now(),
        }
    }

    pub fn response(&self) -> HealthResponse {
        HealthResponse {
            status: "ok".to_string(),
            version: self.version.clone(),
            commit: if self.commit.trim().is_empty() {
                "unknown".to_string()
            } else {
                self.commit.clone()
            },
            uptime_seconds: self.started_at.elapsed().as_secs(),
            features: self.features.clone(),
        }
    }

    pub fn response_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(&self.response())
    }
}

fn backend_features() -> Vec<String> {
    let mut features = vec![
        "service-registry".to_string(),
        "service-discovery".to_string(),
        "message-broker".to_string(),
    ];

    if cfg!(debug_assertions) {
        features.push("debug-build".to_string());
    }

    features
}

pub async fn serve_health_endpoint(addr: &str, state: HealthState) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let state = state.clone();

        tokio::spawn(async move {
            let mut buffer = [0_u8; 1024];
            let read = match socket.read(&mut buffer).await {
                Ok(n) => n,
                Err(_) => return,
            };

            let request = String::from_utf8_lossy(&buffer[..read]);
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/");

            let (status_line, body) = if path == "/health" {
                (
                    "HTTP/1.1 200 OK",
                    state.response_json().unwrap_or_else(|_| "{}".to_string()),
                )
            } else {
                (
                    "HTTP/1.1 404 Not Found",
                    serde_json::json!({"error":"not_found"}).to_string(),
                )
            };

            let response = format!(
                "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );

            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_response_contains_required_json_fields_without_secrets() {
        let state = HealthState::with_commit("abc1234");
        let value: serde_json::Value =
            serde_json::from_str(&state.response_json().unwrap()).unwrap();

        assert_eq!(value["status"], "ok");
        assert_eq!(value["version"], crate::VERSION);
        assert_eq!(value["commit"], "abc1234");
        assert!(value["uptime_seconds"].as_u64().is_some());
        assert!(value["features"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("service-registry")));

        let text = value.to_string().to_ascii_uppercase();
        assert!(!text.contains("TOKEN"));
        assert!(!text.contains("SECRET"));
        assert!(!text.contains("PASSWORD"));
        assert!(!text.contains("KEY"));
    }

    #[test]
    fn empty_commit_falls_back_to_unknown() {
        let state = HealthState::with_commit("   ");
        assert_eq!(state.response().commit, "unknown");
    }
}
