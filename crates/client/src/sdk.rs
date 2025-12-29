use anyhow::{Context, Result};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone)]
pub struct CameoClient {
    base_url: Url,
    http: Client,
}

impl CameoClient {
    pub fn new(url: &str) -> Result<Self> {
        let base_url = Url::parse(url).context("Invalid URL")?;
        Ok(Self {
            base_url,
            http: Client::new(),
        })
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        let url = self.base_url.join("_cluster/health")?;
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("Failed to send health request")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Health check failed: {} - {}", status, text);
        }

        resp.json::<HealthResponse>()
            .await
            .context("Failed to parse health response")
    }

    pub async fn list_indexes(&self) -> Result<ListIndexesResponse> {
        let url = self.base_url.join("_indexes")?;
        let resp = self.http.get(url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("Failed to list indexes: {}", resp.status());
        }
        resp.json()
            .await
            .context("Failed to parse indexes response")
    }

    pub async fn search(
        &self,
        index: &str,
        query: &str,
        limit: Option<usize>,
    ) -> Result<JsonValue> {
        let url = self.base_url.join(&format!("api/{}/search", index))?;
        let body = serde_json::json!({
            "query": query,
            "limit": limit
        });

        let resp = self.http.post(url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Search failed: {} - {}", status, text);
        }
        resp.json().await.context("Failed to parse search response")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: String,
    pub active_shards: usize,
    pub cluster_name: Option<String>,
    pub distributed_enabled: Option<bool>,
    pub total_nodes: Option<usize>,
    pub connected_nodes: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListIndexesResponse {
    pub indexes: Vec<IndexInfo>,
    pub total_indexes: usize,
    pub total_shards: usize,
    pub node_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub document_count: u64,
    pub total_size_bytes: u64,
    pub size_mb: u64,
    pub shard_count: usize,
    pub field_names: Vec<String>,
}
