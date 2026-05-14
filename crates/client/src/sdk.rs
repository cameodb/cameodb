use anyhow::{Context, Result};
use reqwest::{Client, Url, header};
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

        // Configure client for large file downloads with appropriate timeouts
        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(300)) // 5 minute timeout for large files
            .connect_timeout(std::time::Duration::from_secs(30))
            .pool_idle_timeout(std::time::Duration::from_secs(90));

        // For external HTTPS requests (schema detection from URLs), we need to handle
        // certificate validation more gracefully. Check if we should accept invalid certs.
        if std::env::var("CAMEODB_ACCEPT_INVALID_CERTS").is_ok() {
            builder = builder.danger_accept_invalid_certs(true);
        }

        let http = builder.build().context("Failed to build HTTP client")?;

        Ok(Self { base_url, http })
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

    pub async fn list_indexes(&self, include_data_size: bool) -> Result<ListIndexesResponse> {
        let mut url = self.base_url.join("_indexes")?;
        if include_data_size {
            url.set_query(Some("data_size=true"));
        }
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
        fields: Option<Vec<String>>,
    ) -> Result<JsonValue> {
        let url = self.base_url.join(&format!("api/{}/search", index))?;
        let body = serde_json::json!({
            "query": query,
            "limit": limit,
            "fields": fields,
        });

        let resp = self.http.post(url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Search failed: {} - {}", status, text);
        }
        resp.json().await.context("Failed to parse search response")
    }

    pub async fn get_index_config(&self, index: &str) -> Result<IndexConfigResponse> {
        let url = self
            .base_url
            .join(&format!("api/{}/_config", index))
            .context("Invalid config URL")?;
        let resp = self.http.get(url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("Failed to fetch index config: {}", resp.status());
        }
        resp.json()
            .await
            .context("Failed to parse index config response")
    }

    pub async fn put_index_config(&self, index: &str, config: &JsonValue) -> Result<()> {
        let url = self
            .base_url
            .join(&format!("api/{}/_config", index))
            .context("Invalid config URL")?;
        let resp = self.http.put(url).json(config).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to set index config: {} - {}", status, text);
        }
        Ok(())
    }

    pub async fn delete_index(&self, index: &str, delete_schema: bool) -> Result<JsonValue> {
        let mut url = self
            .base_url
            .join(&format!("api/{}", index))
            .context("Invalid delete URL")?;
        if delete_schema {
            url.set_query(Some("delete_schema=true"));
        }

        let resp = self.http.delete(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Delete index failed: {} - {}", status, text);
        }

        resp.json()
            .await
            .context("Failed to parse delete index response")
    }

    pub async fn bulk_index(&self, index: &str, batch: &[JsonValue]) -> Result<JsonValue> {
        let url = self
            .base_url
            .join(&format!("api/{}/_bulk", index))
            .context("Invalid bulk URL")?;
        let resp = self.http.post(url).json(batch).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Bulk ingest failed: {} - {}", status, text);
        }
        resp.json()
            .await
            .context("Failed to parse bulk ingest response")
    }

    pub async fn stream_index_ndjson(&self, index: &str, body: Vec<u8>) -> Result<JsonValue> {
        let url = self
            .base_url
            .join(&format!("api/{}/document/stream", index))
            .context("Invalid streaming ingest URL")?;
        let resp = self
            .http
            .post(url)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Streaming ingest failed: {} - {}", status, text);
        }
        resp.json()
            .await
            .context("Failed to parse streaming ingest response")
    }

    /// Expose underlying HTTP client for auxiliary requests (e.g., fetching CSV schema samples)
    pub fn http(&self) -> &Client {
        &self.http
    }

    pub async fn admin_memory_stats(&self) -> Result<AdminMemoryResponse> {
        let url = self.base_url.join("_admin/memory")?;
        let resp = self.http.get(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Admin memory stats failed: {} - {}", status, text);
        }
        resp.json()
            .await
            .context("Failed to parse memory stats response")
    }

    pub async fn admin_memory_trim(&self, force: bool) -> Result<AdminMemoryResponse> {
        let mut url = self.base_url.join("_admin/memory/trim")?;
        if force {
            url.query_pairs_mut().append_pair("force", "true");
        }
        let resp = self.http.post(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Admin memory trim failed: {} - {}", status, text);
        }
        resp.json()
            .await
            .context("Failed to parse memory trim response")
    }

    pub async fn admin_index_commit(&self, index: &str) -> Result<AdminIndexCommitResponse> {
        let url = self
            .base_url
            .join(&format!("_admin/index/{}/commit", index))?;
        let resp = self.http.post(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Admin index commit failed: {} - {}", status, text);
        }
        resp.json()
            .await
            .context("Failed to parse index commit response")
    }

    pub async fn admin_index_evict_writer(
        &self,
        index: &str,
    ) -> Result<AdminIndexEvictWriterResponse> {
        let url = self
            .base_url
            .join(&format!("_admin/index/{}/evict_writer", index))?;
        let resp = self.http.post(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Admin index evict-writer failed: {} - {}", status, text);
        }
        resp.json()
            .await
            .context("Failed to parse index evict-writer response")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: String,
    pub active_shards: usize,
    pub cluster_name: Option<String>,
    pub cluster_enabled: Option<bool>,
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
    #[serde(default)]
    pub total_size_bytes: Option<u64>,
    #[serde(default, alias = "size_mb")]
    pub index_size_mb: Option<u64>,
    #[serde(default)]
    pub data_size_mb: Option<u64>,
    pub shard_count: usize,
    #[serde(default)]
    pub field_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexConfigResponse {
    #[serde(default)]
    pub fields: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdminMemoryResponse {
    pub before: ProcessMemoryStats,
    pub after: Option<ProcessMemoryStats>,
    pub jemalloc: Option<JemallocStats>,
    pub purge_result: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessMemoryStats {
    pub vm_size_kb: Option<u64>,
    pub vm_rss_kb: Option<u64>,
    pub rss_anon_kb: Option<u64>,
    pub rss_file_kb: Option<u64>,
    pub rss_shmem_kb: Option<u64>,
    pub vm_data_kb: Option<u64>,
    pub vm_swap_kb: Option<u64>,
    pub threads: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JemallocStats {
    pub allocated: Option<u64>,
    pub active: Option<u64>,
    pub resident: Option<u64>,
    pub metadata: Option<u64>,
    pub retained: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardError {
    pub shard_id: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminIndexCommitResponse {
    pub index: String,
    pub shards_total: usize,
    pub shards_committed: usize,
    pub errors: Vec<ShardError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminIndexEvictWriterResponse {
    pub index: String,
    pub shards_total: usize,
    pub writers_evicted: usize,
    pub writers_missing: usize,
    pub errors: Vec<ShardError>,
}
