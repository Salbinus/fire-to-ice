// src/config.rs
pub struct Config {
    pub gcp_project: String,
    pub gcs_bucket: String,              // e.g. "my-lake"
    pub gcs_prefix: String,              // e.g. "warehouse"
    pub catalog_uri: String,             // Iceberg REST (BigLake or Nessie)
    pub table_ns: String,                // e.g. "farm"
    pub table_orders: String,            // "orders"
    pub batch_max_rows: usize,           // e.g. 25_000
    pub batch_max_seconds: u64,          // e.g. 30
  }
  
  impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
      let _ = dotenvy::dotenv();
      Ok(Self {
        gcp_project: std::env::var("GCP_PROJECT")?,
        gcs_bucket: std::env::var("GCS_BUCKET")?,
        gcs_prefix: std::env::var("GCS_PREFIX").unwrap_or_else(|_| "warehouse".into()),
        catalog_uri: std::env::var("ICEBERG_CATALOG_URI")?,     // e.g. BigLake/Nessie
        table_ns: std::env::var("TABLE_NS").unwrap_or_else(|_| "farm".into()),
        table_orders: "orders".into(),
        batch_max_rows: std::env::var("BATCH_MAX_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(25_000),
        batch_max_seconds: std::env::var("BATCH_MAX_SECONDS").ok().and_then(|v| v.parse().ok()).unwrap_or(30),
      })
    }
  }
  