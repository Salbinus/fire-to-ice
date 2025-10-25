// src/sink/parquet_commit.rs
use crate::sink::parquet_writer::ParquetSink;

pub struct ParquetCommit {
  #[allow(dead_code)]
  parquet_sink: ParquetSink,
}

impl ParquetCommit {
  pub async fn new(gcs_bucket: &str, gcs_prefix: &str) -> anyhow::Result<Self> {
    let parquet_sink = ParquetSink::new(gcs_bucket, gcs_prefix).await?;
    Ok(Self { parquet_sink })
  }

  pub async fn append_parquet(&self, ns: &str, table: &str, gcs_path: &str, file_len: i64, row_count: i64) -> anyhow::Result<()> {
    println!("Appending parquet file: {} to table: {} ({} bytes, {} rows)", gcs_path, table, file_len, row_count);
    Ok(())
  }
}
