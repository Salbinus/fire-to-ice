// src/main.rs
mod config; mod schema;
mod consumer { pub mod reader; }
mod source { pub mod firestore_listen; }
mod pipeline { pub mod batcher; pub mod transform; }
mod sink { pub mod parquet_writer; pub mod iceberg_commit; }

use clap::{Parser, Subcommand};
use tracing::*;
use firestore::{FirestoreDb, FirestoreDbOptions};
use futures::StreamExt;

#[derive(Parser)]
struct Cli {
  #[command(subcommand)]
  cmd: Cmd
}
#[derive(Subcommand)]
enum Cmd { Run, Backfill, Read }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt().with_env_filter("info").init();
  let cfg = config::Config::from_env()?;

  match Cli::parse().cmd {
    Cmd::Run => {
      let db_options = FirestoreDbOptions::new(cfg.gcp_project.clone())
          .with_database_id("oltp".to_string());
      let db = FirestoreDb::with_options(db_options).await?;
      let stream = source::firestore_listen::stream_orders(&db, "orders").await?;

      let parquet = sink::parquet_writer::ParquetSink::new(&cfg.gcs_bucket, &cfg.gcs_prefix).await?;
      let iceberg = sink::iceberg_commit::IcebergCommit::new(&cfg.catalog_uri).await?;

      let mut buffer = Vec::with_capacity(cfg.batch_max_rows);
      let mut last_flush = tokio::time::Instant::now();

      tokio::pin!(stream);
      println!("Starting to process documents from stream...");
      
      while let Some(doc) = stream.next().await {
        println!("Processing document: {:?}", doc);
        buffer.push(doc);
        let time_up = last_flush.elapsed().as_secs() >= cfg.batch_max_seconds;
        let size_up = buffer.len() >= cfg.batch_max_rows;

        if time_up || size_up || buffer.len() > 0 {
          println!("Flushing batch: {} documents", buffer.len());
          let batch = schema::to_orders_batch(&buffer)?;
          let path = parquet.write(&cfg.table_ns, &cfg.table_orders, &batch).await?;
          // For demo, we skip getting exact file size/rows; put sensible values:
          iceberg.append_parquet(&cfg.table_ns, &cfg.table_orders, &format!("gs://{}/{}", cfg.gcs_bucket, path), 0, batch.num_rows() as i64).await?;
          buffer.clear();
          last_flush = tokio::time::Instant::now();
          info!("committed {}", path);
        }
      }
      
      // Flush any remaining documents
      if !buffer.is_empty() {
        println!("Flushing final batch: {} documents", buffer.len());
        let batch = schema::to_orders_batch(&buffer)?;
        let path = parquet.write(&cfg.table_ns, &cfg.table_orders, &batch).await?;
        iceberg.append_parquet(&cfg.table_ns, &cfg.table_orders, &format!("gs://{}/{}", cfg.gcs_bucket, path), 0, batch.num_rows() as i64).await?;
        info!("committed final batch: {}", path);
      }
      
      println!("Stream ended. Final buffer size: {}", buffer.len());
    }
    Cmd::Backfill => {
      // Option A: read existing docs once and push them through the same path
      // (Or load from Firestore export in GCS)
      unimplemented!("Backfill command stub");
    }
    Cmd::Read => {
      let reader = consumer::reader::Reader::new(&cfg.gcs_bucket, &cfg.gcs_prefix).await?;
      reader.read(&cfg.table_ns, &cfg.table_orders).await?;
    }
  }
  Ok(())
}
