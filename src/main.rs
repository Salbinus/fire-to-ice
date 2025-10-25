// src/main.rs
mod config; mod schema;
mod consumer { pub mod reader; }
mod source { pub mod firestore_listen; }
mod sink { pub mod parquet_writer; pub mod parquet_commit; }

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
enum Cmd { 
  Run { collection: Option<String> },
  Backfill, 
  Read 
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt().with_env_filter("info").init();
  let cfg = config::Config::from_env()?;

  match Cli::parse().cmd {
    Cmd::Run { collection } => {
      let db_options = FirestoreDbOptions::new(cfg.gcp_project.clone())
          .with_database_id("oltp".to_string());
      let db = FirestoreDb::with_options(db_options).await?;

      let parquet = sink::parquet_writer::ParquetSink::new(&cfg.gcs_bucket, &cfg.gcs_prefix).await?;
      let commit = sink::parquet_commit::ParquetCommit::new(&cfg.gcs_bucket, &cfg.gcs_prefix).await?;

      // Define all collections to process
      let collections = match collection {
        Some(col) => vec![col],
        None => vec![
          "orders".to_string(),
          "varieties".to_string(), 
          "variety_inventory".to_string(),
          "materials".to_string(),
          "batches".to_string(),
          "inventory_transactions".to_string(),
        ],
      };

      for collection_name in collections {
        println!("🚀 Starting ingestion for collection: {}", collection_name);
        
        // Route to appropriate stream based on collection name
        let stream: Box<dyn futures::Stream<Item = serde_json::Value> + Unpin> = match collection_name.as_str() {
          "orders" => Box::new(source::firestore_listen::stream_orders(&db, &collection_name).await?),
          "varieties" => Box::new(source::firestore_listen::stream_varieties(&db, &collection_name).await?),
          "variety_inventory" => Box::new(source::firestore_listen::stream_variety_inventory(&db, &collection_name).await?),
          "materials" => Box::new(source::firestore_listen::stream_materials(&db, &collection_name).await?),
          "batches" => Box::new(source::firestore_listen::stream_batches(&db, &collection_name).await?),
          "inventory_transactions" => Box::new(source::firestore_listen::stream_inventory_transactions(&db, &collection_name).await?),
          _ => return Err(anyhow::anyhow!("Unknown collection: {}", collection_name)),
        };

        let mut buffer = Vec::with_capacity(cfg.batch_max_rows);
        let mut last_flush = tokio::time::Instant::now();

        tokio::pin!(stream);
        println!("📊 Processing documents from collection: {}...", collection_name);
        
        while let Some(doc) = stream.next().await {
          println!("📄 Processing document: {:?}", doc);
          buffer.push(doc);
          let time_up = last_flush.elapsed().as_secs() >= cfg.batch_max_seconds;
          let size_up = buffer.len() >= cfg.batch_max_rows;

          if time_up || size_up || buffer.len() > 0 {
            println!("💾 Flushing batch: {} documents", buffer.len());
            let batch = match collection_name.as_str() {
              "orders" => schema::to_orders_batch(&buffer)?,
              "varieties" => schema::to_varieties_batch(&buffer)?,
              "variety_inventory" => schema::to_variety_inventory_batch(&buffer)?,
              "materials" => schema::to_materials_batch(&buffer)?,
              "batches" => schema::to_batches_batch(&buffer)?,
              "inventory_transactions" => schema::to_inventory_transactions_batch(&buffer)?,
              _ => return Err(anyhow::anyhow!("Unknown collection: {}", collection_name)),
            };
            let path = parquet.write(&cfg.table_ns, &collection_name, &batch).await?;
            commit.append_parquet(&cfg.table_ns, &collection_name, &format!("gs://{}/{}", cfg.gcs_bucket, path), 0, batch.num_rows() as i64).await?;
            buffer.clear();
            last_flush = tokio::time::Instant::now();
            info!("✅ committed {} for {}", path, collection_name);
          }
        }
        
        // Flush any remaining documents
        if !buffer.is_empty() {
          println!("💾 Flushing final batch: {} documents", buffer.len());
          let batch = match collection_name.as_str() {
            "orders" => schema::to_orders_batch(&buffer)?,
            "varieties" => schema::to_varieties_batch(&buffer)?,
            "variety_inventory" => schema::to_variety_inventory_batch(&buffer)?,
            "materials" => schema::to_materials_batch(&buffer)?,
            "batches" => schema::to_batches_batch(&buffer)?,
            "inventory_transactions" => schema::to_inventory_transactions_batch(&buffer)?,
            _ => return Err(anyhow::anyhow!("Unknown collection: {}", collection_name)),
          };
          let path = parquet.write(&cfg.table_ns, &collection_name, &batch).await?;
          commit.append_parquet(&cfg.table_ns, &collection_name, &format!("gs://{}/{}", cfg.gcs_bucket, path), 0, batch.num_rows() as i64).await?;
          info!("✅ committed final batch: {} for {}", path, collection_name);
        }
        
        println!("🏁 Completed ingestion for collection: {}", collection_name);
      }
      
      println!("🎉 All collections processed successfully!");
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
