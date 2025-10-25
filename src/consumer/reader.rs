// This module provides functionality to read data from GCS bucket into a Polars dataframe.
// The bucket structure is: bucket/prefix/namespace/table/data/ where parquet files are stored.

use polars::prelude::*;
use object_store::{ObjectStore, path::Path as ObjectStorePath};
use object_store::gcp::GoogleCloudStorageBuilder;
use std::sync::Arc;
use futures::StreamExt;

pub struct Reader {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    prefix: String,
}

impl Reader {
    pub async fn new(bucket: &str, prefix: &str) -> anyhow::Result<Self> {
        let store = GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket)
            .build()?;
        
        Ok(Self { 
            store: Arc::new(store),
            bucket: bucket.to_string(), 
            prefix: prefix.to_string() 
        })
    }

    pub async fn read(&self, ns: &str, table: &str) -> anyhow::Result<()> {
        println!("Reading from GCS bucket: {}", self.bucket);
        println!("Path pattern: {}/{}/{}/data/", self.prefix, ns, table);
        
        // List files in the data directory
        let data_path = format!("{}/{}/{}/data/", self.prefix, ns, table);
        let prefix_path = ObjectStorePath::from(data_path);
        
        println!("Listing files in: {}", prefix_path);
        
        let mut files = self.store.list(Some(&prefix_path));
        let mut parquet_files = Vec::new();
        
        while let Some(meta) = files.next().await {
            let meta = meta?;
            if meta.location.as_ref().ends_with(".parquet") {
                parquet_files.push(meta.location);
            }
        }
        
        if parquet_files.is_empty() {
            println!("No parquet files found in: {}", prefix_path);
            return Ok(());
        }
        
        println!("Found {} parquet files", parquet_files.len());
        
        // Read all parquet files to get the complete table
        println!("Reading all parquet files to reconstruct the complete table...");
        
        let scan_args = ScanArgsParquet::default();
        let mut lazy_frames = Vec::new();
        
        for file in &parquet_files {
            println!("Adding file to scan: {}", file);
            let gcs_path = format!("gs://{}/{}", self.bucket, file);
            let polars_path = PlPath::new(&gcs_path);
            
            let lazy_frame = LazyFrame::scan_parquet(polars_path, scan_args.clone())?;
            lazy_frames.push(lazy_frame);
        }
        
        // Combine all lazy frames into one table
        let combined_lazy = concat(lazy_frames, Default::default())?;
        
        // Execute the combined query - spawn blocking operation on separate thread
        let df = tokio::task::spawn_blocking(move || {
            combined_lazy.sort_by_exprs(vec![col("_ingest_ts_ms")], SortMultipleOptions::default().with_order_descending_multi([true])).collect()
        })
        .await??;
        
        println!("Loaded dataframe with {} rows and {} columns", df.height(), df.width());
        println!("Columns: {:?}", df.get_column_names());
        
        println!("\n=== Dataframe ===");
        println!("{}", df);
        
        // Show data types
        println!("\n=== Data Types ===");
        for (name, dtype) in df.get_columns().iter().map(|col| (col.name(), col.dtype())) {
            println!("{}: {:?}", name, dtype);
        }
        
        Ok(())
    }
}
