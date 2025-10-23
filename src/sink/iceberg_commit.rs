// src/sink/iceberg_commit.rs
use iceberg::{Catalog, CatalogBuilder, TableIdent, memory::MemoryCatalogBuilder};
use std::sync::Arc;
use std::collections::HashMap;

pub struct IcebergCommit {
  catalog: Arc<dyn Catalog>,
}

impl IcebergCommit {
  pub async fn new(catalog_uri: &str) -> anyhow::Result<Self> {
    // For now, keep using memory catalog but log the intended GCS location
    let mut props = HashMap::new();
    props.insert("warehouse".to_string(), "memory://".to_string());
    
    if catalog_uri.starts_with("gs://") {
      println!("Intended GCS warehouse: {} (currently using memory catalog)", catalog_uri);
      println!("Note: To use GCS, you'll need to implement proper Iceberg table creation");
    }
    
    let catalog = MemoryCatalogBuilder::default()
        .load("memory", props)
        .await?;
    
    Ok(Self { catalog: Arc::new(catalog) })
  }

  pub async fn append_parquet(&self, ns: &str, table: &str, gcs_path: &str, file_len: i64, row_count: i64) -> anyhow::Result<()> {
    let id = TableIdent::from_strs([ns, table])?;
    
    // Try to load the table, if it doesn't exist, create the namespace first
    match self.catalog.load_table(&id).await {
        Ok(_tbl) => {
            println!("Table found, would append parquet file: {} to table: {}", gcs_path, table);
        }
        Err(_) => {
            // Table doesn't exist, try to create namespace first
            let namespace_id = iceberg::NamespaceIdent::new(ns.to_string());
            match self.catalog.create_namespace(&namespace_id, std::collections::HashMap::new()).await {
                Ok(_) => {
                    println!("Created namespace: {}", ns);
                }
                Err(_) => {
                    println!("Namespace {} already exists or couldn't be created", ns);
                }
            }
            
            // For now, just log what we would do
            println!("Would create table and append parquet file: {} to table: {}", gcs_path, table);
        }
    }
    
    println!("File size: {} bytes, Row count: {}", file_len, row_count);
    Ok(())
  }
}
