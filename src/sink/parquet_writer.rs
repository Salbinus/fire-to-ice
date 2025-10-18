// src/sink/parquet_writer.rs
use object_store::{ObjectStore, path::Path};
use object_store::gcp::GoogleCloudStorageBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use arrow_array::RecordBatch;
use uuid::Uuid;
use std::io::Cursor;

pub struct ParquetSink {
  store: Box<dyn ObjectStore>,
  bucket: String,
  prefix: String,
}

impl ParquetSink {
  pub async fn new(bucket: &str, prefix: &str) -> anyhow::Result<Self> {
    let store = GoogleCloudStorageBuilder::new().with_bucket_name(bucket).build()?;
    Ok(Self { store: Box::new(store), bucket: bucket.into(), prefix: prefix.into() })
  }

  pub async fn write(&self, ns: &str, table: &str, batch: &RecordBatch) -> anyhow::Result<String> {
    let mut buf = Vec::new();
    {
      let props = WriterProperties::builder().build();
      let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
      writer.write(batch)?;
      writer.close()?;
    }

    let run_id = Uuid::new_v4().to_string();
    let date = chrono::Utc::now().date_naive();
    let path = format!("{}/{}/{}/data/ingest_date={}/run_id={}/part-{}.parquet",
                       self.prefix, ns, table, date, run_id, "000");
    let path = Path::from(path);

    self.store.put(&path, buf.into()).await?;
    Ok(path.to_string())
  }
}
