// src/schema.rs
use arrow_array::{Float64Array, StringArray, RecordBatch, TimestampMillisecondArray, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("variety", DataType::Utf8, false),
        Field::new("quantity_in_kg", DataType::Float64, true),
        Field::new("delivery_date", DataType::Utf8, true), // keep ISO date string or use date32
        Field::new("price_in_euro", DataType::Float64, true),
        Field::new("_ingest_ts_ms", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
    ]))
}

pub fn to_orders_batch(rows: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
    let mut id = Vec::new();
    let mut variety = Vec::new();
    let mut qty = Vec::new();
    let mut delivery = Vec::new();
    let mut price = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut ingest_ts = Vec::new();

    for r in rows {
        id.push(r["id"].as_str().unwrap_or_default().to_string());
        variety.push(r["variety"].as_str().unwrap_or_default().to_string());
        
        // Get numeric values directly from JSON (should be f64 from Firestore)
        let qty_val = r["quantity_in_kg"].as_f64().unwrap_or(0.0);
        let price_val = r["price_in_euro"].as_f64().unwrap_or(0.0);
        
        qty.push(qty_val);
        delivery.push(r["delivery_date"].as_str().unwrap_or_default().to_string());
        price.push(price_val);
        ingest_ts.push(now_ms);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(id)),
        Arc::new(StringArray::from(variety)),
        Arc::new(Float64Array::from(qty)),
        Arc::new(StringArray::from(delivery)),
        Arc::new(Float64Array::from(price)),
        Arc::new(TimestampMillisecondArray::from(ingest_ts)),
    ];
    RecordBatch::try_new(orders_schema(), cols).map_err(Into::into)
}
