use arrow_array::{Float64Array, StringArray, TimestampMillisecondArray, RecordBatch, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[allow(dead_code)]
pub fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("variety", DataType::Utf8, false),
        Field::new("quantity_in_kg", DataType::Float64, true),
        Field::new("delivery_date", DataType::Utf8, true),
        Field::new("price_in_euro", DataType::Float64, true),
        Field::new("_ingest_ts_ms", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
    ]))
}

#[allow(dead_code)]
pub fn to_orders_batch(rows: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
    let mut ids = Vec::new();
    let mut varieties = Vec::new();
    let mut qtys = Vec::new();
    let mut deliveries = Vec::new();
    let mut prices = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut ingests = Vec::new();

    for r in rows {
        ids.push(r["id"].as_str().unwrap_or_default().to_string());
        varieties.push(r["variety"].as_str().unwrap_or_default().to_string());
        qtys.push(r["quantityInKg"].as_f64().or_else(|| r["quantity_in_kg"].as_f64()));
        deliveries.push(r["deliveryDate"].as_str().unwrap_or_default().to_string());
        prices.push(r["priceInEuro"].as_f64().or_else(|| r["price_in_euro"].as_f64()));
        ingests.push(now_ms);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(ids)),
        Arc::new(StringArray::from(varieties)),
        Arc::new(Float64Array::from(qtys)),
        Arc::new(StringArray::from(deliveries)),
        Arc::new(Float64Array::from(prices)),
        Arc::new(TimestampMillisecondArray::from(ingests)),
    ];

    RecordBatch::try_new(orders_schema(), cols).map_err(Into::into)
}
