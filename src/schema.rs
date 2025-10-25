// src/schema.rs
use arrow_array::{Float64Array, StringArray, RecordBatch, TimestampMillisecondArray, ArrayRef, UInt32Array};
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

// Variety schema
pub fn varieties_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("updated_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("_ingest_ts_ms", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
    ]))
}

pub fn to_varieties_batch(rows: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
    let mut id = Vec::new();
    let mut name = Vec::new();
    let mut description = Vec::new();
    let mut created_at = Vec::new();
    let mut updated_at = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut ingest_ts = Vec::new();

    for r in rows {
        id.push(r["id"].as_str().unwrap_or_default().to_string());
        name.push(r["name"].as_str().unwrap_or_default().to_string());
        description.push(r["description"].as_str().unwrap_or_default().to_string());
        
        // Parse timestamps
        let created_ts = r["created_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
        let updated_ts = r["updated_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
            
        created_at.push(created_ts);
        updated_at.push(updated_ts);
        ingest_ts.push(now_ms);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(id)),
        Arc::new(StringArray::from(name)),
        Arc::new(StringArray::from(description)),
        Arc::new(TimestampMillisecondArray::from(created_at)),
        Arc::new(TimestampMillisecondArray::from(updated_at)),
        Arc::new(TimestampMillisecondArray::from(ingest_ts)),
    ];
    RecordBatch::try_new(varieties_schema(), cols).map_err(Into::into)
}

// VarietyInventory schema
pub fn variety_inventory_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("variety_id", DataType::Utf8, false),
        Field::new("quantity_in_stock", DataType::Float64, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("last_updated", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("updated_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("_ingest_ts_ms", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
    ]))
}

pub fn to_variety_inventory_batch(rows: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
    let mut id = Vec::new();
    let mut variety_id = Vec::new();
    let mut quantity_in_stock = Vec::new();
    let mut unit = Vec::new();
    let mut last_updated = Vec::new();
    let mut created_at = Vec::new();
    let mut updated_at = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut ingest_ts = Vec::new();

    for r in rows {
        id.push(r["id"].as_str().unwrap_or_default().to_string());
        variety_id.push(r["variety_id"].as_str().unwrap_or_default().to_string());
        quantity_in_stock.push(r["quantity_in_stock"].as_f64().unwrap_or(0.0));
        unit.push(r["unit"].as_str().unwrap_or_default().to_string());
        
        let last_updated_ts = r["last_updated"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
        let created_ts = r["created_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
        let updated_ts = r["updated_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
            
        last_updated.push(last_updated_ts);
        created_at.push(created_ts);
        updated_at.push(updated_ts);
        ingest_ts.push(now_ms);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(id)),
        Arc::new(StringArray::from(variety_id)),
        Arc::new(Float64Array::from(quantity_in_stock)),
        Arc::new(StringArray::from(unit)),
        Arc::new(TimestampMillisecondArray::from(last_updated)),
        Arc::new(TimestampMillisecondArray::from(created_at)),
        Arc::new(TimestampMillisecondArray::from(updated_at)),
        Arc::new(TimestampMillisecondArray::from(ingest_ts)),
    ];
    RecordBatch::try_new(variety_inventory_schema(), cols).map_err(Into::into)
}

// Material schema
pub fn materials_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("quantity_in_stock", DataType::Float64, false),
        Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("updated_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("_ingest_ts_ms", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
    ]))
}

pub fn to_materials_batch(rows: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
    let mut id = Vec::new();
    let mut name = Vec::new();
    let mut unit = Vec::new();
    let mut quantity_in_stock = Vec::new();
    let mut created_at = Vec::new();
    let mut updated_at = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut ingest_ts = Vec::new();

    for r in rows {
        id.push(r["id"].as_str().unwrap_or_default().to_string());
        name.push(r["name"].as_str().unwrap_or_default().to_string());
        unit.push(r["unit"].as_str().unwrap_or_default().to_string());
        quantity_in_stock.push(r["quantity_in_stock"].as_f64().unwrap_or(0.0));
        
        let created_ts = r["created_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
        let updated_ts = r["updated_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
            
        created_at.push(created_ts);
        updated_at.push(updated_ts);
        ingest_ts.push(now_ms);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(id)),
        Arc::new(StringArray::from(name)),
        Arc::new(StringArray::from(unit)),
        Arc::new(Float64Array::from(quantity_in_stock)),
        Arc::new(TimestampMillisecondArray::from(created_at)),
        Arc::new(TimestampMillisecondArray::from(updated_at)),
        Arc::new(TimestampMillisecondArray::from(ingest_ts)),
    ];
    RecordBatch::try_new(materials_schema(), cols).map_err(Into::into)
}

// Batch schema
pub fn batches_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("variety_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("inoculation_date", DataType::Utf8, false),
        Field::new("expected_harvest_date", DataType::Utf8, false),
        Field::new("actual_harvest_date", DataType::Utf8, true),
        Field::new("quantity_planted", DataType::UInt32, false),
        Field::new("quantity_harvested", DataType::Float64, true),
        Field::new("notes", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("updated_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("_ingest_ts_ms", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
    ]))
}

pub fn to_batches_batch(rows: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
    let mut id = Vec::new();
    let mut variety_id = Vec::new();
    let mut status = Vec::new();
    let mut inoculation_date = Vec::new();
    let mut expected_harvest_date = Vec::new();
    let mut actual_harvest_date = Vec::new();
    let mut quantity_planted = Vec::new();
    let mut quantity_harvested = Vec::new();
    let mut notes = Vec::new();
    let mut created_at = Vec::new();
    let mut updated_at = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut ingest_ts = Vec::new();

    for r in rows {
        id.push(r["id"].as_str().unwrap_or_default().to_string());
        variety_id.push(r["variety_id"].as_str().unwrap_or_default().to_string());
        status.push(r["status"].as_str().unwrap_or_default().to_string());
        inoculation_date.push(r["inoculation_date"].as_str().unwrap_or_default().to_string());
        expected_harvest_date.push(r["expected_harvest_date"].as_str().unwrap_or_default().to_string());
        actual_harvest_date.push(r["actual_harvest_date"].as_str().unwrap_or_default().to_string());
        quantity_planted.push(r["quantity_planted"].as_u64().unwrap_or(0) as u32);
        quantity_harvested.push(r["quantity_harvested"].as_f64().unwrap_or(0.0));
        notes.push(r["notes"].as_str().unwrap_or_default().to_string());
        
        let created_ts = r["created_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
        let updated_ts = r["updated_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
            
        created_at.push(created_ts);
        updated_at.push(updated_ts);
        ingest_ts.push(now_ms);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(id)),
        Arc::new(StringArray::from(variety_id)),
        Arc::new(StringArray::from(status)),
        Arc::new(StringArray::from(inoculation_date)),
        Arc::new(StringArray::from(expected_harvest_date)),
        Arc::new(StringArray::from(actual_harvest_date)),
        Arc::new(UInt32Array::from(quantity_planted)),
        Arc::new(Float64Array::from(quantity_harvested)),
        Arc::new(StringArray::from(notes)),
        Arc::new(TimestampMillisecondArray::from(created_at)),
        Arc::new(TimestampMillisecondArray::from(updated_at)),
        Arc::new(TimestampMillisecondArray::from(ingest_ts)),
    ];
    RecordBatch::try_new(batches_schema(), cols).map_err(Into::into)
}

// InventoryTransaction schema
pub fn inventory_transactions_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("transaction_type", DataType::Utf8, false),
        Field::new("material_id", DataType::Utf8, true),
        Field::new("variety_id", DataType::Utf8, true),
        Field::new("quantity", DataType::Float64, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("reason", DataType::Utf8, false),
        Field::new("batch_id", DataType::Utf8, true),
        Field::new("order_id", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("updated_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("_ingest_ts_ms", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
    ]))
}

pub fn to_inventory_transactions_batch(rows: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
    let mut id = Vec::new();
    let mut transaction_type = Vec::new();
    let mut material_id = Vec::new();
    let mut variety_id = Vec::new();
    let mut quantity = Vec::new();
    let mut unit = Vec::new();
    let mut reason = Vec::new();
    let mut batch_id = Vec::new();
    let mut order_id = Vec::new();
    let mut created_at = Vec::new();
    let mut updated_at = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut ingest_ts = Vec::new();

    for r in rows {
        id.push(r["id"].as_str().unwrap_or_default().to_string());
        transaction_type.push(r["transaction_type"].as_str().unwrap_or_default().to_string());
        material_id.push(r["material_id"].as_str().unwrap_or_default().to_string());
        variety_id.push(r["variety_id"].as_str().unwrap_or_default().to_string());
        quantity.push(r["quantity"].as_f64().unwrap_or(0.0));
        unit.push(r["unit"].as_str().unwrap_or_default().to_string());
        reason.push(r["reason"].as_str().unwrap_or_default().to_string());
        batch_id.push(r["batch_id"].as_str().unwrap_or_default().to_string());
        order_id.push(r["order_id"].as_str().unwrap_or_default().to_string());
        
        let created_ts = r["created_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
        let updated_ts = r["updated_at"].as_str()
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
            .unwrap_or_default()
            .timestamp_millis();
            
        created_at.push(created_ts);
        updated_at.push(updated_ts);
        ingest_ts.push(now_ms);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(id)),
        Arc::new(StringArray::from(transaction_type)),
        Arc::new(StringArray::from(material_id)),
        Arc::new(StringArray::from(variety_id)),
        Arc::new(Float64Array::from(quantity)),
        Arc::new(StringArray::from(unit)),
        Arc::new(StringArray::from(reason)),
        Arc::new(StringArray::from(batch_id)),
        Arc::new(StringArray::from(order_id)),
        Arc::new(TimestampMillisecondArray::from(created_at)),
        Arc::new(TimestampMillisecondArray::from(updated_at)),
        Arc::new(TimestampMillisecondArray::from(ingest_ts)),
    ];
    RecordBatch::try_new(inventory_transactions_schema(), cols).map_err(Into::into)
}
