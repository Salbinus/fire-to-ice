// src/source/firestore_listen.rs
use firestore::FirestoreDb;
use futures::StreamExt;
use serde_json::Value;

pub async fn stream_orders(db: &FirestoreDb, col: &str) -> anyhow::Result<impl futures::Stream<Item=serde_json::Value>> {
    // For now, let's use a simple query to get existing documents
    // The listen functionality requires more complex setup with FirestoreListener
    println!("Querying Firestore collection: {}", col);
    let documents = db.fluent()
        .select()
        .from(col)
        .query()
        .await?;
    
    println!("Found {} documents in collection '{}'", documents.len(), col);
    
    // Convert documents to a stream by converting fields to JSON
    let document_stream = futures::stream::iter(documents.into_iter())
        .map(|doc| {
            // Convert Firestore document fields to JSON
            let mut json_obj = serde_json::Map::new();
            for (key, value) in doc.fields {
                // For now, convert to string representation
                let json_value = match value.value_type {
                    Some(ref value_type) => {
                        // Convert the value type to a string representation
                        Value::String(format!("{:?}", value_type))
                    }
                    None => Value::String("null".to_string()),
                };
                json_obj.insert(key, json_value);
            }
            Value::Object(json_obj)
        });
    
    Ok(document_stream)
}
