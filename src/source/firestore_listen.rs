// src/source/firestore_listen.rs
use firestore::FirestoreDb;

pub async fn stream_orders(db: &FirestoreDb, col: &str) -> anyhow::Result<impl futures::Stream<Item=serde_json::Value>> {
    println!("Querying Firestore collection: {}", col);
    
    // Use the proper firestore-rs API to get documents
    let documents: Vec<serde_json::Value> = db
        .fluent()
        .select()
        .from(col)
        .obj()
        .query()
        .await?;
    
    println!("Found {} documents in collection '{}'", documents.len(), col);
    
    // Convert documents to a stream - they should already be proper JSON
    let document_stream = futures::stream::iter(documents.into_iter());
    
    Ok(document_stream)
}

pub async fn stream_varieties(db: &FirestoreDb, col: &str) -> anyhow::Result<impl futures::Stream<Item=serde_json::Value>> {
    println!("Querying Firestore collection: {}", col);
    
    let documents: Vec<serde_json::Value> = db
        .fluent()
        .select()
        .from(col)
        .obj()
        .query()
        .await?;
    
    println!("Found {} documents in collection '{}'", documents.len(), col);
    let document_stream = futures::stream::iter(documents.into_iter());
    Ok(document_stream)
}

pub async fn stream_variety_inventory(db: &FirestoreDb, col: &str) -> anyhow::Result<impl futures::Stream<Item=serde_json::Value>> {
    println!("Querying Firestore collection: {}", col);
    
    let documents: Vec<serde_json::Value> = db
        .fluent()
        .select()
        .from(col)
        .obj()
        .query()
        .await?;
    
    println!("Found {} documents in collection '{}'", documents.len(), col);
    let document_stream = futures::stream::iter(documents.into_iter());
    Ok(document_stream)
}

pub async fn stream_materials(db: &FirestoreDb, col: &str) -> anyhow::Result<impl futures::Stream<Item=serde_json::Value>> {
    println!("Querying Firestore collection: {}", col);
    
    let documents: Vec<serde_json::Value> = db
        .fluent()
        .select()
        .from(col)
        .obj()
        .query()
        .await?;
    
    println!("Found {} documents in collection '{}'", documents.len(), col);
    let document_stream = futures::stream::iter(documents.into_iter());
    Ok(document_stream)
}

pub async fn stream_batches(db: &FirestoreDb, col: &str) -> anyhow::Result<impl futures::Stream<Item=serde_json::Value>> {
    println!("Querying Firestore collection: {}", col);
    
    let documents: Vec<serde_json::Value> = db
        .fluent()
        .select()
        .from(col)
        .obj()
        .query()
        .await?;
    
    println!("Found {} documents in collection '{}'", documents.len(), col);
    let document_stream = futures::stream::iter(documents.into_iter());
    Ok(document_stream)
}

pub async fn stream_inventory_transactions(db: &FirestoreDb, col: &str) -> anyhow::Result<impl futures::Stream<Item=serde_json::Value>> {
    println!("Querying Firestore collection: {}", col);
    
    let documents: Vec<serde_json::Value> = db
        .fluent()
        .select()
        .from(col)
        .obj()
        .query()
        .await?;
    
    println!("Found {} documents in collection '{}'", documents.len(), col);
    let document_stream = futures::stream::iter(documents.into_iter());
    Ok(document_stream)
}
