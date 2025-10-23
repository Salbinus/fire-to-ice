use tokio::time::{Instant, Duration};
use futures::StreamExt;

/// Collects elements from a stream into Vec batches
#[allow(dead_code)]
pub async fn run_batcher<S>(
    mut stream: S,
    max_rows: usize,
    max_secs: u64,
    mut handle_batch: impl FnMut(Vec<serde_json::Value>) -> anyhow::Result<()> + Send + 'static,
) -> anyhow::Result<()>
where
    S: futures::Stream<Item = serde_json::Value> + Unpin,
{
    let mut buffer = Vec::with_capacity(max_rows);
    let mut last_flush = Instant::now();

    while let Some(item) = stream.next().await {
        buffer.push(item);
        let time_up = last_flush.elapsed() >= Duration::from_secs(max_secs);
        let size_up = buffer.len() >= max_rows;

        if time_up || size_up {
            if !buffer.is_empty() {
                handle_batch(std::mem::take(&mut buffer))?;
                last_flush = Instant::now();
            }
        }
    }

    // Flush remaining
    if !buffer.is_empty() {
        handle_batch(buffer)?;
    }

    Ok(())
}
