# Firestore to Iceberg Pipeline

A high-performance Rust pipeline for ingesting data from Google Firestore and writing it to Apache Iceberg tables stored in Google Cloud Storage.

## Features

- **Micro-batching**: Configurable batch sizes and timeouts
- **Arrow/Parquet**: Efficient columnar data format
- **Iceberg integration**: ACID transactions and schema evolution
- **GCS storage**: Cloud storage backend
- **CLI interface**: Easy-to-use command-line tool

### Prerequisites

- Rust 1.70+
- Google Cloud SDK
- Valid GCP credentials
- Firestore database
- GCS bucket
- Iceberg catalog

### Build

```bash
git clone <repository-url>
cd firestore-to-iceberg
cargo build --release
```

## Configuration

### Environment Variables

```bash
# For Example
export GCP_PROJECT="your-project-id"
export GCS_BUCKET="your-bucket"
export GCS_PREFIX="your-prefix"
export BATCH_MAX_ROWS=50
export BATCH_MAX_SECONDS=5
export ICEBERG_CATALOG_URI="gs://path-to-iceberg-catalog"
```

### Configuration File

Create a `config.toml` file:

```toml
gcp_project_id = "your-project-id"
firestore_collection_path = "your-collection"
gcs_bucket = "your-bucket"
iceberg_table_name = "your-table"

# Optional overrides
batch_size = 1000
batch_timeout_ms = 5000
use_pubsub = false
```

## Usage

### Run Continuous Pipeline

```bash
# Using environment variables
./target/release/firestore-to-iceberg run

# Using configuration file
./target/release/firestore-to-iceberg run --config config.toml
```

### Backfill Historical Data

```bash
./target/release/firestore-to-iceberg backfill \
  --start-date "2023-01-01T00:00:00Z" \
  --end-date "2023-12-31T23:59:59Z"
```

### Compact Table

```bash
./target/release/firestore-to-iceberg compact
```

## Data Schema

The pipeline creates an Iceberg table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `document_id` | string | Firestore document ID |
| `collection_path` | string | Firestore collection path |
| `data` | string | Document data as JSON |
| `create_time` | timestamp | Document creation time |
| `update_time` | timestamp | Document last update time |
| `ingestion_time` | timestamp | Pipeline ingestion time |
| `batch_id` | string | Unique batch identifier |

## Performance Tuning

### Batch Configuration

- **Batch Size**: Larger batches improve throughput but increase latency
- **Batch Timeout**: Shorter timeouts reduce latency but may create smaller files
- **Compression**: Snappy compression balances speed and size

### GCS Configuration

- Use regional buckets close to your compute resources
- Enable lifecycle policies for cost optimization
- Consider using nearline/coldline for archival data

### Iceberg Configuration

- Regular compaction improves query performance
- Partition by date for time-series data
- Use appropriate file formats (Parquet recommended)

## Monitoring

### Logging

The pipeline uses structured logging with configurable levels:

```bash
export RUST_LOG="info"  # debug, info, warn, error
```

### Metrics

Key metrics to monitor:

- Documents processed per second
- Batch processing latency
- GCS upload success rate
- Iceberg commit success rate
- Memory usage
- Error rates

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
   ```

2. **Permission Errors**
   - Ensure service account has Firestore, GCS, and Pub/Sub permissions
   - Check bucket and table access

3. **Memory Issues**
   - Reduce batch size
   - Increase timeout to allow smaller batches
   - Monitor memory usage

4. **Network Issues**
   - Check GCP network connectivity
   - Verify firewall rules
   - Consider using VPC for better performance

### Debug Mode

```bash
export RUST_LOG="debug"
./target/release/firestore-to-iceberg run
```

## Development

### Project Structure

```
src/
├── main.rs              # CLI entry point
├── config.rs            # Configuration management
├── schema.rs            # Arrow/Iceberg schemas
├── source/              # Data sources
│   ├── mod.rs
│   ├── firestore_listen.rs
│   └── pubsub.rs
├── pipeline/            # Processing pipeline
│   ├── mod.rs
│   ├── batcher.rs
│   └── transform.rs
├── sink/                # Output sinks
│   ├── mod.rs
│   ├── parquet_writer.rs
│   └── iceberg_commit.rs
└── util.rs              # Utilities
```

### Testing

```bash
cargo test
cargo test -- --nocapture  # Show output
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:

1. Check the troubleshooting section
2. Search existing issues
3. Create a new issue with detailed information
4. Include logs and configuration (sanitized)

## Roadmap

- [ ] Schema evolution support
- [ ] Custom partitioning strategies
- [ ] Metrics and monitoring integration
- [ ] Kubernetes deployment manifests
- [ ] Docker containerization
- [ ] Multi-region support
- [ ] Data quality validation
- [ ] Incremental processing
- [ ] Dead letter queue handling

