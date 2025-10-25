# Firestore to Parquet Pipeline

A high-performance Rust pipeline for ingesting data from Google Firestore and writing it to Parquet files stored in Google Cloud Storage.

## Features

- **Micro-batching**: Configurable batch sizes and timeouts
- **Arrow/Parquet**: Efficient columnar data format
- **GCS storage**: Cloud storage backend
- **CLI interface**: Easy-to-use command-line tool

### Prerequisites

- Rust 1.70+
- Google Cloud SDK
- Valid GCP credentials
- Firestore database
- GCS bucket

### Build

```bash
git clone <repository-url>
cd fire-to-ice
cargo build --release
```

## Configuration

### Environment Variables (or create a `.env` file:)

```bash
# Required
export GCP_PROJECT="your-project-id"
export GCS_BUCKET="your-bucket"
export GCS_PREFIX="your-prefix"
export BATCH_MAX_ROWS=50
export BATCH_MAX_SECONDS=5
```

## Usage

### Run Continuous Pipeline


## Output Structure

Files are organized in GCS as:
```
gs://your-bucket/data/
├── orders/data/ingest_date=2024-01-15/part-000.parquet
├── varieties/data/ingest_date=2024-01-15/part-000.parquet
├── variety_inventory/data/ingest_date=2024-01-15/part-000.parquet
├── materials/data/ingest_date=2024-01-15/part-000.parquet
├── batches/data/ingest_date=2024-01-15/part-000.parquet
└── inventory_transactions/data/ingest_date=2024-01-15/part-000.parquet
```

## Troubleshooting

### Project Structure

```
src/
├── main.rs              # CLI entry point
├── config.rs            # Configuration management
├── schema.rs            # Arrow schemas & batch converters
├── source/              # Data sources
│   ├── mod.rs
│   └── firestore_listen.rs
├── sink/                # Output sinks
│   ├── mod.rs
│   ├── parquet_writer.rs
│   └── parquet_commit.rs
└── consumer/             # Data consumers
    └── reader.rs
```

### Testing

```bash
cargo test
cargo test -- --nocapture  # Show output
```
