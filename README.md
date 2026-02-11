# target-kafka

`target-kafka` is a [Singer](https://hub.meltano.com/singer/spec) target for Apache Kafka, built with the [Meltano Singer SDK](https://sdk.meltano.com). It writes tap output to Kafka topics (one topic per stream) with configurable batching, compression, and message keys. Delivery is **at-least-once**: on failure, records may be retried and duplicates are possible.

## Installation

Install from PyPI:

```bash
uv tool install target-kafka
```

Install from GitHub (replace `your-org` with your GitHub org or username):

```bash
uv tool install git+https://github.com/your-org/target-kafka.git@main
```

**Publishing:** To publish to PyPI and add this loader to [Meltano Hub](https://hub.meltano.com), see [docs/PUBLISHING.md](docs/PUBLISHING.md). For version bumps and release steps, see [docs/RELEASING.md](docs/RELEASING.md).

## Configuration

### Accepted config options

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| bootstrap_servers | Yes | — | Kafka bootstrap servers (comma-separated, e.g. `localhost:9092,localhost:9093`) |
| topic_prefix | No | `""` | Prefix to add to all topic names |
| key_properties | No | `[]` | Record properties to use as Kafka message key (empty = round-robin partition) |
| batch_size | No | `100` | Number of records to batch before sending to Kafka |
| compression_type | No | `snappy` | Kafka message compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| include_sdc_properties | No | `true` | Include Meltano metadata (`_sdc_extracted_at`, `_sdc_received_at`, etc.) |

A full list of supported settings and capabilities is available by running:

```bash
target-kafka --about
```

### Configure using environment variables

With `--config=ENV`, the target reads configuration from environment variables (and from a `.env` file in the working directory if present). Use names like `TARGET_KAFKA_BOOTSTRAP_SERVERS`, `TARGET_KAFKA_TOPIC_PREFIX`, etc. See `.env.example` for a template.

## Usage

### Run with Meltano

You can run `target-kafka` in a pipeline using [Meltano](https://meltano.com/):

```bash
meltano run tap-smoke-test target-kafka
```

### Run the target directly

```bash
target-kafka --version
target-kafka --help

# Example with config file
tap-smoke-test | target-kafka --config config.json

# Example with env config
tap-smoke-test | target-kafka --config ENV
```

This target works in any Singer environment and does not require Meltano.

## Developer resources

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

### Setup and tests

```bash
uv sync
uv run pytest
uv run target-kafka --help
```

### Testing with Meltano

```bash
uv tool install meltano
meltano invoke target-kafka --version
meltano run tap-smoke-test target-kafka
```

### SDK dev guide

See the [Singer SDK dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for developing taps and targets.
