# Real-Time Urban Air Quality Monitoring

IT4043E project for monitoring and forecasting urban air quality using OpenAQ, Open-Meteo, Kafka, Spark, HDFS, MongoDB, Spark MLlib, GraphFrames, FastAPI, and Kubernetes.

## Structure

```text
IT4043E_Project/
├── README.md
├── docs/              # Project documentation
├── config/            # Application settings
├── collectors/        # OpenAQ and weather data collection
├── streaming/         # Validation, cleaning, and aggregation
├── ml/                # Training and prediction
├── api/               # FastAPI service
├── dashboard/         # Dashboard application
├── infra/             # Service and database configuration
├── tests/              # Tests and fixtures
└── data/              # Small local samples
```

## Run the first executable slice

From the repository root, using Python 3.11 or newer (no dependencies or services required):

```sh
python3 -m streaming.replay data/sample_measurements.jsonl
python3 -m unittest discover -s tests -v
```

The replay validates hourly PM2.5 events, normalizes units, resolves duplicate deliveries and corrections, and produces provisional station/hour means. The **synthetic demonstration fixture** contains 10 deliveries, 8 distinct revisions, 7 current observations, and 3 usable observations. Expected means are:

| Station | UTC hour on 2026-01-01 | PM2.5 (µg/m³) |
| --- | --- | --- |
| `demo:station-1` | 00:00 | 42 |
| `demo:station-1` | 01:00 | 35 |
| `demo:station-2` | 00:00 | 0 |

The JSON output also retains original records, revision history, quality failures, and malformed input lines. See the [implemented input contract](docs/data-contracts.md). This is a bounded, in-memory correctness demo; Kafka, Spark, HDFS, MongoDB, collectors, and the API are still pending. The fixture is unsuitable for real monitoring, training, or evaluation.

Keep small samples in `data/`; store credentials, generated models, checkpoints, and large datasets outside Git. Generated local results belong in the ignored `data/outputs/` directory.

Start with the [detailed system design](docs/system-design.md), which records the agreed Lambda architecture, required technologies, Spark assessment coverage, and implementation milestones. The [implementation plan](docs/implementation-plan.md), [architecture](docs/architecture.md), and [data contracts](docs/data-contracts.md) describe the first executable slice and the remaining work.
