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

Collector and pipeline-contract tests (OpenAQ pagination and adaptation, HTTP
retries, settings validation, and collector envelopes accepted by the Spark-side
validators) need the collector dependencies, but no network or running services.
Without them those test modules are skipped:

```sh
python3 -m pip install -e ".[collectors]"
python3 -m unittest discover -s tests -v
```

GitHub Actions runs both variants on Python 3.11 and 3.13 for every push and pull request.

The JSON output also retains original records, revision history, quality failures, and malformed input lines. See the [implemented input contract](docs/data-contracts.md). This is a bounded, in-memory correctness demo. The fixture is unsuitable for real monitoring, training, or evaluation.

## Collect source data globally

The crawler discovers **all OpenAQ locations and parameters**, including stations
with unknown city names. MongoDB schema v2 separates catalog, measurements,
revisions, fast latest snapshots, weather vintages and durable crawler jobs.
`database.MongoDB` provides the dedicated read/write API. The serving path is
**collectors → Kafka → Spark Structured Streaming → MongoDB**. Collectors use a
separate control database for jobs, quotas, metadata cache and the durable outbox;
only Spark projects Kafka events into the serving database.

Put `OPENAQ_API_KEY` in `.env`, then start the continuous Docker deployment:

```sh
if [ ! -f .env ]; then cp .env.example .env; fi
docker compose --profile pipeline up -d --build --wait
docker compose logs -f crawler spark
```

The service refreshes the global inventory daily, checks recent latest snapshots
hourly, triggers detail queries for changes, and reconciles sensor history in
bounded windows. Weather and Kafka publication run independently. MongoDB-backed
leases, checkpoints, revisions and shared API budgets support restart recovery.
Provider quotas can lengthen global refresh cycles; the status command exposes
backlogs rather than promising every station updates within an hour.

See [Spark pipeline and migration](docs/streaming.md), [operation and API limits](docs/collectors.md), [MongoDB schema and Python
reads](docs/mongodb-schema.md), and [live verification](docs/source-feasibility.md).
HDFS, hourly serving views, the API/dashboard and the assessed Kubernetes
deployment remain pending.

Keep small samples in `data/`; store credentials, generated models, checkpoints, and large datasets outside Git. Generated local results belong in the ignored `data/outputs/` directory.

Start with the [detailed system design](docs/system-design.md), which records the agreed Lambda architecture, required technologies, Spark assessment coverage, and implementation milestones. The [implementation plan](docs/implementation-plan.md), [architecture](docs/architecture.md), and [data contracts](docs/data-contracts.md) describe the first executable slice and the remaining work.
