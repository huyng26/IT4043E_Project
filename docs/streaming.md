# Kafka → Spark → MongoDB

The implemented serving path is:

```text
OpenAQ / Open-Meteo → collectors → Kafka → Spark Structured Streaming → serving MongoDB
```

## Ownership

`MONGO_COLLECTOR_DATABASE` defaults to `urban_air_quality_collector`. It contains
raw-response recovery journals, outbox acknowledgements, request budgets, leased
jobs, query checkpoints, `collector_latest` change-detection state and the catalog/
coordinate cache required to schedule requests. No measurements or weather facts
are projected by the collector. This control database is not an API data source.

`MONGO_DATABASE` defaults to `urban_air_quality`. Spark owns serving writes:
measurement revisions/current/latest, source latest snapshots, weather vintages,
station/sensor/locality metadata and station/weather mappings. The two database
names must differ. Collector CLI `cities` and `history` read the serving database;
`status`, discovery and backfill use collector control state.

Collectors emit structurally validated envelopes and preserve provider data.
Spark performs PM2.5 normalization/quality decisions and guarded revision writes;
it derives weather missing-value flags. Local JSONL replay remains the reference
for hourly rules; Spark hourly aggregates and coverage views are not implemented yet.

## Topics and execution

| Topic | Spark projection |
| --- | --- |
| `air_quality_raw` | `measurement_revisions`, `measurements`, `latest_measurements` |
| `weather_raw` | `weather_vintages` |
| `air_quality_latest` | `source_latest`; no invented reporting period |
| `source_catalog` | Parameter/station/sensor/locality and weather mapping metadata |

`streaming.spark` uses Spark 3.5.8 Structured Streaming and the matching Kafka
connector. Its default micro-batch interval is 30 seconds, with a 10,000-offset
input limit. `foreachBatch` runs validation and writes on executor partitions;
records are not collected into the driver. Only per-partition offset summaries
are collected after writes succeed. They are recorded in `control/stream:kafka-mongo`.

Each executor partition owns and closes one MongoDB client. Existing deterministic
observation/revision/vintage keys make fact writes repeatable. Current revisions
reject stale input, including corrections to null/negative values. Malformed or
collector-rejected records are retained in `stream_rejections`, keyed by Kafka
topic/partition/offset, with original bytes, timestamp, key and error.

A database failure, including a failure to store a rejection, fails the batch.
Spark commits progress only after the callback succeeds, then retries from its
durable checkpoint after a restart. Completion markers do not skip input on retry.
This is at-least-once execution with idempotent fact projections, not a transaction
across Kafka and MongoDB. Run one writer per serving database/checkpoint; catalog
metadata currently follows Kafka delivery order rather than revisioned batch serving.
See the official [Kafka integration guide](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html)
and [foreachBatch behavior](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch).

## Docker

```sh
if [ ! -f .env ]; then cp .env.example .env; fi
# Put OPENAQ_API_KEY in .env before starting the crawler.
docker compose --profile pipeline up -d --build --wait
docker compose logs -f crawler spark
```

The `collectors` profile also starts Spark. `Dockerfile.spark` supplies Java 17,
PySpark and a cached Kafka connector. Spark uses local two-thread execution for
development, with a named checkpoint volume. This is not the planned Kubernetes
deployment with two executor processes. Host and container runs must use separate
checkpoint locations and must not write concurrently to the same serving database.

## Host

Install Java 17 and Python 3.11, then:

```sh
.venv/bin/python -m pip install -e '.[collectors,streaming]'
docker compose --profile kafka up -d --wait
.venv/bin/python -m streaming.spark --checkpoint data/checkpoints/spark-mongo
# In a second terminal:
.venv/bin/python -m collectors run --source all --publish
```

`--available-now` drains currently available Kafka input and exits. Normal runs
keep processing new input. `--starting-offsets earliest` applies only to a new
checkpoint; restarts use stored offsets. Keep the same checkpoint with its serving
database. Kafka's seven-day retention is not a long-term archive; missing offsets
fail visibly (`failOnDataLoss=true`). HDFS archival remains pending.

## Existing deployments

Stop the old crawler before switching versions. The new collector starts in a
separate control database; it rediscovers the catalog instead of reusing serving
facts as scheduling state. Existing serving facts are retained.

To reprocess legacy outbox data still present in the serving database:

```sh
.venv/bin/python -m collectors migrate
.venv/bin/python -m collectors publish
# Keep Spark running to drain the resulting Kafka events.
```

Migration copies legacy catalog/outbox input to pending entries in the collector
database, using stable migration IDs. It preserves original envelopes and source
acknowledgements. Repeat it safely; publishing is bounded to 1,000 records per
command, or use the continuous publisher. Old raw responses not yet in the legacy
outbox must be recovered using the old deployment before switching database names.

HDFS archival, Spark hourly views, batch publication, Kubernetes, ML, API and the
dashboard remain subsequent work; this change establishes the processing boundary.

## Integration verification

Use a separate Compose project and disposable databases/checkpoints. Start only
MongoDB/Kafka for the synthetic test; the crawler does not need to contact public APIs:

```sh
docker compose build crawler spark
docker compose -p uaq-architecture-test --profile kafka up -d --wait mongodb kafka kafka-init
RUN_MONGO_TESTS=1 RUN_KAFKA_TESTS=1 RUN_SPARK_TESTS=1 .venv/bin/python -m unittest discover -s tests -v
docker compose -p uaq-architecture-test --profile pipeline down -v
```

The opt-in Spark test starts bounded Spark containers, verifies empty serving
facts before Spark, then checks catalog/weather/snapshot writes, duplicate and
correction behavior, invalid input bytes and a second run with the same checkpoint.
Set `SPARK_TEST_COMPOSE_PROJECT` to change the test project name. Do not run this
test against production Kafka topics; it publishes explicitly synthetic events.
