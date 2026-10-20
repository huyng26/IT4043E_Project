# Global collection and operation

Collection covers **all locations and parameters returned by OpenAQ**, without a
Hanoi/country/radius filter or a station count cap. MongoDB stores the catalog,
source facts and durable scheduling state. See [MongoDB schema/API](mongodb-schema.md).

## Run continuously in Docker

Create `.env` from `.env.example` if needed and put `OPENAQ_API_KEY` in `.env`:

```sh
if [ ! -f .env ]; then cp .env.example .env; fi
docker compose --profile collectors up -d --build --wait
docker compose logs -f crawler
```

The `collectors` profile starts MongoDB, Kafka, initializes all three topics, and
runs the crawler with `restart: unless-stopped`, heartbeat leases, and a Kafka
publisher. Credentials are injected at runtime and excluded from the build context.
The config is mounted read-only; edit `config/collectors.json` and restart the
crawler to apply settings. MongoDB and Kafka use persistent named volumes.

```sh
docker compose stop crawler                 # retain schedules and data
docker compose --profile collectors up -d   # resume durable work
```

MongoDB is bound to `127.0.0.1:27017`, Kafka to `127.0.0.1:9092`. Containers use
service names internally. The local database defaults to `urban_air_quality`
without authentication. Change `MONGO_PORT` and the host `MONGO_URI` together if
necessary. MongoDB's file descriptor limit is explicitly 65,536 to support the
schema/index workload; see [MongoDB ulimit guidance](https://www.mongodb.com/docs/v8.0/reference/ulimit/).

## Host CLI

```sh
docker compose up -d --wait mongodb
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[collectors]'
.venv/bin/python -m collectors init-db
.venv/bin/python -m collectors discover
.venv/bin/python -m collectors run --source all --publish
```

Host publication also needs `docker compose --profile kafka up -d --wait`.
Use one deployment configuration per database; multiple workers coordinate
through persistent leases and shared API budgets. Do not run mixed code versions
against the same catalog indefinitely. Environment variables override `.env`.
`--config PATH` goes before the subcommand.

## Schedule and scope

| Work | Default schedule and behavior |
| --- | --- |
| Global inventory | Immediately on first start, then daily; `/parameters` and all `/locations` pages. |
| Change detection | Every hour per parameter; all pages of `/parameters/{id}/latest` with a frozen seven-day observation lookback per sweep. |
| Detailed measurements | A new/changed snapshot requests the sensor job immediately; ordinary reconciliation runs every six hours. |
| Weather | Every hour per deduplicated stationary coordinate; independent worker and budget. |
| Historical backfill | Explicit jobs for every catalog sensor, in bounded 24-hour source windows. |
| Kafka publication | Continuously drains pending records; waits five seconds when caught up. |

`run` bootstraps discovery automatically. No sensor selection is required. Inventory
includes mobile sensors, old stations and stations with unknown city/coordinates;
none is dropped merely because the first PM2.5 hourly policy cannot aggregate it.
OpenAQ locality labels are preserved; missing city names remain null rather than
being fabricated. Weather enrichment requires valid stationary coordinates and
uses an explicit rounded-coordinate mapping, independent of station identity.

OpenAQ documents pull endpoints; this implementation uses polling rather than
claiming an immediate webhook trigger. The latest sweep detects new timestamps
and changed latest values, then enqueues detailed period queries. Fast snapshots
are stored separately from detailed measurements and published to a separate
`air_quality_latest` topic, because `/latest` does not supply reporting intervals.
The seven-day window focuses change detection on recent source observations;
older delayed data and corrections depend on sensor reconciliation/backfill.
References: [latest semantics](https://docs.openaq.org/resources/latest) and
[parameter latest query](https://docs.openaq.org/api/operations/parameters_latest_get_v3_parameters__parameters_id__latest_get).

An OpenAQ worker focuses on catalog/latest sweeps; another reserves execution
for detailed sensor jobs. Workers also give turns to metadata repair/backfill,
so a long inventory or change queue cannot permanently monopolize all execution.
Each catalog/latest page is checkpointed and yields to scheduling. Sensor scans
use a saved cycle target/cursor to advance through the overlapping lookback,
instead of repeatedly querying the same first window after a restart.

Initial detailed collection covers the most recent 24 hours. Subsequent cycles
look back 48 hours from completed progress to detect corrections and fill gaps.
A 24-hour source query window advances only after every page and durable fact/
outbox write succeeds. Empty responses can complete a query window; completion
is not evidence of usable coverage. Older history requires explicit backfill.

## Global scale and API limits

Near realtime is a scheduling target, not an unconditional worldwide hourly SLA.
The shared per-key OpenAQ budget caps all inventory, latest, detail, backfill and
retry requests at 60/minute and 2,000/hour. At least one detailed request per
10,000 sensors already needs five hours at that hourly limit, before other work.
Latest batching reduces detection cost but cannot replace complete period queries.
Backlogs remain durable and visible in `status`; increasing worker count does not
increase provider quota. A stronger global freshness SLA needs a suitable provider
quota/bulk strategy. [OpenAQ limits](https://docs.openaq.org/using-the-api/rate-limits).

The Open-Meteo budget also tracks 5,000/hour and 10,000/day, across restarts and
workers. Requests exceeding two weeks of data reserve weighted units conservatively.
The default six-variable, four-day request costs one unit. At one request per
location/hour, 10,000/day supports about 416 distinct hourly locations; global
weather jobs can therefore queue longer than one hour. Coordinate reuse reduces
requests without dropping air-quality stations. [Open-Meteo limits and accounting](https://open-meteo.com/en/pricing).

`429`, rate headers and `Retry-After` defer shared requests. Network failures and
selected 408/5xx responses retry with exponential backoff. Missing/deleted source
resources (404/410) retry after a day rather than creating a hot loop; they remain
visible as source errors. No data is marked successfully collected on an HTTP
failure. Provider pagination has no snapshot guarantee, so overlap and historical
reconciliation remain necessary when upstream pages change during a sweep.

## Backfill, recovery and migration

Finish the initial global catalog before queuing history for all sensors:

```sh
.venv/bin/python -m collectors discover
.venv/bin/python -m collectors backfill --start 2026-07-01T00:00:00Z --end 2026-10-01T00:00:00Z
```

`backfill` creates persistent jobs rather than blocking the CLI on every sensor.
Without dates it uses the configured 90-day period ending at the current UTC hour.
The same fixed bounds reuse job identities and resume committed windows. The
continuous crawler executes the queue alongside live work. Historical progress
is separate from live cursors; completed fixed jobs remain inspectable.

```sh
.venv/bin/python -m collectors recover
.venv/bin/python -m collectors migrate
.venv/bin/python -m collectors status
```

Recovery restages incomplete captured responses with original timestamps and
request/row IDs. Migration projects existing v1 outbox facts/catalog into v2
without deleting raw data or resetting publication acknowledgements. Both are
retry-safe. Source facts and outbox records must finish before response staging
is marked complete. An ACK from Kafka precedes marking an outbox row published;
a crash between them can repeat delivery. There is no cross-system transaction.

Weather keeps the explicit `gfs_seamless` model, original units, nulls/zero and
fetch vintages. After downtime its past range expands up to the forecast API's
92-day limit; larger gaps fail visibly and require a future archive backfill.
Model issue time remains null when absent. Fetch availability is never backdated
to the observation/forecast valid time. Full historical weather archive ingestion
and HDFS archival are still pending.

## Inspect data and verify

```sh
.venv/bin/python -m collectors cities --country VN --limit 100
.venv/bin/python -m collectors history --station openaq:LOCATION_ID --parameter pm25 --start 2026-10-19T00:00:00Z --end 2026-10-20T00:00:00Z
.venv/bin/python -m collectors poll --source all --max-jobs 10
.venv/bin/python -m unittest discover -s tests -v
RUN_MONGO_TESTS=1 RUN_KAFKA_TESTS=1 .venv/bin/python -m unittest discover -s tests -v
```

Use a real catalog location ID for history. `poll` is a bounded maintenance pass;
`run` is the continuous service. `discover --max-pages 2` can inspect a small
resumable portion; `discover --refresh` requests a new scan when the daily one is
not due. A partial scan is not reported as a completed global catalog. `status`
shows catalog completion, source facts, outstanding/failed jobs and oldest due
schedules. Tests use synthetic fixtures and disposable `uaq_test_*` databases/
topics. See [live verification](source-feasibility.md).
