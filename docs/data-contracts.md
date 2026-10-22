# Data contracts

The [system design](system-design.md) defines the target Kafka/HDFS/MongoDB contracts. This document distinguishes implemented replay/collector envelopes and staging from processing and serving interfaces still awaiting implementation.

## Implemented local PM2.5 envelope

`streaming/measurements.py` parses internal PM2.5 envelopes; `streaming/replay.py`
reads one JSON object per nonblank line. The example below is **synthetic internal
data**, not an OpenAQ response. `collectors/openaq.py` maps provider measurements
into this contract, preserving their original objects and reporting periods.

```json
{
  "schema_version": 1,
  "source": "demo",
  "station_id": "demo:station-1",
  "sensor_id": "sensor-1",
  "pollutant": "pm25",
  "period_start": "2026-01-01T00:00:00Z",
  "period_end": "2026-01-01T01:00:00Z",
  "collected_at": "2026-01-01T01:05:00Z",
  "ingested_at": "2026-01-01T01:06:00Z",
  "value": 40,
  "unit": "ug/m3",
  "source_flags": [],
  "provider_payload": {"value": 40, "synthetic": true}
}
```

All shown fields are required. Station IDs start with `source:`; sensor IDs are interpreted within the source/station. Values must be finite numbers or null; zero is a valid value. Timestamps must carry a timezone and are normalized to UTC for identities and hourly output. The initial contract requires `period_start < period_end <= collected_at <= ingested_at`.

### Identity and revisions

- `event_id`: SHA-256 of canonical JSON containing source, station, sensor, pollutant, and UTC-normalized period start/end. The interval is half-open `[start, end)`.
- `revision_hash`: SHA-256 of canonical JSON containing those identity fields plus original value, unit, source flags, and provider payload. Delivery timestamps are excluded.
- Canonical JSON sorts object keys, uses compact separators, and rejects nonfinite numbers. Numeric/string representations within source payloads remain significant; adapters must preserve consistent types.
- The current observation is the revision with greatest `(collected_at, revision_hash)`. The hash breaks equal-time ties deterministically; it does not establish upstream chronology. An ingestion-time tie-break selects the representative delivery of an otherwise identical current revision.
- The local prototype uses the collection-time fallback only. Providers with ordered revision metadata need an adapter/ordering extension before production use.
- Replays retain original collection metadata. Every distinct revision remains in the report; repeated delivery does not add a revision or another aggregate contribution.

The report retains one representative raw envelope per revision (greatest collection/ingestion times), rather than every delivery. The input file remains the full raw source for this demo. Future HDFS archival must retain **every Kafka record** with its partition/offset metadata.

### Quality and hourly output

Supported mass units are `ug/m3`, `µg/m³`, `μg/m³`, and `mg/m3`. Normalized output uses `ug/m3`; milligrams are multiplied by 1,000. No gas-unit conversions are attempted.

Readings remain visible with quality flags for null or negative values, unsupported units, nonfinite converted concentrations, nonempty source flags, or unsupported reporting periods. Only UTC-aligned, exactly one-hour source periods enter the initial aggregation. Unrecognized source flags are conservatively treated as unusable until a provider-specific policy exists. Structural errors are reported with the original input line.

Resolve revisions **before** filtering unusable observations. A newer invalid reading removes the old value from hourly results. For each station/hour, calculate the arithmetic mean of usable current hourly sensor readings and report `sample_count`, units, and `status: provisional`. Multiple sensors are weighted equally. Hours with no usable readings have no aggregate row; missing/invalid observations remain visible in `current_measurements`.

`sample_count` is not completeness. Expected cadence, coverage thresholds, subhourly/overlapping periods, and AQI require further rules after real sensors are selected. Raw invalid values remain preserved; normalized negative values are flagged and never aggregated.

### Replay report

`python3 -m streaming.replay data/sample_measurements.jsonl` writes a JSON report with:

- `summary`: delivery, rejection, distinct revision, duplicate, current, and usable counts.
- `hourly_metrics`: sorted provisional station/hour results.
- `current_measurements`: selected revisions, normalized values, quality flags, and original envelopes.
- `measurement_revisions`: distinct revisions and original envelopes.
- `rejected_records`: line number, error reason, and original text for structurally invalid input.

By default, the CLI completes successfully when individual records are rejected:
inspect `summary.rejected_records` and the detailed rejection list. For automated
validation, add `--strict` to exit with status 1 when any records are rejected:

```sh
python3 -m streaming.replay --strict data/sample_measurements.jsonl
```

Strict mode still writes the complete JSON report, including valid observations
and rejected lines. Quality-flagged observations are parsed records and do not
cause strict mode to fail. An unreadable input file fails the command in either
mode. Processing is bounded and in memory; this is a local correctness tool, not
a persistent stream processor.

## Implemented collector envelopes and MongoDB staging

OpenAQ emits the PM2.5 envelope above with `source: openaq`,
`station_id: openaq:<location_id>`, a string sensor ID, computed `event_id` and
`revision_hash`, and a full-response reference `raw_response_id`. All returned
OpenAQ parameters are collected; PM2.5 keeps the established processing policy.
Inline flags are retained and conservatively mapped into `source_flags`. Structurally invalid
measurements become rejected envelopes with `status`, `error`, original
`provider_payload`, and collection/ingestion metadata.

Weather envelopes use schema version 1 and `source: open-meteo`, with:

- `location_id`, associated `station_ids`, `valid_time`, and explicit `model`.
- `data_kind: modeled_weather`, requested coordinates and provider grid metadata.
- `values`, per-variable `units`, and explicit flags for null values.
- `issue_time: null`, `available_at`, and `availability_basis: recorded_fetch_time`.
- Collection/ingestion timestamps, original per-hour payload, and full-response reference.
- `event_id` from source/location/hour/model; `revision_hash` from values, units
  and payload; `vintage_id` additionally includes fetch availability.

Fetch availability does not prove when a historical value was originally
published. Malformed weather responses become rejected envelopes without
advancing successful weather progress.

| Collector collection | Identity/purpose |
| --- | --- |
| `stations` | Source-qualified location ID; global discovery metadata and attribution, with nullable locality and GeoJSON coordinates. |
| `sensors` | Source-qualified sensor ID; station association and metadata. |
| `raw_responses` | Request UUID; response text, safe response headers, query parameters, original collection timestamp, and recoverable ingestion context. |
| `collector_outbox` | Hash of request ID and row index; original envelope, topic/key, pending state, and acknowledged broker coordinates. |
| `api_budgets` | Provider plus hashed API-key scope; atomic rolling reservations and shared deferrals. |
| `control` | Completed source windows and catalog generations. |
| `collector_jobs` | Persistent schedule, source/kind, progress, leases, generation triggers and retry state. |

Responses are journaled before adaptation; incomplete responses can be restaged
with original collection provenance. A completed window means all pages/outbox
writes succeeded, not source completeness. Separate fetches remain separate
deliveries. Broker acknowledgement precedes marking a row published, so crashes
can produce repeat deliveries. This staging does not implement serving views or
replace HDFS archival. See [operations](collectors.md).

## Kafka contracts implemented; HDFS archival pending

| Topic | Event | Partition key |
| --- | --- | --- |
| `air_quality_raw` | One source sensor measurement, with period, original payload, identities, revisions, quality, and collection/ingestion metadata. | Source-qualified station ID |
| `weather_raw` | One location/valid-hour record, with model provenance, fetch vintage, and issue time when supplied (recorded availability otherwise). | Weather location key |
| `air_quality_latest` | Explicit source snapshot with observed time and value; no invented reporting period. | Source-qualified station ID |

The optional Docker Kafka/collectors profiles create three partitions per topic and seven-day
retention, with one replica. Weather location mappings remain separate from station
identities. Rejected source records stay on their corresponding raw topic.

Raw HDFS records retain Kafka topic, partition, offset, broker timestamp, original bytes, and provenance, including malformed events. Partition raw data by source/ingestion date, curated Parquet by observation date, and keep checkpoints, versioned features/models, and benchmarks separate.

## MongoDB source facts implemented; hourly serving pending

The dedicated `database.MongoDB` class now implements versioned catalog and source
fact storage, bounded reads, schema validators and indexes. See the authoritative
[v2 schema and repository API](mongodb-schema.md). In particular, `measurements`
contains guarded current observation revisions, `measurement_revisions` retains
distinct content, `latest_measurements` serves latest detailed sensor periods,
`source_latest` contains explicit instant snapshots, and `weather_vintages` retains
fetch availability. These source facts are separate from future hourly views.

| Processing/serving collection | Pending purpose |
| --- | --- |
| `speed_hourly_views` | Station/parameter/hour, quality, coverage and processed input metadata. |
| `batch_hourly_views` | Published generation plus station/parameter/hour and archive cutoff. |
| `predictions` | Station/parameter/origin/horizon/model version. |
| `graph_results` | Versioned station relationships and paired sample counts. |
| `control` publication keys | Processing/publication state beyond existing collector progress. |

Publish a batch generation only after all writes/checks succeed. Serve a batch key when it covers that key's latest processed input, otherwise its speed view. Retain serving provenance in API results.

## Forecasts and API — pending

Forecasts contain origin, target time, horizon `1..24`, concentration/unit, model/feature version, generated time, and input freshness. Features must be available at the forecast origin. Use chronological evaluation with the documented temporal gaps; do not use realized future weather as historical forecast input. Predictions may be missing when freshness or coverage is insufficient.

Implement the [system design's API routes](system-design.md#api-and-dashboard-interfaces): liveness/readiness, stations, latest measurements, bounded history, weather, predictions, station-network analytics, and pipeline status. History defaults to 24 hours, permits at most 90 days, and uses cursor pagination capped at 1,000 rows. All responses expose UTC timestamps, units, missing values, quality, and freshness. AQI is deferred.
