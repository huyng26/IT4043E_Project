# Data contracts

The [system design](system-design.md) defines the target Kafka/HDFS/MongoDB contracts. This document distinguishes the first implemented internal measurement envelope from storage and interfaces still awaiting implementation.

## Implemented local PM2.5 envelope

`streaming/measurements.py` parses one JSON object per nonblank line. The example is **synthetic internal data**, not an OpenAQ API response. A real collector adapter must map source semantics and preserve the original provider object.

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

The CLI completes successfully when individual records are rejected: inspect `summary.rejected_records` and the detailed rejection list. An unreadable input file fails the command. Processing is bounded and in memory; this is a local correctness tool, not a persistent stream processor.

## Target Kafka and HDFS contracts — pending

| Topic | Event | Partition key |
| --- | --- | --- |
| `air_quality_raw` | One source sensor measurement, with period, original payload, identities, revisions, quality, and collection/ingestion metadata. | Source-qualified station ID |
| `weather_raw` | One location/valid-hour record, with model provenance, fetch vintage, and issue time when supplied (recorded availability otherwise). | Weather location key |

Start with three partitions per topic and seven-day retention, with one replica for the local broker. Weather location mappings remain separate from station identities. The weather schema is not yet implemented.

Raw HDFS records retain Kafka topic, partition, offset, broker timestamp, original bytes, and provenance, including malformed events. Partition raw data by source/ingestion date, curated Parquet by observation date, and keep checkpoints, versioned features/models, and benchmarks separate.

## Target MongoDB collections — pending

Use regular collections with explicit deterministic keys and station/time indexes. A logical key alone does not implement retry-safe writes: guarded updates and failure tests are required.

| Collection | Logical identity/purpose |
| --- | --- |
| `stations` | Source-qualified station metadata, coordinates, sensors, cadence, and attribution. |
| `measurement_revisions` | Observation ID plus payload revision hash. |
| `current_measurements` | Observation ID, with guarded revision replacement. |
| `weather_vintages` | Location, valid hour, model, availability/issue vintage, and revision. |
| `speed_hourly_views` | Station/pollutant/hour, quality, coverage, and latest processed input metadata. |
| `batch_hourly_views` | Generation plus station/pollutant/hour and committed archive cutoff. |
| `predictions` | Station/pollutant/origin/horizon/model version. |
| `graph_results` | Versioned station relationships, paired sample counts, and components. |
| `control` | Collector/processing progress and publication state. |

Publish a batch generation only after all writes/checks succeed. Serve a batch key when it covers that key's latest processed input, otherwise its speed view. Retain serving provenance in API results.

## Forecasts and API — pending

Forecasts contain origin, target time, horizon `1..24`, concentration/unit, model/feature version, generated time, and input freshness. Features must be available at the forecast origin. Use chronological evaluation with the documented temporal gaps; do not use realized future weather as historical forecast input. Predictions may be missing when freshness or coverage is insufficient.

Implement the [system design's API routes](system-design.md#api-and-dashboard-interfaces): liveness/readiness, stations, latest measurements, bounded history, weather, predictions, station-network analytics, and pipeline status. History defaults to 24 hours, permits at most 90 days, and uses cursor pagination capped at 1,000 rows. All responses expose UTC timestamps, units, missing values, quality, and freshness. AQI is deferred.
