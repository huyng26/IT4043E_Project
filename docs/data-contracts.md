# Proposed data contracts

These are design notes, not implemented JSON schemas or SQL. Finalize types, required fields, indexes, and table engines in phase 1–2.

## Kafka topics

| Topic | Content | Proposed partition key |
| --- | --- | --- |
| `air_quality_raw` | Original OpenAQ measurement payload plus provenance envelope. | Stable source-qualified station ID |
| `weather_raw` | Original Open-Meteo payload plus requested coordinates and provenance envelope. | Stable weather-location key |

Topic partition counts and retention depend on station volume and recovery requirements. Both streaming and archival use their own consumer groups.

Proposed common envelope:

| Field | Meaning |
| --- | --- |
| `schema_version` | Contract version for parsing and migration. |
| `event_id` | Stable identity derived from source keys, valid time, and variable/type; record revisions separately. |
| `source` | Provider name and endpoint/dataset provenance. |
| `station_id` / `location_key` | Source-qualified station identity or coordinate-based weather identity. |
| `event_time` | Observation time for measurements or valid time for weather; UTC with timezone. |
| `ingested_at` | Time the collector received the payload; UTC with timezone. |
| `issued_at` / `available_at` | Weather forecast issue time and known availability time where supplied/recorded. |
| `source_revision` | Upstream revision/version when available; otherwise a documented payload-hash policy. |
| `payload` | Original provider response or measurement record, retaining units and quality metadata. |

One air-quality event should represent one sensor measurement. A weather event may carry one valid hour or a response containing multiple hours; choose one representation before schema implementation. Preserve source payloads and document the deterministic normalized-row identity in either case.

Stable event identity alone cannot distinguish all upstream corrections. Define how revised values supersede earlier rows without losing their archived history.

## ClickHouse tables

| Table | Proposed logical row | Key information |
| --- | --- | --- |
| `stations` | One station metadata record/version. | Source station ID, name, coordinates, city, timezone, sensor/pollutant coverage, updated time. |
| `measurements` | One sensor/pollutant observation and revision. | Event ID, station, sensor, pollutant, observation/ingestion time, original and normalized value/unit, quality flags. |
| `weather` | One location and valid time per weather kind/issue/revision. | Coordinates, observation/forecast kind, valid time, issue/availability time, temperature, humidity, wind, precipitation, pressure, units. |
| `hourly_metrics` | One station/pollutant/hour and aggregation version. | UTC hour, concentration summary, count, expected coverage, completeness, AQI/sub-index where valid, standard version, processing time. |
| `predictions` | One station/pollutant/origin/horizon/model version. | Origin, target time, horizon 1–24 hours, predicted concentration/unit, model version, feature version, generated time, input freshness/status. |

These are logical identities, not an assumption that ClickHouse enforces unique keys. Choose engines, revision ordering, insert deduplication, and latest-row query behavior explicitly, including repeated streaming batches.

Weather can be shared by several stations. Maintain a documented coordinate/location mapping; do not assume a provider's location ID equals an OpenAQ station ID. Retain forecast vintages rather than overwriting them when they are used for backtesting.

## Time, units, quality, and AQI

- Store UTC timestamps and use the station timezone only for presentation and deliberately defined calendar features.
- Retain source units. Define pollutant-specific conversion rules; conversions requiring physical assumptions must record those assumptions. Unknown units are quarantined.
- Distinguish missing data from zero. Store coverage and quality flags for aggregates; define minimum coverage before considering an aggregate usable.
- Compute AQI only under an explicitly selected standard/version, using its required pollutant units and averaging periods. Longer-period calculations require sufficient preceding history.
- If a station-wide AQI is derived from pollutant sub-indices, retain the contributing pollutants, missing coverage, and dominant pollutant. Display unsupported or incomplete results explicitly.

## ML features and forecast contract

Candidate features include lagged pollution, past rolling summaries, lagged weather, hour/day calendar variables, and station metadata. Keep a versioned feature specification shared by training and inference.

For forecast origin `t`, features must be available at `t`; targets are concentrations at `t + h` for configured horizons `h = 1..24`. Completed observations or aggregates published after `t` cannot be used as inputs for a backtest at `t`. Future-weather features must come from a forecast vintage available at `t`, not realized future weather. A past-only weather model is the initial alternative if such vintages are unavailable.

Store concentration forecasts first. Derived AQI forecasts need the same averaging/history and coverage checks as monitored AQI. Prediction intervals are optional later work and must be labeled and evaluated if added.

## Planned API surface

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Service liveness. |
| `GET /ready` | Serving dependency readiness. |
| `GET /api/v1/stations` | Station inventory and available pollutant coverage. |
| `GET /api/v1/stations/{station_id}/latest` | Latest usable readings, available AQI, quality, and freshness. |
| `GET /api/v1/stations/{station_id}/history` | Bounded time range of measurements or hourly metrics. |
| `GET /api/v1/stations/{station_id}/weather` | Weather mapped to the station and requested time range. |
| `GET /api/v1/stations/{station_id}/predictions` | Forecast origin, horizon values, model metadata, and stale/missing status. |

Define range limits, pollutant filters, pagination, error envelopes, and forecast-selection rules before implementation. All responses should expose units and timestamps; stale or absent data must not be presented as current readings.
