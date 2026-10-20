# MongoDB schema v2 and repository

`database.MongoDB` is the project's MongoDB read/write boundary. It owns connection
lifetime, validators, indexes, catalog/fact writes, bounded reads, job state and
atomic budget reservations. It does not poll APIs, sleep, schedule workers, or
publish Kafka messages. Those responsibilities belong to `collectors/`.

Use regular collections. Deterministic IDs and guarded updates are necessary for
corrections and uniqueness; MongoDB time-series collections are not used here.
All database dates are timezone-aware UTC BSON dates. Exact collection ordering
also uses a fixed-width microsecond ISO string because BSON dates retain only
millisecond precision. Kafka/source envelopes keep their original ISO timestamps.

## Catalog

| Collection | `_id` | Main fields and purpose |
| --- | --- | --- |
| `parameters` | `openaq:<parameter ID>` | `source_id`, `name`, `unit`, provider payload; all parameters returned by OpenAQ. |
| `cities` | Source/country/normalized-locality hash | `country_code`, `name`, `normalized_name`, `identity_kind`, nullable `administrative_id`, `updated_at`. |
| `stations` | `openaq:<location ID>` | Country, nullable city/locality, timezone, GeoJSON `geo`, mobility, provider/licenses, parameter names, raw metadata and first/last seen times. |
| `sensors` | `openaq:<sensor ID>` | Source numeric ID, station/location IDs, country/city, parameter object, original metadata and first/last seen times. |
| `weather_locations` | Hash of requested coordinates | Latitude/longitude and configured coordinate precision. |
| `station_weather` | Station ID | Reference to one weather location, without unbounded station arrays. |

OpenAQ supplies locations and optional `locality`, rather than a canonical global
city registry. A `cities` document represents a **source locality label within a
country**, not a verified administrative municipality. Same-named localities can
be ambiguous; original names, country and station coordinates remain available
for later geocoding/resolution. Missing locality stays null on a station and does
not exclude its sensors or measurements. No inferred city name is fabricated.

Inventory scans have no country, coordinate, parameter or mobility filter. Mobile
stations are retained. Weather enrichment is created for stationary stations with
valid coordinates; the requested coordinates are rounded to the configured grid
to reuse requests. Missing/mobile coordinates do not block air-quality collection.
Station-to-weather mappings are independent of station identity.

Indexes cover country/city/ID, station/parameter/sensor ID, source/sensor uniqueness,
station GeoJSON proximity, and weather mappings. Relationships use references;
measurements are never embedded in station or city documents.

## Air-quality facts

| Collection | Identity | Behavior |
| --- | --- | --- |
| `measurement_revisions` | Hash of event ID and payload revision | Immutable distinct source content with first captured provenance. Every raw delivery is separately retained in raw capture/outbox. |
| `measurements` | Event ID | Current revision of a sensor/parameter/source-period observation. |
| `latest_measurements` | Source-qualified sensor ID | Latest detailed period, with corrections resolved. |
| `source_latest` | Source-qualified sensor ID | Fast `/latest` snapshot: instant observed time, original value/unit and fetch provenance. It has no invented source reporting period. |

A measurement document contains:

```text
_id, schema_version=2, event_id, revision_hash
source, station_id, sensor_id, parameter
period_start, period_end                 # BSON UTC dates
value, unit                             # source value, including null or zero
value_ug_m3, quality_flags               # current PM2.5 policy only
collected_at, ingested_at, revision_order
source_flags, provider_payload, raw_response_id
```

The repository validates envelope fields, chronology and identity/revision hashes
before writing. PM2.5 follows the established unit/quality rules; other parameters
retain original units without inventing gas conversions. Collection accepts
different reporting intervals and mobile sensors even when the initial hourly
PM2.5 processing path cannot aggregate them.

Write order is revision → current observation → latest detailed observation.
Atomic update pipelines replace a projection only when its revision ordering is
newer. Invalid corrections, including null/negative PM2.5 values, supersede older
usable readings. Older-hour corrections cannot displace a later-hour latest
reading. Repeat writes reuse keys; stale writes cannot restore an old value.
There is no transaction spanning these three collections: interrupted projections
are repaired by restaging the captured response or rerunning migration.

`source_latest` is separate because OpenAQ's latest endpoint omits reporting
periods and can miss delayed/historical revisions. Its snapshot comparison
detects changed values at the same observed time as well as new timestamps.
A durable pending-dispatch flag repairs a crash between saving a change and
requesting the detailed sensor job. Detailed facts are the source for later hourly
processing; the fast snapshot is explicitly labeled when read.

History indexes use station/parameter/end-time/ID and sensor/end-time/ID. Revisions
have event/revision uniqueness. Reads use bounded limits and cursor pagination;
history ranges cannot exceed 90 days per request. `get_latest()` exposes detailed
observations; `get_source_latest()` exposes source snapshots with their real dates.

## Weather facts

`weather_vintages` uses the envelope's `vintage_id` and stores location, model,
UTC valid time, actual fetch availability, original per-hour provider data, units,
quality and full-response reference. Every fetch vintage remains available.
`issue_time` stays null when the provider does not supply it.

The index is `(location_id, model, valid_time, available_at descending)`.
`get_weather(..., available_before=forecast_origin)` filters out vintages fetched
after the supplied origin; retrospective data must not be treated as previously
available. Results are bounded and can contain several vintages of one valid hour.

## Ingestion and operational state

| Collection | Purpose |
| --- | --- |
| `raw_responses` | Append-only response text, safe headers, request parameters, original collection timestamp, ingestion context and staging completion. |
| `collector_outbox` | Request/row identity, original envelope, topic/key and acknowledged broker coordinates. |
| `api_budgets` | Hashed key scope, atomic rolling request reservations, weighted weather charges and shared reset deferrals. |
| `collector_jobs` | Durable schedule, payload, priority, checkpoint, lease owner/expiry, trigger generation, errors and success times. |
| `control` | Completed query windows, schema marker and last completed catalog generation. |

Job acquisition is atomic. Heartbeats renew leases during slow queries or quota
waits; an expired owner cannot acknowledge/checkpoint a job claimed by another
worker. A trigger arriving during execution increments a generation, making the
job due again after completion. Each catalog/latest page and detailed sensor
window is a bounded scheduling unit. Failure retains committed progress.

Successful query cursors do not prove provider coverage: empty responses can
complete. The status command exposes pending jobs, oldest due times and failures.
Source freshness is the observation date; fetch/scheduling times describe pipeline
availability and delay. These meanings must remain separate in the future API.

## Python reads

Install collector dependencies and load `.env` before constructing the repository:

```python
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from database import MongoDB

load_dotenv()
with MongoDB() as db:
    cities = db.list_cities(country_code="VN", limit=100)
    stations = db.list_stations(country_code="VN", parameter="pm25", limit=100)
    if stations["items"]:
        station_id = stations["items"][0]["_id"]
        latest = db.get_latest(station_id, parameter="pm25")
        weather_location = db.get_weather_location_for_station(station_id)
        end = datetime.now(timezone.utc)
        history = db.get_measurements(station_id, end - timedelta(days=1), end,
                                      parameter="pm25", limit=100)
        # Pass history["next_cursor"] for the next page of this same range.
```

## Migration and retention

`python -m collectors migrate` projects existing outbox measurement/weather
envelopes and legacy station metadata into v2. It preserves original timestamps,
raw records and Kafka acknowledgement state, is repeatable, and does not delete
old data. `recover` handles incomplete captured responses instead. Migration is
an explicit maintenance command, not a full scan on every worker startup.

Validation is moderate so existing v1 documents are retained during migration;
new inserts are validated. Global inventory reads select v2 catalog documents.
No raw/fact TTL deletion is configured. A later retention policy must account for
HDFS archive acknowledgement and serving/replay requirements before deleting data.
Spark hourly/batch serving views remain a separate layer from source facts.
