# Live source verification

Verification date: **2026-10-20**. The configured scope is the complete global
OpenAQ catalog, with no country/radius/station cap and all returned parameters.

The source observations below are historical verification of the earlier
deployment. The current serving path is collectors → Kafka → Spark → MongoDB,
with a separate collector-control database; see [pipeline operation](streaming.md).
On **2026-10-25**, synthetic integration tests against real MongoDB/Kafka/Spark
verified the new boundary, duplicate/revision handling, rejected-record retention
and restart from the same Spark checkpoint. This does not reverify live source
availability or establish usable historical coverage.

## OpenAQ

Live authentication and global endpoints have been verified. The initial two-page
bounded discovery pass read the parameter catalog and first location page,
persisting **43 parameters, 1,000 stations, 2,809 sensors and 388 source locality
labels**. This was explicitly a partial scan, not a claim of complete global
coverage. The continuous Docker service resumes later catalog pages and keeps
latest sweeps/detailed collection running alongside them.

A later observed checkpoint already contained more than 8,000 stations, 34,000
sensors and 400 detailed measurement observations. Counts continue changing while
the crawler runs. Query `python -m collectors status` for current counts and the
last completed catalog generation. Only a completed generation proves that the
scheduled catalog scan reached its final API page; it does not prove a consistent
upstream snapshot or complete historical measurement coverage.

Real measurements were written to `measurements`, `measurement_revisions` and
`latest_measurements`. Snapshots are stored separately in `source_latest`; hourly
aggregation policy remains PM2.5-only. Deleted/unavailable source metadata can
return 404 and is retained as a scheduled source error, not fabricated as a station.

OpenAQ `locality` is optional. The stored city count describes source locality
label groups, not a geocoded administrative city registry. Stations with missing
locality still participate in inventory and air-quality collection.

## Open-Meteo and Kafka

Live weather requests have been verified for the original Hanoi test point and
station-derived global coordinate mappings using the explicit `gfs_seamless`
model. Original 192 weather deliveries were migrated to v2 without deleting the
old raw/outbox records. The continuous service stores new modeled weather vintages
with actual fetch availability and publishes them through Kafka.

Docker initializes `air_quality_raw`, `weather_raw` and `air_quality_latest`, each
with three partitions, one replica and seven-day retention. Source-latest snapshots
have a dedicated topic because their reporting period is unknown.

## Verification limits

The test suite covers real MongoDB/Kafka, guarded revisions, bounded reads,
partial-write recovery, source scope, resumable scans, trigger generations and
exclusive leases. MongoDB's Docker open-file limit was raised to 65,536 after the
schema workload exposed the previous limit. Services resumed existing volume
state after recreation.

The configured hourly latest/weather schedules are targets subject to shared API
quotas and backlog, not a guaranteed hourly refresh of every worldwide sensor.
Usable 90-day history, source cadence/quality completeness, HDFS archive, Spark
hourly aggregation and end-to-end serving freshness still require later verification.
