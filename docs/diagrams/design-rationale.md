# Architecture and diagram design rationale

This document explains the choices shown in the [system](system-architecture.png) and [data](data-architecture.png) diagrams. It connects the architecture to the project's monitoring, correction, forecasting and assessment requirements, and explains the visual conventions used to present it.

The [system design](../system-design.md) remains the authoritative specification. Collectors, MongoDB source storage, Kafka publication and the local PM2.5 replay are implemented. Spark/HDFS processing, derived serving views, FastAPI, React and the assessed Kubernetes deployment are planned. The diagrams show the target system with that distinction visible.

## What the design needs to support

The dashboard needs recent air-quality readings, while historical analysis needs corrected, reproducible data. Those requirements interact: providers may return the same observation repeatedly, deliver an old interval late, or change a value already used in an hourly result. Weather adds another time dimension because a value's valid hour and the time it became available are different.

The design therefore preserves original inputs and their identities, builds replaceable projections for queries, and records enough progress to recover after interruption. Collection covers all OpenAQ locations and parameters. Initial aggregation supports stationary, UTC-aligned one-hour PM2.5 observations; collecting a record does not automatically make it eligible for that calculation.

The project also has an assessed stack: Kafka, PySpark, HDFS, MongoDB, Kubernetes, MLlib and GraphFrames. The choices below explain their roles within that constraint. Performance benefits still need evidence from the planned benchmarks.

## System architecture choices

**Separate speed and batch paths.** The speed path updates monitoring results as events arrive. The batch path rebuilds corrected history from a fixed archived input snapshot. This Lambda structure supports fresh results alongside reproducible historical computations, including delayed observations and corrections.

The cost is maintaining equivalent validation, revision selection and aggregation rules in both paths. The local replay provides an executable reference for those rules. Spark implementations must be checked against it; using the same technology alone would not guarantee equivalent results.

**Capture before publication.** MongoDB holds original provider responses, source facts and a durable publication outbox. The collector can recover an interrupted response and retry pending publication without fetching a replacement response with different provenance. A response is marked staged only after the required fact and outbox writes finish.

Kafka acknowledgement comes before marking an outbox row published. A crash between those steps can cause another delivery, so downstream projections must tolerate repeats. This is an explicit recovery protocol; there is no atomic transaction spanning MongoDB and Kafka. The diagram's MongoDB-to-Kafka arrow represents the outbox publisher, not a database service directly pushing records.

**Use Kafka between collection and processing.** Kafka buffers collected events and lets the speed query and archive query advance independently. A slow archive does not need to become part of each collector request. The three topics preserve different record semantics: detailed air-quality intervals, modeled weather with fetch vintages, and instant air-quality snapshots.

Kafka retention is bounded by the documented transport policy. HDFS is the planned replayable history store. Independent consumption requires separate progress/checkpoints and explicit recovery checks for each query.

**Give MongoDB and HDFS different storage roles.** MongoDB supports catalog lookups, bounded station/time reads, guarded source projections and future dashboard views. HDFS retains every raw Kafka record, including invalid records and broker coordinates, then stores curated Parquet, features, models and checkpoints. Keeping raw input makes it possible to rerun corrected parsing or processing rules without treating earlier derived output as the original evidence.

This separation introduces consistency work: batch computation must use a committed archive cutoff, and published results must identify their input coverage. The two MongoDB shapes show ingestion and serving responsibilities within the same logical database. They do not prescribe separate MongoDB deployments.

**Publish batch generations and select results per key.** A batch job writes and validates a new generation before publishing it. FastAPI uses that generation for an hourly key only if it covers the key's latest processed input. Otherwise it uses the speed result. For example, a correction beyond the batch's input cutoff requires the speed result until a later published generation includes it. The batch job's completion time alone is insufficient evidence of coverage.

A failed publication leaves the previous generation available. This costs additional generation and progress metadata, but prevents clients from receiving a partially published rebuild. API results expose their provenance so users can distinguish provisional and reconciled values.

**Keep analytics and request handling separate.** Batch reconstruction, MLlib forecasting and GraphFrames analysis use historical datasets and publish queryable outputs. FastAPI reads those outputs and applies serving rules; React presents them. This keeps model training and graph computation outside an interactive dashboard request. The shared “Spark batch & analytics” shape groups related workloads visually; they remain distinct jobs with different inputs and schedules. Forecast quality must be evaluated against persistence, and station relationships must not be interpreted as evidence of causation.

## Data architecture choices

**Reference catalog entities from observations.** Stations and sensors hold metadata, while observation history lives in separate collections. References avoid embedding growing histories inside station documents and allow measurements to be queried by sensor, station and interval. The repository and collection workflow manage these logical references.

The `cities` collection represents provider locality labels. Missing locality stays optional, and missing coordinates or mobility do not remove stations from the air-quality catalog. Weather enrichment has its own eligibility requirements. This preserves collection scope without inventing metadata.

**Separate observation identity, content revision and delivery.** An observation's identity comes from its source, station, sensor, parameter and reporting interval. Changed content creates a revision; receiving the same content again does not create another logical observation. `measurement_revisions` preserves distinct content, `measurements` selects the current revision, and `latest_measurements` projects the latest detailed interval per sensor.

For example, two usable sensor readings of 30 and 40 produce an hourly mean of 35. If the first reading is corrected to 50, the result becomes 45. Counting the correction as a third reading would incorrectly produce 40. If the correction makes the first reading unusable, its old contribution must be removed. This is why revision selection precedes quality filtering and aggregation.

Regular MongoDB collections with deterministic keys and guarded updates implement these identities explicitly. The tradeoff is maintaining several projections. Interrupted writes are repaired through recoverable capture and replay; the source collections are not updated by one transaction spanning the whole pipeline.

**Keep source snapshots separate from detailed periods.** `source_latest` provides fast change detection, but its source endpoint omits the reporting interval. Assigning an invented hour would turn freshness information into unsupported aggregation evidence. Detailed measurements supply the periods needed by the hourly processing policy. Separate collections and topics make that distinction visible through ingestion and future consumers.

**Store weather mappings and fetch vintages explicitly.** `station_weather` maps eligible stationary coordinates to reusable weather locations, allowing stations to share a weather request location. `weather_vintages` retains the valid time, model and actual fetch availability for each vintage. A later fetch must not silently replace the evidence of what was available earlier.

Suppose a forecast is issued at 10:00 and weather valid at 09:00 is first fetched at 11:00. That vintage cannot be used as information available to the 10:00 forecast. Keeping `valid_time` and `available_at` separate supports that check. Historical data fetched later may still support explicitly retrospective analysis; it does not establish historical publication availability. Retaining vintages costs storage and requires an availability-aware selection rule.

Full fields, keys and indexes remain in the [MongoDB schema](../mongodb-schema.md); event and projection rules remain in the [data contracts](../data-contracts.md).

## Diagram design choices

The system view answers “which components move, process and serve data?” The data view answers “what is stored, how are records related, and how do datasets become serving results?” Keeping those questions separate allows each drawing to stay readable without repeating every field and operational policy.

Shapes give components recognizable roles before their labels are read:

| Visual form | Meaning |
| --- | --- |
| Oval | External data provider. |
| Rounded rectangle | Collector, processing job, API or logical entity. |
| Cylinder | Persistent storage responsibility. |
| Queue profile with small record marks | Event transport through Kafka. |
| Stacked documents | Retained revisions, weather vintages or curated datasets. |
| Browser frame | The React presentation layer. |

Depth comes from cylinder caps, document offsets and browser chrome. These are native editable shapes. Stacked pages communicate multiplicity; they do not specify a replica count, file count or deployment topology. The Kafka marks are schematic records rather than partitions.

Green highlights the measurement and speed paths; purple highlights archival, batch and analytics. Blue/gray provides the shared component vocabulary. Status is encoded separately: solid application outlines indicate implemented components or source collections, while dashed outlines indicate planned components or datasets. External providers also have solid outlines. Neither color nor outline establishes current service health.

Connectors show the main data flow, logical references or projections according to the view. Orthogonal routes keep them out of unrelated shapes. Labels are reserved for distinctions such as current-revision selection, cardinality and batch publication. The HDFS bidirectional connector represents job reads and writes. The upper green branch in the storage lifecycle makes it clear that speed results do not wait for historical batch processing.

Some relationships are expressed in text instead of long crossing arrows. FastAPI also reads catalog and weather facts; `station_weather` references the station by its key. Grouped storage shapes omit individual physical paths and collection fields. The companion documentation retains those details so the visual simplification does not change the design.

## Boundaries and decisions still open

The deployment design uses kind on one physical workstation. It demonstrates distributed processes and DataNode replication, while all local nodes still share a physical failure domain. The diagram does not establish machine-level availability or measured capacity.

Further implementation must resolve or verify:

- Coverage thresholds, expected cadence and treatment of subhourly observations.
- Final derived collection schemas, indexes and representation of hours with no usable readings.
- Raw response/outbox retention once archival acknowledgement is available.
- Usable historical coverage, weather availability assumptions and model performance.
- Distributed recovery behavior, speed/batch equivalence and resource sizing under measured workloads.

See the [implementation plan](../implementation-plan.md) for the delivery sequence and the [collector guide](../collectors.md) for current recovery, scheduling and quota behavior.
