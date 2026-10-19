# Implementation plan

The [detailed system design](system-design.md) defines the agreed stack: Kafka, PySpark on Kubernetes, HDFS, MongoDB, Spark MLlib, GraphFrames, FastAPI, and React. Use kind for local deployment. PM2.5 monitoring comes first; AQI is deferred.

## Start coding during milestone 1

Infrastructure does not need to block the first executable code. Start with a small local replay that makes the measurement contract and correction rules reviewable, while source feasibility is investigated.

**Implemented:** `streaming/measurements.py` and `streaming/replay.py` provide a standard-library-only hourly PM2.5 processing path. Run from the repository root:

```sh
python3 -m streaming.replay data/sample_measurements.jsonl
python3 -m unittest discover -s tests -v
```

It validates records, preserves original payloads, derives stable observation/revision identities, normalizes supported mass units, resolves revisions, and recomputes station/hour means. Tests cover repeated deliveries, corrections, replay order, invalid corrections, missing values, zero, unsupported units, and source flags.

This slice accepts only UTC-aligned one-hour source periods for aggregation. It does not estimate coverage, calculate AQI, or prove distributed delivery guarantees. Its synthetic fixture is separate from future real historical data. Use its tests and contract as the reference when implementing Spark processing; verify equivalent results from Spark.

**Still required for milestone 1:** select the pilot city and real sensors, verify access/cadence/history, implement a source feasibility report and source-to-envelope adapter, and adapt the processing policy if selected sensors use other reporting intervals. No real station selection or API access has been validated yet.

## Delivery sequence

| Milestone | Work | Done when |
| --- | --- | --- |
| 1. Executable rules and source feasibility | Keep the local replay runnable; add real station/sensor discovery, configuration, an OpenAQ adapter, and a source coverage report. | Demo/tests pass, selected sensors and reporting periods are documented, and real payloads can be converted to the internal envelope. |
| 2. Kubernetes infrastructure | Add kind configuration and manifests for Kafka, HDFS, MongoDB, and Spark, with persistent storage and required indexes/topics. | Two Spark executors process tasks; HDFS reports three replicas; Kafka publish/read and MongoDB read/write checks pass. |
| 3. Ingestion and archive | Implement independent OpenAQ and Open-Meteo polling, pagination, retries, shared request budgets, and durable progress; archive Kafka records to HDFS. | Both topics receive data, progress survives restarts, and immutable archives retain broker coordinates and provenance for replay. |
| 4. Monitoring path | Port/verify the local rules in Spark speed processing; add deterministic MongoDB writes, FastAPI, and an initial React view. | Concentrations, history, quality, missing data, and freshness are visible; duplicates and corrections have tested effects. |
| 5. Batch serving | Rebuild history from a committed archive snapshot; publish versioned batch results and implement per-key speed/batch selection. | Corrected history is published safely; failed publication preserves the previous generation; serving does not duplicate hours. |
| 6. Advanced analytics | Add Spark analytics evidence, MLlib forecasting against persistence, and GraphFrames station relationships. | Required techniques execute with saved evidence; chronological forecast metrics and graph outputs are available through the API. |
| 7. Assessment package | Document startup, benchmark runs, recovery demonstrations, and limitations. | The project can be reproduced and demonstrated from written instructions. |

## Other coding that can start early

- **Source discovery CLI:** report candidate sensors, cadence, available history, and missing coverage before investing in ingestion.
- **MongoDB keys/index definitions and Kafka topic configuration:** turn documented storage identities into deployment artifacts for milestone 2.
- **API and React prototypes using explicitly labeled fixtures:** work can proceed before live ingestion, provided readiness and missing-data behavior remain honest.

Prefer the source discovery CLI next: station cadence and real payloads determine which collection and aggregation rules are needed.

## Essential correctness checks

- Preserve source payloads, units, observation periods, identities, revisions, collection times, and ingestion times.
- Expect repeated delivery. Test retry-safe projections, stale revision rejection, corrections that invalidate earlier readings, and recovery after partial writes.
- Keep every raw Kafka record in HDFS, including invalid input; give each streaming query its own checkpoint.
- Reconcile speed and batch results using committed offsets and per-key input coverage. Publish a batch generation only after all checks succeed.
- Use chronological model evaluation and recorded availability where known. Keep generated performance workloads separate from real training/evaluation data.
- Show source freshness separately from processing delay. Retain missing values as null and quality failures explicitly.

The [system design](system-design.md) contains the complete rubric, deployment sizing assumptions, source references, and assessment verification scenarios. Only the local replay is implemented so far.
