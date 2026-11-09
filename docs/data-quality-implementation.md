# Dirty-data implementation map

Status: **inactive placeholders added; feature implementation pending**. The
existing collector, local replay and four-topic Spark consumer still behave as
before. New Python functions raise `NotImplementedError`; imports perform no
network, database, topic or file writes. The example configuration is not loaded.
The [quality design](data-quality.md) defines the intended behavior and evaluation.

## New files and responsibilities

| Placeholder | Implement here |
| --- | --- |
| [quality/contracts.py](../streaming/quality/contracts.py) | Wrapper, decision, summary and separate evaluator-manifest contracts. Topic/dataset constants reserve the planned names. |
| [quality/profiling.py](../streaming/quality/profiling.py) | Profile frozen real data, natural defects, units, periods, revisions and known cadence before mutation. |
| [quality/rules.py](../streaming/quality/rules.py) | Pure, versioned parse/repair/quality decisions shared by local replay, Spark speed and batch. Preserve received bytes and use existing measurement semantics. |
| [quality/coverage.py](../streaming/quality/coverage.py) | Expected/received/usable/missing sensor-hours from known active inventory and cadence after revision selection. |
| [quality/dispatcher.py](../streaming/quality/dispatcher.py) | Experiment-only wrapper decoding and executor projection into explicitly isolated storage. |
| [quality_replay.py](../streaming/quality_replay.py) | Bounded deterministic fault generation, separate evaluator manifest, then optional Kafka publication using existing utilities. |
| [quality_evaluation.py](../streaming/quality_evaluation.py) | Baseline comparison, repair correctness, precision/recall, false positives, coverage loss and retry/order invariance. Only this side reads expected answers. |
| [quality_batch.py](../streaming/quality_batch.py) | Bounded shared-rule rebuilds and quarantine replay from fixed archive cutoffs; validate before publishing a new generation. |
| [quality_schema.py](../database/quality_schema.py) | Future quality collection validators/indexes; integrate with the existing repository rather than adding a DB client. |
| [api/quality.py](../api/quality.py) | Future bounded quality-summary router with explicit live or experiment/run scope. No FastAPI app/router exists yet. |
| [dashboard/quality/README.md](../dashboard/quality/README.md) | Quality panel requirements until the React application exists. |
| [quality_features.py](../ml/quality_features.py) | Future coverage/missingness/age features with availability-aware inputs and training-only feature imputation. |
| [quality.example.json](../config/quality.example.json) | Inactive draft configuration for seed, fault profile, input selection and explicit experiment storage. |

`streaming/quality/__init__.py` marks the shared package. Function signatures and
configuration fields are draft interfaces to refine during implementation, not
completed processing APIs. `api/` and `ml/` remain outside the current setuptools
package list; add their packaging/dependencies when those services are built.

## Changes to existing implementation

| Existing code | Change when the corresponding slice is implemented |
| --- | --- |
| [models.py](../models.py) | Preserve `MeasurementEvent` schema-v1 validation and identity/revision helpers. Define a distinct repair boundary before strict parsing; do not silently change the meaning of existing source envelopes. |
| [streaming/measurements.py](../streaming/measurements.py) | Extract/reuse existing unit and quality rules through the shared policy. Keep revision selection before usability filtering; preserve zero, invalidating corrections and hourly identity semantics. |
| [streaming/replay.py](../streaming/replay.py) | Add an explicit quality experiment mode/adapter using shared rules. Preserve the current fixture command and report contract. Keep mutation/evaluation outside core processing. |
| [streaming/processing.py](../streaming/processing.py) | Use shared decisions for live facts and write deterministic quality outcomes. Keep its four-topic live contract; the experiment dispatcher handles wrappers separately. |
| [streaming/spark.py](../streaming/spark.py) | Add a separately selected experiment entry point/configuration with its own database and checkpoint, reusing execution mechanics. Continue executor-side processing and failed-write retry behavior. |
| [database/documents.py](../database/documents.py) | Align normalized/quality projections with shared decisions; preserve received payloads, revision guards and original/derived hashes. |
| [database/schema.py](../database/schema.py) and [mongodb.py](../database/mongodb.py) | Integrate quality validators/indexes, deterministic decision upserts, recomputed summaries and bounded reads through `MongoDB`. Add migration/version handling before enabling new writes. |
| [compose.yaml](../compose.yaml) and future `infra/` manifests | Provision the replay topic and explicitly opted-in experiment job/resources only once dispatcher/storage isolation work. Do not subscribe the current live writer to replay wrappers. |
| [tests/test_replay.py](../tests/test_replay.py) | Keep current regression checks; add behavior-focused quality and integration tests as implementations arrive. |

Collectors continue to capture original responses and source envelopes. Dirty
data is produced from a frozen copy after collection, using `KafkaManager` for
publication. HDFS raw archival and committed archive snapshots remain prerequisite
work for durable batch replay; local frozen JSONL can support the first slice.

## Implementation order

1. **Contracts and source profile.** Define wrapper/decision/manifest shapes and
   snapshot/cadence metadata. Implement profiling against frozen local input.
2. **Shared rules and local faults.** Implement deterministic single-fault
   generation, pure repair/quality decisions and local comparison. Prove unit and
   timezone equivalence, null/zero behavior, malformed-byte retention, duplicate
   handling and corrections that invalidate an earlier value. Neither the parser
   nor the processing worker receives expected answers.
3. **Coverage and evaluation.** Add grouped gaps/delays, then the mixed profile.
   Evaluate against an unchanged baseline; report destructive data loss instead
   of reconstructing missing values from the manifest. Save reports locally.
4. **Persistence and isolated Kafka/Spark.** Implement schema/repository methods,
   wrapper dispatch and an explicit experiment launcher. Require run-specific
   output paths/database/checkpoint. Verify repeated publications, partial-write
   recovery and restart counts before enabling quality writes on the live path.
5. **Archive and batch reconciliation.** Connect fixed HDFS snapshots, archive all
   replay records and rebuild with the same policy. Compare final speed/batch
   output at matching cutoffs and rule versions; validate before publication.
6. **Serving and features.** Expose prepared quality summaries in FastAPI/React;
   add quality-aware features when the forecast pipeline exists. Optional sensor
   anomaly/neighbor warnings come after the core workflow.

Start with steps 1–3 without Kafka, Spark or public APIs. No placeholder should be
treated as a successful no-op: calling an unfinished operation fails explicitly.
Do not wire these functions into the current startup path until their behavior
and recovery checks pass.
