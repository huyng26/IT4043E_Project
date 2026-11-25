# Dirty-data implementation map

Status: **implementation steps 1–3 done locally; steps 4–6 pending**. Contracts,
shared rules, profiling, coverage arithmetic, the deterministic fault builder, two
group scenarios and the baseline-vs-corrupted evaluator are implemented and tested
without Kafka, Spark, MongoDB or the network. The
existing collector, local replay and four-topic Spark consumer still behave as
before: nothing below is wired into them. Remaining placeholders raise
`NotImplementedError`; imports perform no network, database, topic or file
writes. The example configuration is `enabled: false` and refuses to run.
The [quality design](data-quality.md) defines the intended behavior and evaluation.
**Continuing this work? Start with the [handoff](quality-handoff.md)** for the
progress table, the next step and the rules that are easy to break.

## Files and responsibilities

| File | Status | Responsibility |
| --- | --- | --- |
| [quality/contracts.py](../streaming/quality/contracts.py) | **Implemented** | Wrapper (exact field set, base64/SHA-256 checks), decision and evaluator-manifest shapes with validators; retry-safe decision keys. |
| [quality/rules.py](../streaming/quality/rules.py) | **Implemented** (`rules-v1`) | `evaluate_payload` and `select_current_decisions`; see [shared rules](#shared-rules-rules-v1). |
| [quality/profiling.py](../streaming/quality/profiling.py) | **Implemented** | Deterministic profile of a frozen snapshot (+ optional catalog): natural defects by kind, units, periods, duplicates, revisions, cadence, coverage. |
| [quality/coverage.py](../streaming/quality/coverage.py) | **Implemented** | Catalog parsing, expected hourly sensor-hours and expected/received/usable/missing arithmetic. Unknown cadence yields `null`, not full coverage. |
| [quality/local_run.py](../streaming/quality/local_run.py) | **Implemented** | Manifest-free local processing: snapshot lines or replay wrappers to one saved decision each (results files). It never imports the evaluator or injector. |
| [quality_replay.py](../streaming/quality_replay.py) | `build` **implemented**; `publish_replay` placeholder | Seeded single-fault, `mixed-v1` and control builds, plus `missing_interval` and `correction_to_null` group scenarios, writing `replay.jsonl` + evaluator-only `manifest.json`. |
| [quality/dispatcher.py](../streaming/quality/dispatcher.py) | Placeholder | Experiment-only wrapper decoding and executor projection into explicitly isolated storage. |
| [quality_evaluation.py](../streaming/quality_evaluation.py) | **Implemented** (local) | Saved-results comparison with the baseline; see [evaluating a run](#evaluating-a-run). Only this side reads expected answers. |
| [quality_batch.py](../streaming/quality_batch.py) | Placeholder | Bounded shared-rule rebuilds and quarantine replay from fixed archive cutoffs; validate before publishing a new generation. |
| [quality_schema.py](../database/quality_schema.py) | Placeholder | Future quality collection validators/indexes; integrate with the existing repository rather than adding a DB client. |
| [api/quality.py](../api/quality.py) | Placeholder | Future bounded quality-summary router with explicit live or experiment/run scope. No FastAPI app/router exists yet. |
| [dashboard/quality/README.md](../dashboard/quality/README.md) | Requirements only | Quality panel requirements until the React application exists. |
| [quality_features.py](../ml/quality_features.py) | Placeholder | Future coverage/missingness/age features with availability-aware inputs and training-only feature imputation. |
| [quality.example.json](../config/quality.example.json) | Draft, disabled | Configuration read by `quality_replay build`; set `enabled: true` in a copy to run. |

The decision/dispatcher/storage interfaces are still drafts. `api/` and `ml/`
remain outside the current setuptools package list; add their packaging and
dependencies when those services are built.

## Shared rules (`rules-v1`)

`evaluate_payload(payload, source_topic=..., rule_version=...)` returns one
decision per received payload and reuses `models`/`streaming.measurements` for
identity, revision hash, unit factors and flags. Only `air_quality_raw` PM2.5 is
supported; other topics or unknown rule versions raise `ValueError`.

| Input | Outcome |
| --- | --- |
| Valid envelope | `accepted`; existing flags (`missing_value`, `negative_concentration`, `unsupported_unit`, `source_flagged`, `unsupported_reporting_period`) stay flags, never quarantine. |
| Numeric string (`" 35.0 "`) | Repaired to what `json.loads` yields for the trimmed literal (action `numeric_string_parsed`), so a stringified baseline round-trips to the **same** revision hash. Only the JSON number grammar is accepted; `""`, `"unknown"`, `"1,5"`, `"+5"`, `".5"`, `"NaN"`, `"1e999"`, booleans and containers are quarantined as `unparseable_value`, never zero. |
| Non-UTC offset | Same UTC instant and identity; action `timestamp_normalized_to_utc`. A timestamp without an offset is quarantined (`missing_timezone`). |
| `µg/m³`, `μg/m³`, `mg/m3` | Action `unit_alias_normalized` / `unit_converted_to_ug_m3`, only when a concentration exists. Received value and unit are retained. `ppm` etc. are flagged, not converted. |
| Claimed `event_id`/`revision_hash` | Must match the content **after** representation repair, otherwise `integrity_mismatch`. Absent hashes are allowed for local fixtures; the live path still requires them. |
| Bad UTF-8/JSON, non-object, `status: rejected`, missing field, other schema failure, non-PM2.5 | `quarantined` with a reason and a null identity, so it cannot retract an observation. |

`select_current_decisions` mirrors `current_measurements` (greatest
`(collected_at, revision_hash)`, then `ingested_at`) before usability filtering, so a
correction to null/negative removes an earlier usable value. A parity test checks
both against the fixture. Anomaly warnings, aggregation and hourly views are not
part of this slice.

## Building a replay

```sh
python -m streaming.quality_replay build --config my-quality.json \
    --snapshot data/frozen.jsonl --output-dir runs/<name>
```

Profiles are `control` (snapshot bytes unchanged), `single-fault` (exactly one
fault type) and `mixed-v1` (balanced round-robin over the configured faults).
`selected_input_fraction` applies to valid PM2.5 deliveries (to eligible ones for
`single-fault`); natural defects pass through unmutated. Selection uses
`sha256(seed, record, fault)`; the run ID, delivery IDs, release order and both
output files are byte-reproducible for the same snapshot, config and code. Outputs
are created exclusively and an existing run is never overwritten. Expected outcomes in
the manifest are written per fault, not computed by the rules, so the evaluator is
not circular. Publication is unimplemented.

### Group scenarios

Set `experiment.group_scenarios` (and optionally `group_fraction`, default 0.1) to a
list of `missing_interval` and/or `correction_to_null`. Groups are sensor/hour
pairs of valid hourly records, ranked by the same stable hash and chosen *before*
row faults; every record in a chosen group is reserved from the row profile, so no
compound faults arise. The `control` profile rejects scenarios; `flatline` is
rejected because it needs the deferred anomaly warnings.

| Scenario | Builder behavior | Manifest record |
| --- | --- | --- |
| `missing_interval` | Every delivery of the group is omitted; wrappers carry no trace of it. | `omitted_records` entry with the group and record numbers. |
| `correction_to_null` | Only groups whose current value is a non-negative, unflagged `ug/m3` number can be chosen. A later revision (`collected_at` +1 h, `value: null`, refreshed hash) is released after every other delivery. | `role: correction`, `fault: correction_to_null`, expected flag `missing_value` and the superseded delivery. |

## Evaluating a run

```sh
python -m streaming.quality_evaluation run --run-dir runs/<name> \
    --snapshot data/frozen.jsonl [--catalog catalog.json]
```

`run` processes the baseline snapshot and the replay through the shared rules
(`quality/local_run.py`, no manifest access), saves `baseline_results.json` and
`experiment_results.json` beside the run, and writes
`evaluation/evaluation.json`. Steps are also available separately
(`process-baseline`, `process-replay`, `evaluate`). Exit code 0 means every
assessable check passed, 1 that a check failed, 2 that inputs were unusable or
did not belong together (different rule version, snapshot or run, or a catalog
that differs from the one frozen in the manifest). Nothing is overwritten and the
report is byte-reproducible.

| Report section | What it measures |
| --- | --- |
| `accounting` | Planned deliveries equal accepted plus quarantined terminal decisions; missing or unexpected decisions are listed; omissions are counted separately. |
| `detection` | Per fault type TP/FP/FN, precision and recall from the manifest's expected signal. A zero denominator is `null`. Faults with no processing signal (delay) are `not_applicable` and scored by invariance. |
| `valid_control` | False positives among unlabeled deliveries whose baseline is clean, and any unlabeled delivery that differs from the baseline. Natural defects are reported under `natural_defects` and never scored. |
| `repair_correctness` | Repaired identity, period, usability and value against the baseline within `1e-9` ug/m3; wrong and unrepaired counts. |
| `logical_invariance` | Events that changed other than the destructive ones, destructive faults left usable, corrections that took effect, and stability over seeded delivery shuffles. |
| `coverage`, `gap_detection` | Expected/received/usable/missing sensor-hours for both runs, lost usable sensor-hours attributed to faults, and unexplained loss or gain. Gap precision/recall needs a catalog; without one it is `not_assessed` and expectations fall back to baseline observations. |
| `aggregate_effect` | Station-hour mean error where the baseline is fully recoverable; for affected hours, agreement with the baseline minus the destroyed events, plus hours lost. |

Lost data is reported, never reconstructed. `timeliness`, `forecast_robustness`
and `restart_recovery` are listed as not assessed until the Kafka/Spark path exists.

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

**Done:** steps 1–3 (contracts, profiling, shared rules, deterministic
single-fault/mixed generation, missing-interval and correction-to-null scenarios,
local processing and the saved evaluation report; tests in `tests/test_quality_*.py`).
**Next:** step 4, persistence and the isolated Kafka/Spark path (wrapper
dispatcher, experiment launcher with its own database and checkpoint, `publish_replay`).
Flatline scenarios wait for the deferred anomaly warnings; the evaluator's
timeliness and restart measurements wait for step 4.

Steps 1–3 needed no Kafka, Spark or public APIs. No placeholder should be
treated as a successful no-op: calling an unfinished operation fails explicitly.
Do not wire these functions into the current startup path until their behavior
and recovery checks pass.
