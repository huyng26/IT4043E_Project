# Dirty-data extension: progress and handoff

Read this first when continuing the quality work. It records what is finished,
what is next, and the rules that are easy to break. The design is in
[data-quality.md](data-quality.md); file-by-file status is in
[data-quality-implementation.md](data-quality-implementation.md). Update the
"Progress" table and "Next step" below whenever a step lands.

## Progress

| Step | Scope | State |
| --- | --- | --- |
| 1 | Contracts (wrapper, decision, manifest), snapshot profiling, coverage arithmetic | **Done**, local only |
| 2 | Shared rules `rules-v1`, seeded single-fault / `mixed-v1` / control replay builder | **Done** |
| 3 | `missing_interval` and `correction_to_null` scenarios, manifest-free local processing, evaluator and saved report | **Done** |
| 4 | Persistence schema, wrapper dispatcher, `publish_replay`, isolated Kafka topic + Spark job + database/checkpoint, transport grading | **Done**, verified end to end with compose |
| 5 | HDFS archive snapshots, batch rebuild with the same rules, speed/batch comparison | Not started (needs HDFS and batch from the main plan) |
| 6 | API/dashboard quality views, quality features for forecasting | Not started |

Steps 1–3 run with the standard library only; step 4 uses Kafka, Spark and MongoDB
through the compose `quality` profile and is unit-tested with fakes
(`tests/test_quality_transport.py`). **None of it is wired into the live collector,
Spark or MongoDB path.**

### Step 4 verification (2026-11-25)

Both runs went through real Kafka and Spark (`quality-spark` container) and were
graded with `spark-run`. Spark's stored decisions were identical to local
processing in both.

* **Synthetic** (72 records, all 9 faults + both group scenarios, 76 deliveries,
  published twice): every check passed, including the 5 transport checks.
  152 Kafka records gave 76 decisions; 76 redeliveries were counted, not duplicated.
* **Real sample** (3,000 `air_quality_raw` envelopes frozen from the collector
  outbox, 2,996 deliveries): passed. The job was killed after 745 decisions and
  restarted from its checkpoint: 2,996 decisions, none lost or duplicated. A second
  publication while the job ran: latency p50 9.9 s, p95 16.7 s, max 18.5 s with a
  2 s trigger and 300 records per batch (includes queueing behind that limit).
* **Real-data findings:** every stored envelope has `source_flagged` because it was
  collected before commit `5bf84c9` (the old collector flagged
  `flagInfo {"hasFlags": false}`), so nothing in the sample is usable and the
  usable-value scores are trivial. `supported_unit_variation`, `unsupported_unit`
  and `correction_to_null` selected nothing: the injector expects the unit spelled
  `ug/m3` and an unflagged value, while real rows use `µg/m³`. 76% of the sample is
  not PM2.5 (now accepted with `unsupported_pollutant`).

## Next step

Step 5 (HDFS archive and batch rebuild) needs HDFS and the batch layer from the main
plan, which do not exist yet. Useful work before that:

1. **Real envelopes re-derived (done 2026-11-25).** `python -m collectors.regenerate`
   reran the fixed adapter over all 21,477 captured pages: 548,177 envelopes (same
   count and `event_id`s as the outbox), 6,820 genuinely flagged instead of all.
   Output: `data/outputs/real/regenerated.jsonl` (683 MB, git-ignored, SHA-256 in
   its `.report.json`). The same 3,000 records re-derived
   (`snapshot-3000-regenerated.jsonl`) give 234 usable PM2.5 observations instead
   of 0; a local mixed run on them passed with 46 lost sensor-hours, all explained.
   Still owed: a catalog for that sample.
2. **Stale outbox rows repaired (done 2026-11-25).** All 541,357 unpublished
   flagged rows were rewritten in place by `python -m collectors.regenerate
   --repair-outbox` (original timestamps and lineage kept, marked
   `repair: openaq-hasflags-false-5bf84c9`). None had been published. Backup before
   the change: `data/outputs/backups/collector_outbox_air_quality_raw-before-flag-repair.archive.gz`
   (95 MB, verified by a full test restore); restore with
   `mongorestore --gzip --archive` if ever needed.
3. **injector-v2 (done 2026-11-25).** Unit faults and `correction_to_null` also start
   from `µg/m³`/`μg/m³`. On the regenerated real sample all 10 fault types fire and
   score precision/recall 1.0 (`data/outputs/e2e/real-v2-run`). v1 stays the
   default when a config names no version.
4. **Automate restart recovery** (currently a manual stop/start, see the
   implementation map) and add a freeze command instead of the one-off export.
5. Live quality writes for the four live topics are not designed yet; keep them
   separate from the experiment collections.

Still placeholders (raise `NotImplementedError`): `streaming/quality_batch.py`,
`api/quality.py`, `ml/quality_features.py`. Flatline scenarios are rejected on
purpose: they need the deferred anomaly warnings.

## Run and test

```sh
python -m unittest discover -s tests          # 178 tests; collector/pymongo tests skip without deps
cp config/quality.example.json data/outputs/my-quality.json   # set "enabled": true in the copy
python -m streaming.quality_replay build --config data/outputs/my-quality.json \
    --snapshot <snapshot.jsonl> --output-dir data/outputs/<run>
python -m streaming.quality_evaluation run --run-dir data/outputs/<run> \
    --snapshot <snapshot.jsonl> [--catalog <catalog.json>]

# step 4, through Kafka and Spark (experiment database only):
docker compose --profile quality up -d --wait mongodb kafka
docker compose --profile quality up kafka-init
python -m streaming.quality_replay publish --run-dir data/outputs/<run>
docker compose --profile quality run --rm quality-spark --run-id <run_id> --available-now
python -m streaming.quality_evaluation spark-run --run-dir data/outputs/<run> \
    --snapshot <snapshot.jsonl> --database urban_air_quality_quality
```

- Python 3.11 or newer is required. The conda env `huync` was upgraded from 3.10 to
  3.12 for this. If the `conda` shell function fails with a permission error, call
  `/usr/bin/conda` or `~/.conda/envs/huync/bin/python` directly.
- Tests use `unittest` only. Generated runs belong in the git-ignored `data/outputs/`.
  `huync` now has `pymongo`, `kafka-python`, `requests` and `python-dotenv`; Spark
  runs only inside the `urban-air-quality-spark:local` image.
- Real data lives in `urban_air_quality_collector.collector_outbox` (the serving
  database was never populated). Step 4's verification runs are in
  `data/outputs/e2e/` and the frozen sample in `data/outputs/real/`.
- Building or evaluating refuses to overwrite an existing run; use a new directory.
- Reading the report (`<run>/evaluation/evaluation.json`): `passed` is true when no
  assessable check failed; a `null` check (for example `corrections_effective`
  without a correction scenario) means it was not applicable, not that it passed.

## Rules that are easy to break

- **Keep `config/quality.example.json` at `"enabled": false`.** A test asserts the
  example cannot run. Copy it, never edit it in place.
- **Processing must never see answers.** Wrappers contain only transport fields;
  fault labels, original values and expected outcomes live only in `manifest.json`.
  `rules.py`, `contracts.py`, `coverage.py`, `profiling.py`, `local_run.py` and the
  dispatcher must not import `quality_evaluation` or `quality_replay` (tested).
- **Expected outcomes are hand-written per fault** in `quality_replay.py`, not
  computed by the rules, so the evaluator is not grading the rules with themselves.
- **Selection is deterministic**: `sha256(seed, record, fault)`, never Python `hash()`.
  Same snapshot, config and code give byte-identical output.
- **Revision selection happens before usability filtering** (greatest
  `(collected_at, revision_hash)`, then `ingested_at`), so a null correction removes an
  earlier usable value. Quarantined records have a null identity and retract nothing.
- Repairs are representation-only (numeric string, UTC offset, unit alias/mg/m3).
  Ambiguous values are quarantined, never turned into zero.
- `missing_value` and `unsupported_unit` are both fault names and legitimate
  quality-flag names, so they cannot be used as markers when testing for label leaks.
- Mutation-check new evaluator logic: break it on purpose and confirm a test fails.
- **The experiment database must never be a live one.** `check_experiment_database`
  and `initialize_quality` refuse live names and databases with live collections;
  the quality job reads only `quality_replay*` topics and never the live checkpoint.
  Do not add the replay topic to `streaming.processing.TOPICS`.
- Decisions are insert-once by `(run_id, delivery_id, rule_version)`; receipts by
  Kafka topic/partition/offset. Do not "update" a stored decision: a different
  payload under the same delivery ID is a `conflicting_redelivery` rejection.

## Open decisions

- **Non-PM2.5 pollutants: decided.** Accepted with flag `unsupported_pollutant`,
  never usable (matches the live path). Revisit when other pollutants get value rules.
- **Absent hashes.** The rules accept records without `event_id`/`revision_hash`
  (fixtures); the live path requires them. Decide whether the dispatcher keeps that
  leniency.
- **Real snapshot.** Use `snapshot-3000-regenerated.jsonl` (or a new sample from
  `regenerated.jsonl`), not `snapshot-3000.jsonl`, which predates the flag fix. Add
  its catalog before reporting coverage or gap figures as OpenAQ results.
- **Stale outbox rows: resolved** (see "Next step", item 2).
