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
| 4 | Persistence schema, wrapper dispatcher, `publish_replay`, isolated Kafka topic + Spark job + database/checkpoint | **Not started** |
| 5 | HDFS archive snapshots, batch rebuild with the same rules, speed/batch comparison | Not started (needs HDFS and batch from the main plan) |
| 6 | API/dashboard quality views, quality features for forecasting | Not started |

Everything done so far runs locally with the standard library only. **None of it is
wired into the live collector, Spark or MongoDB path**, and no real collected
snapshot has been profiled yet: all runs used synthetic fixtures
(`tests/quality_support.py`). Profiling a frozen real sample is still owed.

## Next step: step 4

1. Decide how non-PM2.5 pollutants are handled (see "Open decisions").
2. `streaming/quality/dispatcher.py`: decode `quality_replay_raw` wrappers and call
   `evaluate_payload`; project into an experiment-only database. Use the retry-safe
   keys already defined: `experiment_decision_key(run_id, delivery_id, rule_version)`.
3. `database/quality_schema.py` and repository methods through the existing
   `database.MongoDB`; do not add another client. Add migration/version handling.
4. `publish_replay` in `streaming/quality_replay.py` using `KafkaManager`: preserve
   delivery IDs and bytes on retry, honor `scheduled_release_index`, record ACKs.
5. A separately selected Spark entry point with its own checkpoint and database. Do
   not subscribe the live writer to the replay topic.
6. Extend `evaluate_run` with the measurements that need transport: timeliness and
   restart recovery (listed under `not_assessed` in the report), and assert counts
   after repeated publication.

Still placeholders (raise `NotImplementedError`): `streaming/quality/dispatcher.py`,
`streaming/quality_batch.py`, `database/quality_schema.py`, `api/quality.py`,
`ml/quality_features.py`, `publish_replay`. Flatline scenarios are rejected on
purpose: they need the deferred anomaly warnings.

## Run and test

```sh
python -m unittest discover -s tests          # 115 tests, 2 skipped without collector deps
cp config/quality.example.json data/outputs/my-quality.json   # set "enabled": true in the copy
python -m streaming.quality_replay build --config data/outputs/my-quality.json \
    --snapshot <snapshot.jsonl> --output-dir data/outputs/<run>
python -m streaming.quality_evaluation run --run-dir data/outputs/<run> \
    --snapshot <snapshot.jsonl> [--catalog <catalog.json>]
```

- Python 3.11 or newer is required. The conda env `huync` was upgraded from 3.10 to
  3.12 for this. If the `conda` shell function fails with a permission error, call
  `/usr/bin/conda` or `~/.conda/envs/huync/bin/python` directly.
- Tests use `unittest` only. Generated runs belong in the git-ignored `data/outputs/`.
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

## Open decisions

- **Non-PM2.5 pollutants.** `rules-v1` quarantines them as `unsupported_pollutant`
  because `MeasurementEvent` is PM2.5-only, but the live path stores them. Before
  rules are applied to live data, make them accepted-with-flag instead.
- **Absent hashes.** The rules accept records without `event_id`/`revision_hash`
  (fixtures); the live path requires them. Decide whether the dispatcher keeps that
  leniency.
- **Real snapshot.** Freeze and profile a real collected sample (with its catalog)
  before reporting any figure as an OpenAQ data-quality result.
