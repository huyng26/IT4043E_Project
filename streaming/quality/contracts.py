"""Placeholder for versioned replay wrappers and quality decision contracts.

TODO:
- Define/validate the wrapper fields in docs/data-quality.md, including exact
  payload bytes, run/delivery identity, original topic/key and replay timestamps.
- Define normalized output, repair actions, flags, terminal disposition and
  received/derived hashes without embedding expected answers or fault labels.
- Define manifest and summary shapes separately from processor input contracts.
- Preserve existing schema-v1 source envelopes and observation/revision ordering.

Constants below reserve names only; no wrapper/schema validation exists yet.
"""

REPLAY_TOPIC = "quality_replay_raw"
EXPERIMENT_DATASET_KIND = "controlled_corruption"
