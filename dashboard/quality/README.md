# Quality panel placeholder

The React application is not implemented. This directory reserves the quality
panel's location without adding a framework or frontend dependencies.

Once the dashboard exists, build a panel using the planned
`GET /api/v1/quality/summary` endpoint:

- Show an explicit live or controlled-experiment/run label and active rule version.
- Display accepted/quarantined counts separately from overlapping repair, flag,
  duplicate and superseded attributes.
- Show known coverage, unknown completeness, source age and processing lag.
- Show bounded received/normalized examples and quarantine reasons with lineage.
- Compare baseline and experiment metrics from prepared evaluation reports;
  never send expected answers to processing workers.
- Keep suspected sensor anomalies visible as warnings, including high readings.

See [the implementation map](../../docs/data-quality-implementation.md) for
dependencies and build order. No API calls or fault injection run from this file.
