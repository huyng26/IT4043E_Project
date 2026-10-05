# Simple implementation plan

## First working version

Start with one city, a few active stations, and PM2.5 monitoring and forecasting. Keep the other pollutants in the event design and add them as source coverage allows. Use MinIO locally, Docker Compose for infrastructure, XGBoost after a persistence baseline, and one dashboard technology initially.

The goal is one complete path: collect data → Kafka → Spark → ClickHouse → FastAPI → dashboard. Add raw archiving and forecasting to this path in small steps.

## Implementation steps

| Step | Folder | Work | Done when |
| --- | --- | --- | --- |
| 1. Agree settings and start infrastructure | `config/`, `infra/` | Choose stations, units, AQI standard, polling cadence, and event keys. Add Compose for Kafka, Spark, ClickHouse, and MinIO, plus topics, buckets, and table SQL. | Services start and simple publish/read/write checks pass. |
| 2. Collect and archive | `collectors/` | Add OpenAQ and Open-Meteo polling with pagination, retries, and durable progress. Add an independent Kafka consumer that archives raw events to MinIO. | Both topics receive events and the archive contains replayable data. |
| 3. Process and store | `streaming/` | Add one Spark job for validation, unit normalization, deduplication, hourly aggregation, supported AQI, and ClickHouse writes. | Sample events produce expected rows; duplicate and late events have defined behavior. |
| 4. Serve monitoring data | `api/`, `dashboard/` | Add latest readings, station history, and weather endpoints. Build one dashboard showing station selection, charts, and data freshness. | A user can inspect current and historical data for a station. |
| 5. Add forecasting | `ml/` | Prepare chronological datasets and lag features. Evaluate persistence and XGBoost, run hourly inference for horizons 1–24, and write predictions to ClickHouse. Extend the API and dashboard. | Forecasts are visible and MAE/RMSE are reported by horizon against the baseline. |
| 6. Verify and document | `tests/`, `docs/` | Check the complete flow, restart/retry behavior, raw replay, missing data, and stale forecasts. Document setup and demo steps. | The project can be started and demonstrated from written instructions. |

Write relevant checks as each component is implemented. API/dashboard work may use fixtures before ingestion is complete; ML training needs sufficient historical data first.

## Keep implementation small

- Use a few files inside each component folder; avoid extra service layers and shared-library packages initially.
- Keep configuration in `config/` and deployment files in `infra/`. Use environment variables for secrets.
- Put polling and archiving entry points together in `collectors/`, but run the archive consumer independently.
- Keep training, evaluation, and prediction code in `ml/`; use one feature-preparation implementation for training and inference.
- Choose React or Grafana for the first dashboard. Add the other only if needed.
- Add LightGBM, LSTM/TCN, extra cities, and complex orchestration after the first complete version works.

## Essential correctness checks

- Preserve original payloads, source units, event IDs, observation times, and ingestion times.
- Expect repeated delivery; test retry-safe database/archive writes and processing recovery.
- Record the selected AQI standard and required averaging periods. Display unavailable AQI when coverage is insufficient.
- Use chronological evaluation. Features must have been available at the forecast origin; future weather features require the appropriate forecast vintage.
- Show source freshness separately from processing delay. API polling cannot make delayed upstream observations instantaneous.
- Keep raw archives in MinIO/S3. Use `data/` for small local samples and outputs, with ignore rules before generating large or sensitive files.

## Source references

OpenAQ documents API v3, sensor measurement endpoints, API-key authentication, and rate limits. Use these references when implementing the collector: [API overview](https://docs.openaq.org/about/about), [measurements](https://docs.openaq.org/resources/measurements), [API keys](https://docs.openaq.org/using-the-api/api-key), [rate limits](https://docs.openaq.org/using-the-api/rate-limits).

Open-Meteo provides coordinate-based weather model output. Preserve its provenance and distinguish historical weather from forecast inputs available at prediction time: [weather API](https://open-meteo.com/en/docs), [historical forecast API](https://open-meteo.com/en/docs/historical-forecast-api).

All implementation steps remain pending; this repository is a scaffold.
