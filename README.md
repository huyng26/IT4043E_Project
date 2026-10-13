# Real-Time Urban Air Quality Monitoring

IT4043E project for monitoring and forecasting urban air quality using OpenAQ, Open-Meteo, Kafka, Spark, HDFS, MongoDB, Spark MLlib, GraphFrames, FastAPI, and Kubernetes.

## Structure

```text
IT4043E_Project/
├── README.md
├── docs/              # Project documentation
├── config/            # Application settings
├── collectors/        # OpenAQ and weather data collection
├── streaming/         # Validation, cleaning, and aggregation
├── ml/                # Training and prediction
├── api/               # FastAPI service
├── dashboard/         # Dashboard application
├── infra/             # Service and database configuration
├── tests/              # Tests and fixtures
└── data/              # Small local samples
```

The repository currently contains the project plan and directory structure. Keep small samples in `data/`; store credentials, generated models, checkpoints, and large datasets outside Git. Use root-level dependency and configuration files as implementation begins.

Start with the [detailed system design](docs/system-design.md), which records the agreed Lambda architecture, required technologies, Spark assessment coverage, and implementation milestones. It supersedes the preliminary choices in the [implementation plan](docs/implementation-plan.md), [architecture](docs/architecture.md), and [data contracts](docs/data-contracts.md).
