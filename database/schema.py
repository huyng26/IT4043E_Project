"""Versioned regular collections: uniqueness and corrections require explicit keys."""

SCHEMA_VERSION = 2


def validator(required, properties):
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id", "schema_version", *required],
            "properties": {
                "_id": {"bsonType": "string"},
                "schema_version": {"enum": [SCHEMA_VERSION]},
                **properties,
            },
        }
    }


VALIDATORS = {
    "cities": validator(["country_code", "name"], {"name": {"bsonType": "string"}}),
    "parameters": validator(["source", "source_id", "name"], {}),
    "stations": validator(["source", "source_id", "country_code", "city_id"], {}),
    "sensors": validator(["source", "source_id", "station_id", "parameter"], {}),
    "measurements": validator(
        [
            "event_id",
            "sensor_id",
            "station_id",
            "parameter",
            "period_start",
            "period_end",
            "value",
            "revision_order",
        ],
        {
            "period_start": {"bsonType": "date"},
            "period_end": {"bsonType": "date"},
            "value": {"bsonType": ["double", "int", "long", "null"]},
        },
    ),
    "measurement_revisions": validator(
        ["event_id", "revision_hash", "period_end"],
        {"period_end": {"bsonType": "date"}},
    ),
    "latest_measurements": validator(["sensor_id", "period_end", "revision_order"], {}),
    "source_latest": validator(
        ["sensor_id", "observed_at", "collected_at", "payload_hash"],
        {"observed_at": {"bsonType": "date"}},
    ),
    "weather_locations": validator(["latitude", "longitude"], {}),
    "station_weather": validator(["station_id", "weather_location_id"], {}),
    "weather_vintages": validator(
        ["location_id", "valid_time", "available_at", "model"],
        {"valid_time": {"bsonType": "date"}, "available_at": {"bsonType": "date"}},
    ),
    "collector_jobs": validator(
        ["source", "kind", "next_run_at", "generation", "enabled"],
        {"next_run_at": {"bsonType": "date"}},
    ),
}

# Names are stable so schema initialization can be rerun safely.
INDEXES = {
    "cities": [([("country_code", 1), ("name", 1), ("_id", 1)], {})],
    "parameters": [([("source", 1), ("source_id", 1)], {"unique": True})],
    "stations": [
        ([("country_code", 1), ("city_id", 1), ("_id", 1)], {}),
        ([("geo", "2dsphere")], {}),
    ],
    "sensors": [
        ([("station_id", 1), ("parameter.name", 1), ("_id", 1)], {}),
        (
            [("source", 1), ("source_id", 1)],
            {"unique": True, "partialFilterExpression": {"schema_version": 2}},
        ),
    ],
    "measurements": [
        ([("station_id", 1), ("parameter", 1), ("period_end", 1), ("_id", 1)], {}),
        ([("sensor_id", 1), ("period_end", 1), ("_id", 1)], {}),
    ],
    "measurement_revisions": [
        ([("event_id", 1), ("revision_hash", 1)], {"unique": True})
    ],
    "latest_measurements": [([("station_id", 1), ("parameter", 1)], {})],
    "source_latest": [([("station_id", 1), ("parameter", 1), ("observed_at", -1)], {})],
    "station_weather": [([("weather_location_id", 1), ("station_id", 1)], {})],
    "weather_vintages": [
        (
            [("location_id", 1), ("model", 1), ("valid_time", 1), ("available_at", -1)],
            {},
        )
    ],
    "raw_responses": [
        ([("source", 1), ("collected_at", 1)], {}),
        ([("staged_at", 1), ("source", 1), ("collected_at", 1)], {}),
    ],
    "collector_outbox": [
        ([("published_at", 1), ("created_at", 1), ("_id", 1)], {}),
        ([("event_id", 1), ("revision_hash", 1)], {}),
    ],
    "collector_jobs": [
        (
            [
                ("source", 1),
                ("enabled", 1),
                ("priority", -1),
                ("next_run_at", 1),
                ("_id", 1),
            ],
            {},
        )
    ],
    "control": [([("completed_until", 1)], {})],
}
