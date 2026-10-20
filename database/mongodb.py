"""The sole MongoDB read/write boundary for air-quality facts and crawl state.

No network polling, sleeping, Kafka publication or scheduling runs in this class.
Use regular collections so observation uniqueness and revision guards are explicit.
"""

import base64
import json
import os
from datetime import UTC, datetime, timedelta

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import CollectionInvalid, DuplicateKeyError

from database.documents import (
    city_document,
    geo_point,
    measurement_document,
    now,
    order_time,
)
from database.schema import INDEXES, VALIDATORS
from streaming.measurements import digest, timestamp, utc_time

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class MongoDB:
    def __init__(self, uri=None, database=None):
        self.client = MongoClient(
            uri or os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017"),
            serverSelectionTimeoutMS=5000,
            tz_aware=True,
            w=1,
            journal=True,
        )
        self.db = self.client[
            database or os.getenv("MONGO_DATABASE", "urban_air_quality")
        ]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self.client.close()

    def ping(self):
        return bool(self.client.admin.command("ping").get("ok"))

    def initialize(self):
        self.client.admin.command("ping")
        for collection, rule in VALIDATORS.items():
            try:
                self.db.create_collection(
                    collection, validator=rule, validationLevel="moderate"
                )
            except CollectionInvalid:
                self.db.command(
                    "collMod", collection, validator=rule, validationLevel="moderate"
                )
        for collection, definitions in INDEXES.items():
            for keys, options in definitions:
                self.db[collection].create_index(keys, **options)
        self.db.control.update_one(
            {"_id": "schema"},
            {
                "$set": {
                    "version": 2,
                    "initialized_at": now(),
                }
            },
            upsert=True,
        )

    # Catalog: no city-sized arrays, no invented cities for null provider locality.
    def save_parameter(self, parameter):
        identity = f"openaq:{parameter['id']}"
        self.db.parameters.update_one(
            {"_id": identity},
            {
                "$set": {
                    "schema_version": 2,
                    "source": "openaq",
                    "source_id": parameter["id"],
                    "name": parameter["name"],
                    "unit": parameter.get("units"),
                    "provider_payload": parameter,
                    "updated_at": now(),
                }
            },
            upsert=True,
        )
        return identity

    def save_station(self, payload, generation=None):
        identity = f"openaq:{payload['id']}"
        city = city_document(payload)
        if city:
            self.db.cities.update_one({"_id": city["_id"]}, {"$set": city}, upsert=True)
        document = {
            "_id": identity,
            "schema_version": 2,
            "source": "openaq",
            "source_id": payload["id"],
            "name": payload.get("name"),
            "country_code": (payload.get("country") or {}).get("code"),
            "city_id": city["_id"] if city else None,
            "locality": payload.get("locality"),
            "timezone": payload.get("timezone"),
            "is_mobile": payload.get("isMobile"),
            "is_monitor": payload.get("isMonitor"),
            "geo": geo_point(payload.get("coordinates")),
            "provider": payload.get("provider"),
            "licenses": payload.get("licenses"),
            "parameter_names": sorted(
                {sensor["parameter"]["name"] for sensor in payload.get("sensors", [])}
            ),
            "provider_payload": payload,
            "catalog_generation": generation,
            "last_seen_at": now(),
        }
        self.db.stations.update_one(
            {"_id": identity},
            {
                "$set": document,
                "$setOnInsert": {"first_seen_at": now()},
            },
            upsert=True,
        )
        for sensor in payload.get("sensors", []):
            self.save_sensor(sensor, document)
        return document

    def save_sensor(self, payload, station):
        identity = f"openaq:{payload['id']}"
        self.db.sensors.update_one(
            {"_id": identity},
            {
                "$set": {
                    "schema_version": 2,
                    "source": "openaq",
                    "source_id": payload["id"],
                    "station_id": station["_id"],
                    "location_id": station["source_id"],
                    "country_code": station["country_code"],
                    "city_id": station["city_id"],
                    "name": payload.get("name"),
                    "parameter": payload["parameter"],
                    "provider_payload": payload,
                    "last_seen_at": now(),
                },
                "$setOnInsert": {"first_seen_at": now()},
            },
            upsert=True,
        )
        return identity

    def get_station(self, station_id):
        return self.db.stations.find_one({"_id": station_id, "schema_version": 2})

    def get_sensor(self, sensor_id):
        return self.db.sensors.find_one({"_id": sensor_id, "schema_version": 2})

    @staticmethod
    def _limit(limit):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be 1..1000")
        return limit

    @staticmethod
    def _filters(**values):
        if any(
            value is not None and not isinstance(value, str)
            for value in values.values()
        ):
            raise ValueError("filters must be strings")
        return {key: value for key, value in values.items() if value is not None}

    def _page(self, collection, query, after=None, limit=100):
        self._limit(limit)
        if after is not None:
            if not isinstance(after, str):
                raise ValueError("after must be a document ID")
            query["_id"] = {"$gt": after}
        rows = list(self.db[collection].find(query).sort("_id", 1).limit(limit + 1))
        return {
            "items": rows[:limit],
            "next_cursor": rows[limit - 1]["_id"] if len(rows) > limit else None,
        }

    def list_cities(self, country_code=None, *, after=None, limit=100):
        return self._page(
            "cities", self._filters(country_code=country_code), after, limit
        )

    def list_stations(
        self, country_code=None, city_id=None, parameter=None, *, after=None, limit=100
    ):
        query = {
            "schema_version": 2,
            **self._filters(
                country_code=country_code, city_id=city_id, parameter_names=parameter
            ),
        }
        return self._page("stations", query, after, limit)

    def list_sensors(self, station_id=None, parameter=None, *, after=None, limit=100):
        return self._page(
            "sensors",
            {
                "schema_version": 2,
                **self._filters(station_id=station_id, **{"parameter.name": parameter}),
            },
            after,
            limit,
        )

    def iter_sensors(self):
        return (
            self.db.sensors.find({"schema_version": 2}).sort("_id", 1).batch_size(500)
        )

    def iter_parameters(self):
        return self.db.parameters.find({"schema_version": 2}).sort("source_id", 1)

    # Facts: revision history is immutable; guards make retries and stale writes safe.
    def _replace_if_newer(self, collection, identity, document, ordering):
        comparison = {"$lt": [{"$ifNull": ["$" + ordering, ""]}, document[ordering]]}
        self.db[collection].update_one(
            {"_id": identity},
            [
                {
                    "$replaceWith": {
                        "$cond": [
                            comparison,
                            {"$literal": {**document, "_id": identity}},
                            "$$ROOT",
                        ]
                    }
                }
            ],
            upsert=True,
        )

    def save_measurement(self, envelope):
        document = measurement_document(envelope)
        revision_id = digest([document["event_id"], document["revision_hash"]])
        revision = {**document, "_id": revision_id}
        self.db.measurement_revisions.update_one(
            {"_id": revision_id},
            {
                "$setOnInsert": revision,
            },
            upsert=True,
        )
        self._replace_if_newer(
            "measurements", document["_id"], document, "revision_order"
        )
        # Correcting an older hour must not displace the latest observation.
        latest = {
            **document,
            "latest_order": order_time(document["period_end"])
            + ":"
            + document["revision_order"],
        }
        self._replace_if_newer(
            "latest_measurements", document["sensor_id"], latest, "latest_order"
        )
        return document["event_id"]

    def get_latest(self, station_id, parameter=None, limit=100):
        self._limit(limit)
        return list(
            self.db.latest_measurements.find(
                self._filters(station_id=station_id, parameter=parameter)
            )
            .sort("sensor_id", 1)
            .limit(limit)
        )

    def get_source_latest(self, station_id, parameter=None, limit=100):
        self._limit(limit)
        return list(
            self.db.source_latest.find(
                self._filters(station_id=station_id, parameter=parameter)
            )
            .sort([("observed_at", -1), ("_id", 1)])
            .limit(limit)
        )

    def get_measurements(
        self,
        station_id,
        start,
        end,
        *,
        sensor_id=None,
        parameter=None,
        cursor=None,
        limit=100,
    ):
        self._limit(limit)
        if (
            not isinstance(start, datetime)
            or not isinstance(end, datetime)
            or not start.tzinfo
            or not end.tzinfo
        ):
            raise ValueError("start/end must be timezone-aware datetimes")
        if not start < end or end - start > timedelta(days=90):
            raise ValueError("measurement range must be positive and at most 90 days")
        query = {
            **self._filters(
                station_id=station_id, sensor_id=sensor_id, parameter=parameter
            ),
            "period_end": {"$gte": start, "$lt": end},
        }
        if cursor:
            try:
                time_value, identity = json.loads(
                    base64.urlsafe_b64decode(cursor.encode())
                )
                if not isinstance(identity, str):
                    raise ValueError("invalid cursor ID")
                after = utc_time(time_value, "cursor")
            except (ValueError, TypeError, UnicodeError) as error:
                raise ValueError("invalid measurement cursor") from error
            query["$or"] = [
                {"period_end": {"$gt": after}},
                {"period_end": after, "_id": {"$gt": identity}},
            ]
        rows = list(
            self.db.measurements.find(query)
            .sort([("period_end", 1), ("_id", 1)])
            .limit(limit + 1)
        )
        token = None
        if len(rows) > limit:
            last = rows[limit - 1]
            token = base64.urlsafe_b64encode(
                json.dumps([timestamp(last["period_end"]), last["_id"]]).encode()
            ).decode()
        return {"items": rows[:limit], "next_cursor": token}

    def get_revisions(self, event_id, limit=100):
        self._limit(limit)
        return list(
            self.db.measurement_revisions.find(self._filters(event_id=event_id))
            .sort("revision_order", 1)
            .limit(limit)
        )

    def save_latest_snapshot(self, payload, parameter, collected_at, response_id):
        sensor_id = f"openaq:{payload['sensorsId']}"
        observed = utc_time(payload["datetime"]["utc"], "latest.datetime")
        fingerprint = digest(payload)
        previous = self.db.source_latest.find_one({"_id": sensor_id})
        order = (
            order_time(observed) + ":" + order_time(collected_at) + ":" + fingerprint
        )
        changed = previous is None or (
            order >= previous["snapshot_order"]
            and fingerprint != previous["payload_hash"]
        )
        document = {
            "_id": sensor_id,
            "schema_version": 2,
            "source": "openaq",
            "sensor_id": sensor_id,
            "station_id": f"openaq:{payload['locationsId']}",
            "parameter": parameter["name"],
            "unit": parameter.get("units"),
            "value": payload["value"],
            "observed_at": observed,
            "collected_at": collected_at,
            "snapshot_order": order,
            "payload_hash": fingerprint,
            "provider_payload": payload,
            "raw_response_id": response_id,
            "details_pending": changed
            or bool(previous and previous.get("details_pending")),
        }
        self._replace_if_newer("source_latest", sensor_id, document, "snapshot_order")
        return document["details_pending"]

    def mark_latest_dispatched(self, sensor_id, payload):
        self.db.source_latest.update_one(
            {"_id": sensor_id, "payload_hash": digest(payload)},
            {"$set": {"details_pending": False}},
        )

    def save_manual_weather_location(self, location):
        self.db.weather_locations.update_one(
            {"_id": location["location_id"]},
            {
                "$set": {
                    "schema_version": 2,
                    "latitude": location["latitude"],
                    "longitude": location["longitude"],
                }
            },
            upsert=True,
        )

    def save_weather_location(self, station, precision=2):
        if not station.get("geo") or station.get("is_mobile") is not False:
            return None
        lon, lat = station["geo"]["coordinates"]
        # Explicit grid of requested coordinates; never label this as a real city center.
        lat, lon = round(lat, precision), round(lon, precision)
        identity = "open-meteo:" + digest([lat, lon])
        self.db.weather_locations.update_one(
            {"_id": identity},
            {
                "$set": {
                    "schema_version": 2,
                    "latitude": lat,
                    "longitude": lon,
                    "coordinate_precision": precision,
                    "updated_at": now(),
                }
            },
            upsert=True,
        )
        self.db.station_weather.update_one(
            {"_id": station["_id"]},
            {
                "$set": {
                    "schema_version": 2,
                    "station_id": station["_id"],
                    "weather_location_id": identity,
                }
            },
            upsert=True,
        )
        return identity

    def get_weather_location(self, location_id):
        document = self.db.weather_locations.find_one({"_id": location_id})
        return (
            {**document, "location_id": location_id, "station_ids": []}
            if document
            else None
        )

    def get_weather_location_for_station(self, station_id):
        mapping = self.db.station_weather.find_one({"_id": station_id})
        if mapping is None:
            return None
        location = self.get_weather_location(mapping["weather_location_id"])
        if location is not None:
            location["station_ids"] = [station_id]
        return location

    def save_weather(self, envelope):
        self.db.weather_vintages.update_one(
            {"_id": envelope["vintage_id"]},
            {
                "$setOnInsert": {
                    **envelope,
                    "schema_version": 2,
                    "valid_time": utc_time(envelope["valid_time"], "valid_time"),
                    "available_at": utc_time(envelope["available_at"], "available_at"),
                    "collected_at": utc_time(envelope["collected_at"], "collected_at"),
                    "ingested_at": utc_time(envelope["ingested_at"], "ingested_at"),
                }
            },
            upsert=True,
        )

    def get_weather(
        self, location_id, start, end, *, model=None, available_before=None, limit=100
    ):
        self._limit(limit)
        if (
            not start.tzinfo
            or not end.tzinfo
            or not start < end
            or end - start > timedelta(days=90)
        ):
            raise ValueError(
                "weather range must be aware, positive, and at most 90 days"
            )
        query = {
            **self._filters(location_id=location_id, model=model),
            "valid_time": {"$gte": start, "$lt": end},
        }
        if available_before is not None:
            if not available_before.tzinfo:
                raise ValueError("available_before must include a timezone")
            query["available_at"] = {"$lte": available_before}
        return list(
            self.db.weather_vintages.find(query)
            .sort([("valid_time", 1), ("available_at", -1)])
            .limit(limit)
        )

    # Raw capture/outbox are deliberately separate from source facts.
    def capture_response(self, response):
        self.db.raw_responses.insert_one(
            {
                "_id": response.request_id,
                "schema_version": 2,
                "source": response.source,
                "url": response.url,
                "params": response.params,
                "collected_at": response.collected_at,
                "collected_at_iso": timestamp(response.collected_at),
                "body": response.body,
                "headers": response.headers,
                "context": response.context,
                "staged_at": None if response.context else now(),
            }
        )

    def pending_responses(self, source=None):
        query = {"staged_at": None, "context": {"$ne": None}}
        if source:
            query["source"] = source
        return self.db.raw_responses.find(query).sort("collected_at", 1)

    def finish_response(self, request_id):
        self.db.raw_responses.update_one(
            {"_id": request_id}, {"$set": {"staged_at": now()}}
        )

    def enqueue(self, request_id, index, topic, key, envelope):
        identity = digest({"request_id": request_id, "index": index})
        self.db.collector_outbox.update_one(
            {"_id": identity},
            {
                "$setOnInsert": {
                    "schema_version": 2,
                    "topic": topic,
                    "key": key,
                    "envelope": envelope,
                    "event_id": envelope.get("event_id"),
                    "revision_hash": envelope.get("revision_hash"),
                    "raw_response_id": request_id,
                    "created_at": now(),
                    "published_at": None,
                }
            },
            upsert=True,
        )

    def pending_outbox(self, limit=1000):
        self._limit(limit)
        return (
            self.db.collector_outbox.find({"published_at": None})
            .sort([("created_at", 1), ("_id", 1)])
            .limit(limit)
        )

    def acknowledge_outbox(self, identity, topic, partition, offset):
        self.db.collector_outbox.update_one(
            {"_id": identity},
            {
                "$set": {
                    "published_at": now(),
                    "broker": {
                        "topic": topic,
                        "partition": partition,
                        "offset": offset,
                    },
                }
            },
        )

    def progress(self, key):
        return self.db.control.find_one({"_id": key})

    def set_state(self, key, **fields):
        self.db.control.update_one(
            {"_id": key}, {"$set": {**fields, "updated_at": now()}}, upsert=True
        )

    def complete(self, key, until, **details):
        self.db.control.update_one(
            {"_id": key},
            {
                "$max": {"completed_until": until},
                "$set": {
                    "updated_at": now(),
                    **details,
                },
            },
            upsert=True,
        )

    # Persistent request reservations: the caller decides whether/when to wait.
    def reserve_budget(self, scope, limits, units=1, current=None):
        current = current or now()
        try:
            self.db.api_budgets.update_one(
                {"_id": scope},
                {
                    "$setOnInsert": {
                        "version": 0,
                        "requests": [],
                        "blocked_until": EPOCH,
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            pass
        budget = self.db.api_budgets.find_one({"_id": scope})
        maximum_window = max(window for window, _ in limits)
        stamps = [
            stamp
            for stamp in budget["requests"]
            if stamp > current - timedelta(seconds=maximum_window)
        ]
        resume = budget["blocked_until"]
        for window, maximum in limits:
            if units > maximum:
                raise ValueError("request cost exceeds the configured API budget")
            recent = [
                stamp for stamp in stamps if stamp > current - timedelta(seconds=window)
            ]
            if len(recent) + units > maximum:
                resume = max(
                    resume,
                    recent[len(recent) + units - maximum - 1]
                    + timedelta(seconds=window),
                )
        if resume > current:
            return (resume - current).total_seconds() + 0.05
        result = self.db.api_budgets.update_one(
            {"_id": scope, "version": budget["version"]},
            {
                "$set": {"requests": sorted(stamps + [current] * units)},
                "$inc": {"version": 1},
            },
        )
        return None if result.modified_count else 0

    def defer_budget(self, scope, seconds):
        self.db.api_budgets.update_one(
            {"_id": scope},
            {
                "$max": {
                    "blocked_until": now() + timedelta(seconds=max(seconds, 0)),
                },
                "$inc": {"version": 1},
            },
        )

    # Durable jobs: leases prevent duplicate workers; generations retain in-flight triggers.
    def ensure_job(self, identity, source, kind, payload, priority=10):
        self.db.collector_jobs.update_one(
            {"_id": identity},
            {
                "$setOnInsert": {
                    "schema_version": 2,
                    "source": source,
                    "kind": kind,
                    "enabled": True,
                    "priority": priority,
                    "next_run_at": now(),
                    "lease_until": EPOCH,
                    "owner": None,
                    "generation": 0,
                    "failures": 0,
                    "checkpoint": {},
                    "created_at": now(),
                },
                "$set": {"payload": payload},
            },
            upsert=True,
        )

    def request_job(self, identity, priority=20):
        self.db.collector_jobs.update_one(
            {"_id": identity},
            {
                "$min": {"next_run_at": now()},
                "$inc": {"generation": 1},
                "$set": {"priority": priority},
            },
        )

    def claim_job(self, source, owner, lease_seconds=120, kinds=None):
        current = now()
        query = {
            "source": source,
            "enabled": True,
            "next_run_at": {"$lte": current},
            "lease_until": {"$lte": current},
        }
        if kinds:
            query["kind"] = {"$in": list(kinds)}
        return self.db.collector_jobs.find_one_and_update(
            query,
            {
                "$set": {
                    "owner": owner,
                    "lease_until": current + timedelta(seconds=lease_seconds),
                    "started_at": current,
                }
            },
            sort=[("priority", -1), ("next_run_at", 1), ("_id", 1)],
            return_document=ReturnDocument.AFTER,
        )

    def set_job_priority(self, source, kind, priority):
        self.db.collector_jobs.update_many(
            {"source": source, "kind": kind}, {"$set": {"priority": priority}}
        )

    def release_job(self, job):
        self.db.collector_jobs.update_one(
            {"_id": job["_id"], "owner": job["owner"]},
            {
                "$set": {
                    "owner": None,
                    "lease_until": EPOCH,
                    "next_run_at": now(),
                }
            },
        )

    def renew_job(self, identity, owner, lease_seconds=120):
        result = self.db.collector_jobs.update_one(
            {"_id": identity, "owner": owner, "lease_until": {"$gt": now()}},
            {
                "$set": {
                    "lease_until": now() + timedelta(seconds=lease_seconds),
                }
            },
        )
        return bool(result.matched_count)

    def checkpoint_job(self, job, checkpoint):
        result = self.db.collector_jobs.update_one(
            {"_id": job["_id"], "owner": job["owner"], "lease_until": {"$gt": now()}},
            {"$set": {"checkpoint": checkpoint}},
        )
        if not result.matched_count:
            raise RuntimeError("job lease lost before checkpoint")

    def finish_job(self, job, next_run_at, checkpoint=None, error=None):
        current = now()
        updates = {
            "owner": None,
            "lease_until": EPOCH,
            "last_finished_at": current,
            "last_error": error,
            "failures": job["failures"] + 1 if error else 0,
        }
        if checkpoint is not None:
            updates["checkpoint"] = checkpoint
        if not error:
            updates["last_success_at"] = current
        updates["next_run_at"] = {
            "$cond": [
                {"$gt": ["$generation", job["generation"]]},
                current,
                next_run_at,
            ]
        }
        priority = {
            "$cond": [
                {"$gt": ["$generation", job["generation"]]},
                "$priority",
                10 if job["kind"] == "sensor" else job["priority"],
            ]
        }
        result = self.db.collector_jobs.update_one(
            {"_id": job["_id"], "owner": job["owner"], "lease_until": {"$gt": current}},
            [
                {
                    "$set": {
                        **{
                            key: {"$literal": value}
                            for key, value in updates.items()
                            if key != "next_run_at"
                        },
                        "next_run_at": updates["next_run_at"],
                        "priority": priority,
                    }
                }
            ],
        )
        return bool(result.matched_count)

    def status(self):
        current = now()
        counts = {
            name: self.db[name].count_documents({})
            for name in (
                "cities",
                "stations",
                "sensors",
                "measurements",
                "measurement_revisions",
                "source_latest",
                "weather_locations",
                "weather_vintages",
            )
        }
        return {
            "database": self.db.name,
            "schema_version": 2,
            "counts": counts,
            "pending_records": self.db.collector_outbox.count_documents(
                {"published_at": None}
            ),
            "unstaged_responses": self.db.raw_responses.count_documents(
                {"staged_at": None, "context": {"$ne": None}}
            ),
            "jobs": list(
                self.db.collector_jobs.aggregate(
                    [
                        {
                            "$group": {
                                "_id": {"source": "$source", "kind": "$kind"},
                                "total": {"$sum": 1},
                                "due": {
                                    "$sum": {
                                        "$cond": [
                                            {
                                                "$and": [
                                                    "$enabled",
                                                    {"$lte": ["$next_run_at", current]},
                                                ]
                                            },
                                            1,
                                            0,
                                        ]
                                    }
                                },
                                "oldest_due_at": {"$min": "$next_run_at"},
                                "failures": {
                                    "$sum": {"$cond": [{"$gt": ["$failures", 0]}, 1, 0]}
                                },
                            }
                        }
                    ]
                )
            ),
            "catalog": self.progress("catalog:openaq"),
        }

    def iter_outbox_envelopes(self):
        return self.db.collector_outbox.find({}, {"envelope": 1}).batch_size(500)

    def iter_legacy_stations(self):
        return self.db.stations.find(
            {"schema_version": {"$ne": 2}, "metadata": {"$type": "object"}}
        ).batch_size(500)
