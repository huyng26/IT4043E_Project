"""Global catalog ingestion: every API location, including missing city metadata."""

from collectors.storage import now
from streaming.measurements import digest


def register_parameter(store, parameter):
    store.save_parameter(parameter)
    store.ensure_job(
        f"openaq:latest:{parameter['id']}",
        "openaq",
        "latest",
        {"parameter": parameter},
        priority=30,
    )


def register_station(store, payload, config, generation=None):
    station = store.save_station(payload, generation)
    for sensor in payload.get("sensors", []):
        store.ensure_job(
            f"openaq:sensor:{sensor['id']}",
            "openaq",
            "sensor",
            {
                "sensor_id": sensor["id"],
                "location_id": payload["id"],
            },
        )
    if config["weather"]["enabled"]:
        location_id = store.save_weather_location(
            station, config["weather"]["coordinate_precision"]
        )
        if location_id:
            store.ensure_job(
                f"weather:{location_id}",
                "open-meteo",
                "weather",
                {"location_id": location_id},
            )
    return station


def sync_catalog(client, store, config, job, page_budget=1):
    """One bounded unit of a resumable complete global scan, without geographic filters."""
    checkpoint = dict(job["checkpoint"])
    phase = checkpoint.get("phase", "parameters")
    page_number = checkpoint.get("next_page", 1)
    generation = checkpoint.get("generation", job["_id"] + ":" + now().isoformat())
    count = checkpoint.get("records", 0)
    used = 0
    for response in client.pages(
        phase,
        start_page=page_number,
        previous_fingerprint=checkpoint.get("last_page_hash"),
        **({"order_by": "id", "sort_order": "asc"} if phase == "locations" else {}),
    ):
        for payload in response.payload["results"]:
            if phase == "parameters":
                register_parameter(store, payload)
            else:
                register_station(store, payload, config, generation)
            count += 1
        page_number = response.payload.get("meta", {}).get("page", page_number) + 1
        checkpoint = {
            "phase": phase,
            "next_page": page_number,
            "generation": generation,
            "records": count,
            "last_page_hash": digest(response.payload["results"]),
        }
        store.checkpoint_job(job, checkpoint)
        used += 1
        # A page is committed before yielding quota/time to other due sensors.
        if used >= page_budget and not is_last_page(response, client.page_size):
            return checkpoint, False
        if is_last_page(response, client.page_size):
            break
    if phase == "parameters":
        return {
            "phase": "locations",
            "next_page": 1,
            "generation": generation,
            "records": count,
        }, False
    store.complete(
        "catalog:openaq", now(), generation=generation, records=count, scope="global"
    )
    return {}, True


def is_last_page(response, default_limit):
    meta = response.payload.get("meta") or {}
    rows = response.payload["results"]
    found, limit, page = (
        meta.get("found"),
        meta.get("limit", default_limit),
        meta.get("page", 1),
    )
    return (
        not rows or len(rows) < limit or (type(found) is int and page * limit >= found)
    )
