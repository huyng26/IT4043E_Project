"""Non-destructive, retry-safe projection of previously captured envelopes."""


def migrate_envelopes(repository):
    counts = {"stations": 0, "measurements": 0, "weather_vintages": 0, "skipped": 0}
    for row in repository.iter_legacy_stations():
        repository.save_station(row["metadata"])
        counts["stations"] += 1
    for row in repository.iter_outbox_envelopes():
        envelope = row["envelope"]
        if envelope.get("status") == "rejected":
            counts["skipped"] += 1
        elif (
            envelope.get("source") == "openaq"
            and "event_id" in envelope
            and "period_start" in envelope
        ):
            repository.save_measurement(envelope)
            counts["measurements"] += 1
        elif envelope.get("source") == "open-meteo" and "vintage_id" in envelope:
            repository.save_weather(envelope)
            counts["weather_vintages"] += 1
        else:
            counts["skipped"] += 1
    return counts
