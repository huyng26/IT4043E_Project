"""Placeholder for cadence-aware coverage after current-revision selection."""

from collections.abc import Iterable, Mapping


def summarize_coverage(
    current_observations: Iterable[Mapping[str, object]],
    *,
    expected_sensor_hours: Iterable[Mapping[str, object]],
) -> dict[str, object]:
    """TODO: compare expected, received, usable and missing sensor-hours.

    Build expectations from known active inventory/cadence, not a fault manifest.
    Preserve unknown completeness as null, zero as valid, and absent measured
    concentration as null. Duplicate deliveries cannot improve completeness.
    """
    raise NotImplementedError("Cadence-aware quality coverage is not implemented")
