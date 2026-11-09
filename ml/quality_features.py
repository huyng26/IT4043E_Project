"""Placeholder for missingness/age features in the future MLlib pipeline.

No models, Spark dependencies or training jobs are loaded by this module.
"""

from datetime import datetime


def build_quality_features(hourly_dataset: object, *, forecast_origin: datetime) -> object:
    """TODO: add coverage, missingness and input-age columns using past-only data.

    Use only data available by forecast_origin and retain weather vintage rules.
    Exclude controlled corruption from primary training/evaluation. Keep measured
    history and targets unimputed; fit any feature imputer on training data only
    inside the future MLlib pipeline and reuse it for validation/test/inference.
    """
    raise NotImplementedError("Quality-aware forecasting features are not implemented")
