"""Public detector API."""

from __future__ import annotations

import pandas as pd

from .datasets import DatasetId, load_dataset, validate_dataset_id
from .normalization import normalize_names


class GenderDetect:
    """
    Infer the gender classification associated with first names.
    """

    male_value = "male"
    female_value = "female"
    unknown_value = "unknown"

    def __init__(
        self,
        country: str | DatasetId = DatasetId.uk_plus,
        threshold: float = 0.95,
        lower_threshold: float = 0.75,
    ) -> None:
        """
        Load a packaged lookup filtered by confidence thresholds.

        Comparisons remain strict for compatibility: rows exactly equal to a
        threshold are excluded.
        """
        self.country: DatasetId = validate_dataset_id(country)
        self.threshold = threshold
        self.lower_threshold = lower_threshold
        frame = load_dataset(self.country)
        selected = frame[
            (frame["winner_proportion"] > threshold)
            & (frame["lower"] > lower_threshold)
        ]
        self.lookup: dict[str, str] = dict(
            zip(selected["name"], selected["predicted"], strict=True)
        )

    def process_series(self, series: pd.Series, prepare: bool = True) -> pd.Series:
        """Map a pandas Series of names to ``male``, ``female``, or ``unknown``."""
        prepared = self.prepare_series(series) if prepare else series
        return (
            prepared.map(self.lookup)
            .fillna(self.unknown_value)
            .replace({"M": self.male_value, "F": self.female_value})
        )

    @classmethod
    def prepare_series(cls, series: pd.Series) -> pd.Series:
        """Lowercase names and remove punctuation."""
        return normalize_names(series)
