"""Name normalization shared by runtime and dataset generation."""

from __future__ import annotations

import pandas as pd


def normalize_names(series: pd.Series) -> pd.Series:
    """
    Lowercase names and remove punctuation.

    Null values remain null. Whitespace and underscores are retained for compatibility
    with the original normalization expression.
    """
    return series.str.lower().str.replace(r"[^\w\s]", "", regex=True)
