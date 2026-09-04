"""Validation and read-only loading of packaged lookup datasets."""

from __future__ import annotations

import json
from enum import StrEnum
from importlib.resources import files

import pandas as pd

REQUIRED_COLUMNS = ("name", "predicted", "winner_proportion", "lower")


class DatasetId(StrEnum):
    """Supported packaged datasets."""

    ew = "ew"
    ni = "ni"
    s = "s"
    uk = "uk"
    us = "us"
    uk_plus = "uk_plus"


def validate_dataset_id(value: str | DatasetId) -> DatasetId:
    """Return a supported dataset identifier or raise a useful error."""
    try:
        return DatasetId(value)
    except ValueError as error:
        supported = ", ".join(item.value for item in DatasetId)
        raise ValueError(
            f"Unknown dataset {value!r}; expected one of: {supported}"
        ) from error


def load_dataset(dataset_id: str | DatasetId) -> pd.DataFrame:
    """Load and validate one immutable package dataset."""
    validated_id = validate_dataset_id(dataset_id)
    data_root = files("mysoc_gender_detect").joinpath("data")
    manifest = json.loads(
        data_root.joinpath("manifest.json").read_text(encoding="utf-8")
    )
    if manifest.get("schema_version") != 1:
        raise RuntimeError("Unsupported packaged dataset schema")
    with data_root.joinpath(f"{validated_id.value}.parquet").open("rb") as parquet_file:
        frame = pd.read_parquet(parquet_file)
    if tuple(frame.columns) != REQUIRED_COLUMNS:
        raise RuntimeError(
            f"Packaged dataset {validated_id.value!r} has an invalid schema"
        )
    return frame
