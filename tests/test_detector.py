from __future__ import annotations

import hashlib
import json
from importlib.resources import files

import pandas as pd
import pytest

from mysoc_gender_detect import GenderDetect
from mysoc_gender_detect.datasets import DatasetId, load_dataset


def test_known_unknown_null_and_punctuation() -> None:
    detector = GenderDetect()
    result = detector.process_series(
        pd.Series(["James", "Mary", "not-a-real-name", None, "Anne-Marie"])
    )

    assert result.iloc[:4].tolist() == ["male", "female", "unknown", "unknown"]
    assert result.iloc[4] in {"female", "unknown"}


def test_prepare_false_requires_normalized_input() -> None:
    detector = GenderDetect()

    assert (
        detector.process_series(pd.Series(["james"]), prepare=False).iloc[0] == "male"
    )
    assert (
        detector.process_series(pd.Series(["James"]), prepare=False).iloc[0]
        == "unknown"
    )


@pytest.mark.parametrize("dataset_id", DatasetId)
def test_every_advertised_dataset_loads(dataset_id: DatasetId) -> None:
    detector = GenderDetect(country=dataset_id)

    assert detector.lookup


def test_invalid_dataset_is_rejected_before_path_access() -> None:
    with pytest.raises(ValueError, match="Unknown dataset"):
        GenderDetect(country="../../etc/passwd")


def test_threshold_boundaries_are_strict() -> None:
    frame = load_dataset("uk_plus")
    row = frame.iloc[0]
    detector = GenderDetect(
        threshold=float(row["winner_proportion"]), lower_threshold=-1.0
    )

    assert row["name"] not in detector.lookup


def test_custom_thresholds_change_lookup_size() -> None:
    strict = GenderDetect(threshold=0.99, lower_threshold=0.9)
    permissive = GenderDetect(threshold=0.5, lower_threshold=0.0)

    assert len(strict.lookup) < len(permissive.lookup)


def test_packaged_dataset_hashes_match_manifest() -> None:
    data_root = files("mysoc_gender_detect").joinpath("data")
    manifest = json.loads(data_root.joinpath("manifest.json").read_text())

    for dataset_id, details in manifest["datasets"].items():
        content = data_root.joinpath(f"{dataset_id}.parquet").read_bytes()
        assert hashlib.sha256(content).hexdigest() == details["sha256"]
