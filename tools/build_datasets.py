"""Build deterministic runtime parquet lookups from aggregate source tables."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATASET_IDS = ("ew", "ni", "s", "uk", "us", "uk_plus")
SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("predicted", pa.dictionary(pa.int8(), pa.string()), nullable=False),
        pa.field("winner_proportion", pa.float64(), nullable=False),
        pa.field("lower", pa.float64(), nullable=False),
    ]
)


def normalize_names(series: pd.Series) -> pd.Series:
    """Apply the canonical runtime name normalization."""
    return series.str.lower().str.replace(r"[^\w\s]", "", regex=True)


def calculate_lookup(source: pd.DataFrame) -> pd.DataFrame:
    """Aggregate a count table and calculate vectorized confidence values."""
    required = {"name", "F", "M"}
    if not required.issubset(source.columns):
        raise ValueError(f"Source columns must include {sorted(required)}")
    frame = source.loc[:, ["name", "F", "M"]].copy()
    frame["name"] = normalize_names(frame["name"])
    frame = frame.dropna(subset=["name"])
    frame = frame.groupby("name", as_index=False, sort=True)[["F", "M"]].sum()
    frame[["F", "M"]] = frame[["F", "M"]].astype("int64")
    total = frame["M"] + frame["F"]
    frame = frame.loc[total > 0].copy()
    total = frame["M"] + frame["F"]
    winner = frame[["M", "F"]].max(axis="columns")
    proportion = winner / total
    predicted = np.where(frame["F"] == winner, "F", "M")
    variance = total * proportion * (1.0 - proportion)
    lower = (winner - np.sqrt(variance) * 1.96) / total
    return pd.DataFrame(
        {
            "name": frame["name"],
            "predicted": predicted,
            "winner_proportion": proportion,
            "lower": lower,
        }
    ).sort_values("name", ignore_index=True)


def build(source_root: Path, output_root: Path) -> None:
    """Build all six packaged datasets and their provenance manifest."""
    output_root.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, object] = {
        "schema_version": 1,
        "generation_command": "python tools/build_datasets.py",
        "normalization": "lowercase; remove characters matching [^\\w\\s]",
        "sources": {
            "ew": {"coverage": "1996-2019", "publisher": "ONS"},
            "ni": {"coverage": "1997-2016", "publisher": "NISRA"},
            "s": {"coverage": "1974-2019", "publisher": "NRS"},
            "us": {"coverage": "1950-2019", "publisher": "US SSA"},
            "uk": {"coverage": "combined published UK sources"},
            "uk_plus": {"coverage": "UK with missing names filled from US"},
        },
        "datasets": {},
    }
    datasets = manifest["datasets"]
    assert isinstance(datasets, dict)
    metadata = {b"mysoc_gender_detect.schema_version": b"1"}
    for dataset_id in DATASET_IDS:
        source_path = source_root / f"{dataset_id}_all_time.csv"
        lookup = calculate_lookup(pd.read_csv(source_path))
        table = pa.Table.from_pandas(lookup, schema=SCHEMA, preserve_index=False)
        table = table.replace_schema_metadata(metadata)
        output_path = output_root / f"{dataset_id}.parquet"
        pq.write_table(table, output_path, compression="zstd", use_dictionary=True)
        datasets[dataset_id] = {
            "file": output_path.name,
            "rows": len(lookup),
            "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        }
    (output_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> None:
    """Parse command-line paths and build the packaged data."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root", type=Path, default=Path("data/processed"), help="CSV inputs"
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("src/mysoc_gender_detect/data"),
        help="Parquet output directory",
    )
    arguments = parser.parse_args()
    build(arguments.source_root, arguments.output_root)


if __name__ == "__main__":
    main()
