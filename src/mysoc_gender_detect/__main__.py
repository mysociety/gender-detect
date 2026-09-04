"""Command-line interface for name lookup and tabular file conversion."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from .datasets import DatasetId
from .detector import GenderDetect

app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    help="Infer binary gender classifications associated with first names.",
)


def create_detector(
    country: DatasetId, threshold: float, lower_threshold: float
) -> GenderDetect:
    """Create a detector from shared command options."""
    return GenderDetect(
        country=country.value,
        threshold=threshold,
        lower_threshold=lower_threshold,
    )


def read_frame(path: Path) -> pd.DataFrame:
    """Read a supported tabular file according to its extension."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".tsv", ".tab"}:
        return pd.read_csv(path, sep="\t")
    if suffix == ".parquet":
        return pd.read_parquet(path, engine="pyarrow")
    if suffix in {".json", ".jsonl", ".ndjson"}:
        return pd.read_json(path, lines=suffix != ".json")
    supported = ".csv, .tsv, .tab, .parquet, .json, .jsonl, .ndjson"
    raise typer.BadParameter(f"Unsupported input format {suffix!r}; use {supported}")


def write_frame(frame: pd.DataFrame, path: Path) -> None:
    """Write a supported tabular file according to its extension."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame.to_csv(path, index=False)
        return
    if suffix in {".tsv", ".tab"}:
        frame.to_csv(path, sep="\t", index=False)
        return
    if suffix == ".parquet":
        frame.to_parquet(path, engine="pyarrow", index=False)
        return
    if suffix == ".json":
        frame.to_json(path, orient="records")
        return
    if suffix in {".jsonl", ".ndjson"}:
        frame.to_json(path, orient="records", lines=True)
        return
    supported = ".csv, .tsv, .tab, .parquet, .json, .jsonl, .ndjson"
    raise typer.BadParameter(f"Unsupported output format {suffix!r}; use {supported}")


@app.command()
def lookup(
    name: Annotated[str, typer.Argument(help="Name to look up.")],
    country: Annotated[DatasetId, typer.Option()] = DatasetId.uk_plus,
    threshold: Annotated[float, typer.Option(min=0.0, max=1.0)] = 0.95,
    lower_threshold: Annotated[float, typer.Option(min=0.0, max=1.0)] = 0.75,
) -> None:
    """Look up one individual name and print the result as JSON."""
    detector = create_detector(country, threshold, lower_threshold)
    gender = detector.process_series(pd.Series([name], dtype="string")).iloc[0]
    typer.echo(json.dumps({"name": name, "gender": gender}))


@app.command()
def convert(
    input_path: Annotated[
        Path,
        typer.Argument(exists=True, dir_okay=False, readable=True, resolve_path=True),
    ],
    output_path: Annotated[Path, typer.Argument(dir_okay=False, resolve_path=True)],
    name_column: Annotated[str, typer.Option(help="Column containing first names.")] = (
        "name"
    ),
    output_column: Annotated[str, typer.Option(help="Column to create.")] = "gender",
    country: Annotated[DatasetId, typer.Option()] = DatasetId.uk_plus,
    threshold: Annotated[float, typer.Option(min=0.0, max=1.0)] = 0.95,
    lower_threshold: Annotated[float, typer.Option(min=0.0, max=1.0)] = 0.75,
) -> None:
    """Add classifications to a CSV, TSV, Parquet, or JSON file."""
    frame = read_frame(input_path)
    if name_column not in frame.columns:
        raise typer.BadParameter(
            f"Input has no column named {name_column!r}", param_hint="--name-column"
        )
    detector = create_detector(country, threshold, lower_threshold)
    frame[output_column] = detector.process_series(frame[name_column])
    write_frame(frame, output_path)
    typer.echo(f"Wrote {len(frame):,} rows to {output_path}")


if __name__ == "__main__":
    app()
