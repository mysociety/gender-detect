from __future__ import annotations

import pandas as pd
from typer.testing import CliRunner

from mysoc_gender_detect.__main__ import app

runner = CliRunner()


def test_lookup_name_as_json() -> None:
    result = runner.invoke(app, ["lookup", "James"])

    assert result.exit_code == 0
    assert result.stdout == '{"name": "James", "gender": "male"}\n'


def test_lookup_unknown_name_as_json() -> None:
    result = runner.invoke(app, ["lookup", "not-a-real-name"])

    assert result.exit_code == 0
    assert result.stdout == ('{"name": "not-a-real-name", "gender": "unknown"}\n')


def test_convert_csv(tmp_path) -> None:
    input_path = tmp_path / "people.csv"
    output_path = tmp_path / "classified.csv"
    pd.DataFrame({"first_name": ["James", "Mary", None]}).to_csv(
        input_path, index=False
    )

    result = runner.invoke(
        app,
        [
            "convert",
            str(input_path),
            str(output_path),
            "--name-column",
            "first_name",
        ],
    )

    assert result.exit_code == 0
    converted = pd.read_csv(output_path)
    assert converted["gender"].tolist() == ["male", "female", "unknown"]


def test_convert_rejects_missing_name_column(tmp_path) -> None:
    input_path = tmp_path / "people.csv"
    output_path = tmp_path / "classified.csv"
    pd.DataFrame({"other": ["James"]}).to_csv(input_path, index=False)

    result = runner.invoke(app, ["convert", str(input_path), str(output_path)])

    assert result.exit_code == 2
    assert "Input has no column named 'name'" in result.output
