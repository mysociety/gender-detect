from __future__ import annotations

import pandas as pd

from tools.build_datasets import calculate_lookup


def test_calculate_lookup_aggregates_normalized_names() -> None:
    source = pd.DataFrame(
        {
            "name": ["Anne-Marie", "Anne Marie", "Anne-Marie"],
            "F": [8, 1, 2],
            "M": [0, 1, 0],
        }
    )

    result = calculate_lookup(source)

    assert result["name"].tolist() == ["anne marie", "annemarie"]
    assert result.loc[result["name"] == "annemarie", "predicted"].item() == "F"
