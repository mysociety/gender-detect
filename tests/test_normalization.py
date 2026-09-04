from __future__ import annotations

import pandas as pd

from mysoc_gender_detect import GenderDetect, normalize_names


def test_normalize_names_removes_punctuation_and_preserves_nulls() -> None:
    names = pd.Series(["Anne-Marie", "O'Neil", None, "A B", "under_score"])
    expected = pd.Series(["annemarie", "oneil", None, "a b", "under_score"])

    pd.testing.assert_series_equal(normalize_names(names), expected)
    pd.testing.assert_series_equal(GenderDetect.prepare_series(names), expected)
