"""Test metadata-derived package attributes."""

from __future__ import annotations

from importlib.metadata import version

import pytest

import mysoc_gender_detect as package


def test_version_is_loaded_lazily_and_cached() -> None:
    """The package version should be read from installed distribution metadata."""
    package.__dict__.pop("__version__", None)

    package_version = package.__version__

    assert package_version == version("mysoc-gender-detect")
    assert package.__dict__["__version__"] is package_version


def test_unknown_module_attribute_raises_attribute_error() -> None:
    """Unknown attributes should retain normal module behavior."""
    with pytest.raises(AttributeError, match="has no attribute 'missing'"):
        _ = package.missing
