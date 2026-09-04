# Gender Detect

A Python package that infers the binary gender classification statistically associated
with a first name. Ambiguous and insufficiently supported names return `unknown`.
This describes an association in aggregate naming data and is best used for building aggregate analysis.

This is an adaption of the approach from https://github.com/OpenGenderTracking/globalnamedata, and so is a spiritual sequel to https://github.com/malev/gender-detector.

This package is designed to work with pandas and convert a series. 

The underlying datasets have been reprocessed and updated up to versions avaliable in 2019.  The default dataset used is 'uk_plus', which is a combination of UK datasets, using US data to fill in additional names without adjusting gender balances where already present. Alternative options are: `us`, `ni`, `ew`, `s`. 

The thresholds for allowing a gender guess with less than unanimity are adjustable. GenderDetect is subclassable to provide new methods for reducing the name and allowing for key rather than name based lookups. 

## Installation and use

```console
pip install mysoc-gender-detect
```

Gender Detect supports Python 3.10 through 3.14.

```python
import pandas as pd

from mysoc_gender_detect import GenderDetect

detector = GenderDetect()
names = pd.Series(["James", "Mary", "Unknown Name"])
print(detector.process_series(names))
```

`threshold` is the minimum winning proportion and `lower_threshold` is the minimum
normal-approximation lower bound. Both comparisons are strict. Names are lowercased
and punctuation is removed by default. Whitespace and underscores are retained.
Pass `prepare=False` only for input already normalized this way. Unknown, ambiguous,
null, or below-threshold names produce `"unknown"`.

## Command line

Look up one name and receive a JSON result:

```console
mysoc-gender-detect lookup James
# {"name": "James", "gender": "male"}
```

Add a `gender` column to a tabular file:

```console
mysoc-gender-detect convert people.csv classified.csv --name-column first_name
```

CSV, TSV, Parquet, JSON, and newline-delimited JSON are supported. Both commands
accept `--country`, `--threshold`, and `--lower-threshold`. The same CLI is available
with `python -m mysoc_gender_detect`.

## Data and provenance

The packaged files are immutable Parquet lookups generated from snapshots of:

- [England and Wales, ONS](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/livebirths/datasets/babynamesinenglandandwalesfrom1996), 1996-2019
- [Northern Ireland, NISRA](https://www.nisra.gov.uk/publications/baby-names-2016), 1997-2016
- [Scotland, NRS](https://www.nrscotland.gov.uk/statistics-and-data/statistics/statistics-by-theme/vital-events/names/babies-first-names), 1974-2019
- [United States, SSA](https://www.ssa.gov/oact/babynames/limits.html), 1950-2019

Exact hashes and coverage notes are in `src/mysoc_gender_detect/data/manifest.json`.
Source freshness is intentionally separate from the 0.2.0 storage migration.

To regenerate aggregate inputs from native snapshots, run
`python tools/aggregate_sources.py`; then run `python tools/build_datasets.py`.
Aggregate CSV files are temporary build artifacts and are not committed or shipped.

## Development

```console
uv sync --locked --all-groups
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
uv build
```

US Social Security Administration data is public domain. UK datasets use the Open
Government Licence. Unless otherwise stated, combined datasets are licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
