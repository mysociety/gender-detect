# Gender Detect

A python package to extract likely gender from name. 

This is an adaption of the approach from https://github.com/OpenGenderTracking/globalnamedata, and so is a spiritual sequel to https://github.com/malev/gender-detector.

This package is designed to work with pandas to process many names at once, and so has greater optimisiation of the individual lookup stage than alternative packages. 

Underlying datasets have been reprocessed and updated up to versions avaliable in 2019.  The default dataset used is 'uk_plus', which is a combination of UK datasets, using US data to fill in additional names without adjusting gender balances where already present. Alternative options are: `us`, `ni`, `ew`, `s`. 

Thresholds for allowing a gender with less than unanmity are adjustable. GenderDetect is subclassable to provide new methods for reducing the name to allow for key rather than name based lookups. 

## Usage

```
# basic lookup

gd = GenderDetect()
df["gender"] = gd.process_series(df["name"])


# allow lower threshold, use US data

gd = GenderDetect(country="us", threshold=0.8)
df["gender"] = gd.process_series(df["name"])

```


## Data sources

* https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/livebirths/datasets/babynamesinenglandandwalesfrom1996
* https://www.nisra.gov.uk/publications/baby-names-2016
* https://www.nrscotland.gov.uk/statistics-and-data/statistics/statistics-by-theme/vital-events/names/babies-first-names
* https://www.ssa.gov/oact/babynames/limits.html

