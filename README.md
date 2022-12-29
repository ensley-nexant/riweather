# riweather

[![Tests](https://github.com/ensley-nexant/riweather/workflows/Tests/badge.svg)](https://github.com/ensley-nexant/riweather/actions?workflow=Tests)
[![Codecov](https://codecov.io/gh/ensley-nexant/riweather/branch/main/graph/badge.svg)](https://codecov.io/gh/ensley-nexant/riweather)
[![Release](https://github.com/ensley-nexant/riweather/actions/workflows/release.yml/badge.svg)](https://github.com/ensley-nexant/riweather/actions/workflows/release.yml)

Grab publicly available weather data with `riweather`. [See the full documentation](https://ensley-nexant.github.io/riweather).

## Installation

Install with pip:

```
pip install riweather
```

To create interactive maps of weather station locations, install the package along with its optional dependencies:

```
pip install riweather[plots]
```

## Usage

Given a latitude and longitude, get a list of weather stations sorted from nearest to farthest from that location.

```python
>>> import riweather

>>> station_rank = riweather.rank_stations(39.98, -105.13, max_distance_m=20000)
```

Select the top station (or a different station):

```python
>>> station = riweather.select_station(station_rank, rank=0)
```

View information about that station:

```python
>>> station.name, station.usaf_id
```

And pull weather data from that station for a certain year.

```python
>>> station.fetch_temp_data(2022)
```
