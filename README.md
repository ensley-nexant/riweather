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

```pycon
>>> import riweather
>>> station_rank = riweather.rank_stations(39.98, -105.13, max_distance_m=20000)
```

Select the top station (or a different station):

```pycon
>>> station = riweather.select_station(station_rank, rank=0)
```

View information about that station:

```pycon
>>> station.name, station.usaf_id
('ERIE MUNICIPAL AIRPORT', '720534')
```

And pull weather data from that station for a certain year.

```pycon
>>> station.fetch_data(2024, "air_temperature", period="h", temp_scale="F", tz="US/Mountain")
                           air_temperature.temperature_f
2023-12-31 18:00:00-07:00                      33.176848
2023-12-31 19:00:00-07:00                      29.726000
2023-12-31 20:00:00-07:00                      25.415750
2023-12-31 21:00:00-07:00                      22.736750
2023-12-31 22:00:00-07:00                      20.876750
...                                                  ...
2024-12-31 13:00:00-07:00                      34.328000
2024-12-31 14:00:00-07:00                      35.445500
2024-12-31 15:00:00-07:00                      35.640500
2024-12-31 16:00:00-07:00                      34.249250
2024-12-31 17:00:00-07:00                      31.057455
[8784 rows x 1 columns]
```
