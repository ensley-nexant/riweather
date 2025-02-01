"""Grab publicly available weather data."""

from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = "unknown"

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_dbpath = files("riweather.resources").joinpath("metadata.db")
metadata_engine = create_engine(f"sqlite+pysqlite:///{_dbpath}")

from riweather.db import Base

Base.metadata.create_all(metadata_engine)
MetadataSession = sessionmaker(metadata_engine)

from riweather.connection import NOAAFTPConnection
from riweather.stations import (
    Station,
    rank_stations,
    rollup_ending,
    rollup_instant,
    rollup_midpoint,
    rollup_starting,
    select_station,
    upsample,
    zcta_to_lat_lon,
)
from riweather.viz import plot_stations
