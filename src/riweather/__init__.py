"""riweather.

Grab publicly available weather data.
"""
from importlib.metadata import version, PackageNotFoundError
from importlib.resources import files

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = "unknown"

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_dbpath = files("riweather.resources").joinpath("metadata.db")
metadata_engine = create_engine(f"sqlite+pysqlite:///{_dbpath}")

from riweather.db import Base

Base.metadata.create_all(metadata_engine)
MetadataSession = sessionmaker(metadata_engine)

from riweather.connection import NOAAFTPConnection
from riweather.stations import rank_stations, select_station, Station, zcta_to_lat_lon
from riweather.viz import plot_stations
