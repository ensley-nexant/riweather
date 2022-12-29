"""riweather.

Grab publicly available weather data.
"""
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = "unknown"

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_dbpath = os.path.join(os.path.expanduser("~"), ".riweather")
try:
    os.mkdir(_dbpath)
except FileExistsError:
    pass
metadata_engine = create_engine(f"sqlite+pysqlite:///{_dbpath}/metadata.db")

from riweather.db import Base

Base.metadata.create_all(metadata_engine)
MetadataSession = sessionmaker(metadata_engine)

from riweather.connection import NOAAFTPConnection
from riweather.stations import rank_stations, select_station, Station, zcta_to_lat_lon
from riweather.viz import plot_stations
