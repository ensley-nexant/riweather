__version__ = "0.1.0"

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
