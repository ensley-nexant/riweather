"""Metadata database models."""

from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Zcta(Base):
    """ZCTA database model."""

    __tablename__ = "zcta"

    id = Column(Integer, primary_key=True)
    zip = Column(String(5))
    latitude = Column(Float)
    longitude = Column(Float)
    county_id = Column(String)
    state = Column(String(2))

    def __repr__(self):
        """Pretty print ZCTA."""
        return f"Zcta(id={self.id}, zip={self.zip}, latitude={self.latitude}, longitude={self.longitude}, state={self.state})"


class Station(Base):
    """Station database model."""

    __tablename__ = "station"

    id = Column(Integer, primary_key=True)
    usaf_id = Column(String, index=True)
    wban_ids = Column(String)
    recent_wban_id = Column(String(5))
    name = Column(String)
    icao_code = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)
    state = Column(String(2))

    filecounts = relationship("FileCount")


class FileCount(Base):
    """Observation count model."""

    __tablename__ = "filecount"

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey("station.id"))
    wban_id = Column(String)
    year = Column(Integer)
    jan = Column(Integer)
    feb = Column(Integer)
    mar = Column(Integer)
    apr = Column(Integer)
    may = Column(Integer)
    jun = Column(Integer)
    jul = Column(Integer)
    aug = Column(Integer)
    sep = Column(Integer)
    oct = Column(Integer)
    nov = Column(Integer)
    dec = Column(Integer)
    count = Column(Integer)
    n_zero_months = Column(Integer)
    quality = Column(String)

    station = relationship("Station", back_populates="filecounts")
