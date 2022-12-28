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
        return "Zcta(id={0}, zip={1}, latitude={2}, longitude={3}, state={4})".format(
            self.id, self.zip, self.latitude, self.longitude, self.state
        )


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

    files = relationship("File")


class File(Base):
    """File database model."""

    __tablename__ = "file"

    id = Column(Integer, primary_key=True)
    wban_id = Column(String)
    year = Column(Integer)
    size = Column(Integer)

    station_id = Column(Integer, ForeignKey("station.id"))
    station = relationship("Station", back_populates="files")
