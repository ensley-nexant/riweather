"""Test module for the metadata database."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from riweather.db import Base
from riweather.db.models import FileCount, Station, Zcta


@pytest.fixture(scope="module")
def session():
    """In-memory database session for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    yield session
    session.rollback()
    session.close()


@pytest.fixture(scope="module")
def valid_zcta():
    """A sample valid ZCTA database object."""
    return Zcta(
        zip="35768",
        latitude=34.77851579407223,
        longitude=-86.10382679364129,
        county_id="01071",
        state="AL",
    )


@pytest.fixture(scope="module")
def valid_station():
    """A sample valid Station database object."""
    return Station(
        usaf_id="690150",
        wban_ids="93121,99999",
        recent_wban_id="93121",
        name="TWENTY NINE PALMS",
        icao_code="KNXP",
        latitude=34.294,
        longitude=-116.147,
        elevation=610.5,
        state="CA",
    )


@pytest.fixture(scope="module")
def valid_filecount(valid_station):
    """A sample valid FileCount database object."""
    return FileCount(
        wban_id="93121",
        station=valid_station,
        year=2006,
        jan=493,
        feb=441,
        mar=518,
        apr=480,
        may=482,
        jun=518,
        jul=550,
        aug=509,
        sep=483,
        oct=587,
        nov=702,
        dec=732,
        count=6495,
        n_zero_months=0,
        quality="medium",
    )


class TestDatabase:
    """Test cases for the database."""

    def test_db_connection(self, session):
        """The connection was successful."""
        assert session.is_active

    def test_db_add_valid_station(self, session, valid_station):
        """A valid station can be inserted."""
        session.add(valid_station)
        session.commit()
        station = session.query(Station).filter_by(usaf_id=valid_station.usaf_id).first()
        assert station == valid_station

    def test_db_add_valid_file(self, session, valid_filecount):
        """A valid file can be inserted."""
        session.add(valid_filecount)
        session.commit()
        file = session.query(FileCount).filter_by(wban_id=valid_filecount.wban_id).first()
        assert file == valid_filecount

    def test_db_add_valid_zcta(self, session, valid_zcta):
        """A valid ZCTA can be inserted."""
        session.add(valid_zcta)
        session.commit()
        zcta = session.query(Zcta).filter_by(zip=valid_zcta.zip).first()
        assert zcta == valid_zcta
