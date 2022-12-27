import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from riweather.db import Base
from riweather.db.models import File, Station, Zcta


@pytest.fixture(scope="module")
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    yield session
    session.rollback()
    session.close()


@pytest.fixture(scope="module")
def valid_zcta():
    zcta = Zcta(
        zip="35768",
        latitude=34.77851579407223,
        longitude=-86.10382679364129,
        county_id="01071",
        state="AL",
    )
    return zcta


@pytest.fixture(scope="module")
def valid_station():
    station = Station(
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
    return station


@pytest.fixture(scope="module")
def valid_file(valid_station):
    file = File(
        wban_id="93121",
        year=2006,
        size=316356,
        station=valid_station,
    )
    return file


class TestDatabase:
    def test_db_connection(self, session):
        assert session.is_active

    def test_db_add_valid_station(self, session, valid_station):
        session.add(valid_station)
        session.commit()
        station = (
            session.query(Station).filter_by(usaf_id=valid_station.usaf_id).first()
        )
        assert station == valid_station

    def test_db_add_valid_file(self, session, valid_file):
        session.add(valid_file)
        session.commit()
        file = session.query(File).filter_by(wban_id=valid_file.wban_id).first()
        assert file == valid_file

    def test_db_add_valid_zcta(self, session, valid_zcta):
        session.add(valid_zcta)
        session.commit()
        zcta = session.query(Zcta).filter_by(zip=valid_zcta.zip).first()
        assert zcta == valid_zcta
