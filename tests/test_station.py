from datetime import datetime, timezone

import pytest


def test_raw_data_returned(stn):
    data = stn.fetch_raw_data(2025)
    assert len(data) == 2559


def test_parser_control_data(stndata):
    first_record = stndata[0]
    assert first_record.control.model_dump() == pytest.approx(
        {
            "total_variable_characters": 185,
            "usaf_id": "720534",
            "wban_id": "00161",
            "dt": datetime(2025, 1, 1, 0, 15, tzinfo=timezone.utc),
            "data_source_flag": "4",
            "latitude": 40.017,
            "longitude": -105.05,
            "report_type_code": "FM-15",
            "elevation": 1564,
            "call_letter_id": None,
            "qc_process_name": "V020",
        }
    )


def test_parser_mandatory_data_wind(stndata):
    first_record = stndata[0]
    assert first_record.mandatory.wind.model_dump() == pytest.approx(
        {
            "direction_angle": 60,
            "direction_quality_code": "1",
            "type_code": "N",
            "speed_rate": 1.5,
            "speed_quality_code": "1",
        }
    )


def test_parser_mandatory_data_ceiling(stndata):
    first_record = stndata[0]
    assert first_record.mandatory.ceiling.model_dump() == pytest.approx(
        {
            "ceiling_height": 22000,
            "ceiling_quality_code": "5",
            "ceiling_determination_code": None,
            "cavok_code": "N",
        }
    )


def test_parser_mandatory_data_visibility(stndata):
    first_record = stndata[0]
    assert first_record.mandatory.visibility.model_dump() == pytest.approx(
        {
            "distance": 16093,
            "distance_quality_code": "1",
            "variability_code": None,
            "variability_quality_code": "9",
        }
    )


def test_parser_mandatory_data_air_temperature(stndata):
    first_record = stndata[0]
    assert first_record.mandatory.air_temperature.model_dump() == pytest.approx(
        {
            "temperature_c": -1.5,
            "temperature_f": 29.3,
            "quality_code": "1",
        }
    )


def test_parser_mandatory_data_dew_point(stndata):
    first_record = stndata[0]
    assert first_record.mandatory.dew_point.model_dump() == pytest.approx(
        {
            "temperature_c": -9.4,
            "temperature_f": 15.08,
            "quality_code": "1",
        }
    )


def test_parser_mandatory_data_sea_level_pressure(stndata):
    first_record = stndata[0]
    assert first_record.mandatory.sea_level_pressure.model_dump() == pytest.approx(
        {
            "pressure": None,
            "quality_code": "9",
        }
    )
