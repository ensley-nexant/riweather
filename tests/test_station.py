import pandas as pd
import pytest
import pytz


def test_fetch_data_default_args(stn):
    df = stn.fetch_data(2024)
    assert df.shape == (2559, 21)


def test_fetch_data_one_datum(stn):
    df = stn.fetch_data(2024, "air_temperature")
    assert df.shape == (2559, 3)
    assert all("air_temperature" in col for col in df.columns)


def test_fetch_data_invalid_datum(stn):
    with pytest.raises(ValueError, match="datum"):
        stn.fetch_data(2024, "invalid")


def test_fetch_data_multiple_years(stn):
    df = stn.fetch_data([2023, 2024], "air_temperature")
    assert df.shape == (5118, 3)


def test_fetch_data_multiple_datums(stn):
    df = stn.fetch_data(2024, ["wind", "air_temperature"])
    assert df.shape == (2559, 8)
    assert all("air_temperature" in col or "wind" in col for col in df.columns)


def test_fetch_data_resample(stn):
    df = stn.fetch_data(2024, "air_temperature", period="h")
    assert df.shape == (847, 2)
    assert sorted(df.columns) == ["air_temperature.temperature_c", "air_temperature.temperature_f"]
    assert df.index.freq == "h"
    assert df.index[1] - df.index[0] == pd.Timedelta(hours=1)


def test_fetch_data_invalid_rollup(stn):
    with pytest.raises(ValueError, match="rollup"):
        stn.fetch_data(2024, rollup="invalid")


def test_fetch_data_local_tz(stn):
    df = stn.fetch_data(2024, "air_temperature", tz="US/Mountain")
    assert df.shape == (2559, 3)
    assert df.index.tz == pytz.timezone("US/Mountain")


def test_fetch_data_include_control(stn):
    df = stn.fetch_data(2024, "air_temperature", include_control=True)
    assert df.shape == (2559, 13)
    assert "total_variable_characters" in df.columns


def test_fetch_data_no_include_quality_codes(stn):
    df = stn.fetch_data(2024, "air_temperature", include_quality_codes=False)
    assert df.shape == (2559, 2)
    assert "air_temperature.quality_code" not in df.columns


def test_fetch_data_temp_scale_c(stn):
    df = stn.fetch_data(2024, "dew_point", temp_scale="C")
    assert df.shape == (2559, 2)
    assert "dew_point.temperature_f" not in df.columns


def test_fetch_data_temp_scale_f(stn):
    df = stn.fetch_data(2024, "dew_point", temp_scale="F")
    assert df.shape == (2559, 2)
    assert "dew_point.temperature_c" not in df.columns


def test_fetch_data_model_dump_include(stn):
    df = stn.fetch_data(
        2024,
        model_dump_include={
            "air_temperature": {"temperature_f"},
            "dew_point": {"temperature_f"},
            "wind": {"speed_rate"},
        },
    )
    assert df.shape == (2559, 3)
    assert sorted(df.columns) == ["air_temperature.temperature_f", "dew_point.temperature_f", "wind.speed_rate"]


def test_fetch_data_model_dump_include_overrides_datum(stn):
    df = stn.fetch_data(
        2024,
        "visibility",
        model_dump_include={
            "air_temperature": {"temperature_f"},
            "dew_point": {"temperature_f"},
            "wind": {"speed_rate"},
        },
    )
    assert df.shape == (2559, 3)
    assert sorted(df.columns) == ["air_temperature.temperature_f", "dew_point.temperature_f", "wind.speed_rate"]


def test_fetch_data_model_dump_exclude(stn):
    df = stn.fetch_data(
        2024,
        model_dump_exclude={
            "air_temperature": {"temperature_c"},
            "dew_point": {"temperature_c"},
            "wind": {"direction_angle"},
            "sea_level_pressure": True,
        },
    )
    assert df.shape == (2559, 16)
    assert "wind.direction_angle" not in df.columns


def test_fetch_data_model_dump_exclude_overrides_datum(stn):
    df = stn.fetch_data(
        2024,
        "sea_level_pressure",
        model_dump_exclude={
            "air_temperature": {"temperature_c"},
            "dew_point": {"temperature_c"},
            "wind": {"direction_angle"},
            "sea_level_pressure": True,
        },
    )
    assert df.shape == (2559, 16)
    assert all("sea_level_pressure" not in col for col in df.columns)


def test_get_filenames_no_metadata(stn):
    with pytest.warns(UserWarning, match="not found"):
        fp = stn.get_filenames(2026)
    assert len(fp) == 1


def test_quality_report(stn):
    df = stn.quality_report()
    assert df.shape[1] == 18


def test_quality_report_one_year(stn):
    df = stn.quality_report(2024)
    assert df.shape == (18,)


def test_fetch_raw_temp_data_legacy(stn):
    with pytest.warns(DeprecationWarning):
        df = stn.fetch_raw_temp_data(2024)
    assert df.shape == (2559, 4)


def test_fetch_temp_data_legacy(stn):
    with pytest.warns(DeprecationWarning):
        df = stn.fetch_temp_data(2024)
    assert df.shape == (847, 4)
