from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Field, computed_field, field_validator


def missing_if_all_nines(v: str, *, scaling_factor: float = 1) -> str | int | float | None:
    if v is None:
        return None

    v = v.strip()
    if re.search(r"^[-+]?9+$", v):
        return None

    if scaling_factor != 1:
        return float(v) / scaling_factor

    return v


class ControlData(BaseModel):
    """Control data section.

    !!! usage
        [Control Data](../about/integrated_surface_dataset.md#control-data)
    """

    total_variable_characters: int
    """Total number of characters in the variable length section. The total record length equals
    105 plus the value stored in this field.
    """
    usaf_id: Annotated[str, Field(min_length=6, max_length=6, pattern=r"^\w*$")]
    """United States Air Force (USAF) Master Station Catalog identifier. For United States stations,
    this is a value between 720000 and 799999.
    """
    wban_id: Annotated[str, Field(min_length=5, max_length=5, pattern=r"^\d*$")]
    """NCEI Weather Bureau Army-Navy (WBAN) identifier."""
    dt: datetime
    """The date and time of the observation, based on Coordinated Universal Time (UTC)."""
    data_source_flag: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    """A flag indicating the source or combination of sources used in creating the observation.

    !!! note
        [Possible values](../about/shorthand_codes.md#controldatadata_source_flag)
    """
    latitude: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=1000.0))]
    """The latitude coordinate of the observation. Negative values are located in the southern hemisphere."""
    longitude: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=1000.0))]
    """The longitude coordinate of the observation. Negative values are located in the western hemisphere."""
    report_type_code: Annotated[str | None, Field(max_length=5), BeforeValidator(missing_if_all_nines)]
    """A flag indicating the type of geophysical surface observation.

    !!! note
        [Possible values](../about/shorthand_codes.md#controldatareport_type_code)
    """
    elevation: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    """The elevation of the observation relative to Mean Sea Level, in meters."""
    call_letter_id: Annotated[str | None, Field(max_length=5), BeforeValidator(missing_if_all_nines)]
    """The call letters associated with the station."""
    qc_process_name: Annotated[str, Field(max_length=4)]
    """Quality control process applied to the observation.

    !!! note
        [Possible values](../about/shorthand_codes.md#controldataqc_process_name)
    """

    @field_validator("dt", mode="before")
    @classmethod
    def parse_datetime(cls, value: Any) -> datetime:
        """Parse dates and times as they appear in ISD to a tz-aware datetime object.

        This is a [Pydantic validator](https://docs.pydantic.dev/latest/concepts/validators/), users
        shouldn't need to call this directly (though they can).

        Args:
            value: The portion of the record in the data file corresponding to the date and
                time fields, characters 16-27. This is always in the form `YYYYMMDDHHMM`.

        Returns:
            A timezone-aware datetime object representing the date and time, set to UTC.

        Examples:
            >>> ControlData.parse_datetime("201809220115")
            datetime.datetime(2018, 9, 22, 1, 15, tzinfo=datetime.timezone.utc)

            >>> ControlData.parse_datetime("201809310115")
            Traceback (most recent call last):
            ValueError: day is out of range for month
        """
        return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


class WindObservation(BaseModel):
    """An observation of current wind speed and direction."""

    direction_angle: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    """
    The angle, measured in angular degrees in a clockwise direction, between true north and the
    direction from which the wind is blowing.
    """
    direction_quality_code: Annotated[str, Field(max_length=1)]
    """Quality status of the reported [wind direction angle][riweather.parser.WindObservation.direction_angle].

    !!! note
        [Possible values](../about/shorthand_codes.md#quality-codes)
    """
    type_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    """Code that denotes the character of the wind observation.

    !!! note
        [Possible values](../about/shorthand_codes.md#windobservationtype_code)
    """
    speed_rate: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=10.0))]
    """The rate of horizontal travel of air past a fixed point, in meters per second."""
    speed_quality_code: Annotated[str, Field(max_length=1)]
    """Quality status of the reported [wind speed rate][riweather.parser.WindObservation.speed_rate].

    !!! note
        [Possible values](../about/shorthand_codes.md#quality-codes)
    """


class SkyConditionObservation(BaseModel):
    """An observation of current sky condition."""

    ceiling_height: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    """
    The height above ground level (AGL) of the lowest cloud or obscuring phenomena layer aloft
    with 5/8 or more summation total sky cover, which may be predominantly opaque, or the vertical
    visibility into a surface-based obstruction.

    !!! note
        Unlimited ceiling height coded as `22000`.
    """
    ceiling_quality_code: Annotated[str, Field(max_length=1)]
    """
    Quality status of the reported [ceiling height][riweather.parser.SkyConditionObservation.ceiling_height].

    !!! note
        [Possible values](../about/shorthand_codes.md#quality-codes)
    """
    ceiling_determination_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    """Code that denotes the method used to determine the ceiling.

    !!! note
        [Possible values](../about/shorthand_codes.md#skyconditionobservationceiling_determination_code)
    """
    cavok_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    """
    Code that represents whether the "Ceiling and Visibility Okay"
    ([CAVOK](https://www.globeair.com/g/clouds-and-visibility-ok-cavok)) condition has been reported.

    | Value | Description |
    | ----- | ----------- |
    | `N`   | No          |
    | `Y`   | Yes         |
    | `9`   | Missing     |
    """


class VisibilityObservation(BaseModel):
    """An observation of current sky visibility."""

    distance: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    """The horizontal distance, in meters, at which an object can be seen and identified.
    Values greater than 160,000m are entered as `160000`."""
    distance_quality_code: Annotated[str, Field(max_length=1)]
    """
    Quality status of the reported [visibility distance][riweather.parser.VisibilityObservation.distance].

    !!! note
        [Possible values](../about/shorthand_codes.md#quality-codes)
    """
    variability_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    """
    Code that denotes whether or not the reported visibility is variable.

    | Value | Description  |
    | ----- | ------------ |
    | `N`   | Not variable |
    | `V`   | Variable     |
    | `9`   | Missing      |
    """
    variability_quality_code: Annotated[str, Field(max_length=1)]
    """
    Quality status of the reported [variability code][riweather.parser.VisibilityObservation.variability_code].

    !!! note
        [Possible values](../about/shorthand_codes.md#quality-codes)
    """


class AirTemperatureObservation(BaseModel):
    """An observation of current air temperature."""

    temperature_c: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=10.0))]
    """Temperature in degrees Celsius."""
    quality_code: Annotated[str, Field(max_length=1)]
    """
    Quality status of the reported [temperature][riweather.parser.AirTemperatureObservation.temperature_c].

    !!! note
        [Possible values](../about/shorthand_codes.md#airtemperatureobservationquality_code)
    """

    @computed_field
    def temperature_f(self) -> float | None:
        """The temperature of the air, in degrees Fahrenheit."""
        return self.temperature_c * 1.8 + 32 if self.temperature_c is not None else None


class AtmosphericPressureObservation(BaseModel):
    """An observation of current atmospheric pressure."""

    pressure: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=10.0))]
    """Air pressure relative to Mean Sea Level (MSL)."""
    quality_code: Annotated[str, Field(max_length=1)]
    """
    Quality status of the reported [atmospheric pressure][riweather.parser.AtmosphericPressureObservation.pressure].

    !!! note
        [Possible values](../about/shorthand_codes.md#quality-codes)
    """


class MandatoryData(BaseModel):
    """Mandatory data section."""

    wind: WindObservation
    ceiling: SkyConditionObservation
    visibility: VisibilityObservation
    air_temperature: AirTemperatureObservation
    dew_point: AirTemperatureObservation
    sea_level_pressure: AtmosphericPressureObservation


class AdditionalData(BaseModel):
    """Additional data section.

    !!! warning
        Not yet implemented. Reserved for future use.
    """


class ISDRecord(BaseModel):
    """ISD data record."""

    control: ControlData
    mandatory: MandatoryData
    additional: Annotated[list[AdditionalData], Field(default_factory=list)]


def parse_line(line: str) -> ISDRecord:
    control_datum = {
        "total_variable_characters": line[0:4],
        "usaf_id": line[4:10],
        "wban_id": line[10:15],
        "dt": line[15:27],
        "data_source_flag": line[27],
        "latitude": line[28:34],
        "longitude": line[34:41],
        "report_type_code": line[41:46],
        "elevation": line[46:51],
        "call_letter_id": line[51:56],
        "qc_process_name": line[56:60],
    }
    mandatory_datum = {
        "wind": WindObservation.model_validate(
            {
                "direction_angle": line[60:63],
                "direction_quality_code": line[63],
                "type_code": line[64],
                "speed_rate": line[65:69],
                "speed_quality_code": line[69],
            }
        ),
        "ceiling": SkyConditionObservation.model_validate(
            {
                "ceiling_height": line[70:75],
                "ceiling_quality_code": line[75],
                "ceiling_determination_code": line[76],
                "cavok_code": line[77],
            }
        ),
        "visibility": VisibilityObservation.model_validate(
            {
                "distance": line[78:84],
                "distance_quality_code": line[84],
                "variability_code": line[85],
                "variability_quality_code": line[86],
            }
        ),
        "air_temperature": AirTemperatureObservation.model_validate(
            {
                "temperature_c": line[87:92],
                "quality_code": line[92],
            }
        ),
        "dew_point": AirTemperatureObservation.model_validate(
            {
                "temperature_c": line[93:98],
                "quality_code": line[98],
            }
        ),
        "sea_level_pressure": AtmosphericPressureObservation.model_validate(
            {
                "pressure": line[99:104],
                "quality_code": line[104],
            }
        ),
    }
    return ISDRecord(
        control=ControlData(**control_datum),
        mandatory=MandatoryData(**mandatory_datum),
        additional=[],
    )
