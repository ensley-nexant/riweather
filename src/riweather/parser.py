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
    total_variable_characters: int
    usaf_id: Annotated[str, Field(min_length=6, max_length=6, pattern=r"^\w*$")]
    wban_id: Annotated[str, Field(min_length=5, max_length=5, pattern=r"^\d*$")]
    datetime: datetime
    data_source_flag: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    latitude: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=1000.0))]
    longitude: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=1000.0))]
    report_type_code: Annotated[str | None, Field(max_length=5), BeforeValidator(missing_if_all_nines)]
    elevation: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    call_letter_id: Annotated[str | None, Field(max_length=5), BeforeValidator(missing_if_all_nines)]
    qc_process_name: Annotated[str, Field(max_length=4)]

    @field_validator("datetime", mode="before")
    @classmethod
    def parse_datetime(cls, value: Any) -> datetime:
        return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


class WindObservation(BaseModel):
    direction_angle: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    direction_quality_code: Annotated[str, Field(max_length=1)]
    type_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    speed_rate: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=10.0))]
    speed_quality_code: Annotated[str, Field(max_length=1)]


class SkyConditionObservation(BaseModel):
    ceiling_height_dimension: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    ceiling_quality_code: Annotated[str, Field(max_length=1)]
    ceiling_determination_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    cavok_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]


class VisibilityObservation(BaseModel):
    distance_dimension: Annotated[int | None, BeforeValidator(missing_if_all_nines)]
    distance_quality_code: Annotated[str, Field(max_length=1)]
    variability_code: Annotated[str | None, Field(max_length=1), BeforeValidator(missing_if_all_nines)]
    quality_variability_code: Annotated[str, Field(max_length=1)]


class AirTemperatureObservation(BaseModel):
    temperature_c: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=10.0))]
    quality_code: Annotated[str, Field(max_length=1)]

    @computed_field
    def temperature_f(self) -> float | None:
        return self.temperature_c * 1.8 + 32 if self.temperature_c is not None else None


class AtmosphericPressureObservation(BaseModel):
    pressure: Annotated[float | None, BeforeValidator(lambda x: missing_if_all_nines(x, scaling_factor=10.0))]
    quality_code: Annotated[str, Field(max_length=1)]


class MandatoryData(BaseModel):
    wind: WindObservation
    ceiling: SkyConditionObservation
    visibility: VisibilityObservation
    air_temperature: AirTemperatureObservation
    dew_point: AirTemperatureObservation
    sea_level_pressure: AtmosphericPressureObservation


class AdditionalData(BaseModel):
    pass


class ISDRecord(BaseModel):
    control: ControlData
    mandatory: MandatoryData
    additional: Annotated[list[AdditionalData], Field(default_factory=list)]


def parse_line(line: str) -> ISDRecord:
    control_datum = {
        "total_variable_characters": line[0:4],
        "usaf_id": line[4:10],
        "wban_id": line[10:15],
        "datetime": line[15:27],
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
                "ceiling_height_dimension": line[70:75],
                "ceiling_quality_code": line[75],
                "ceiling_determination_code": line[76],
                "cavok_code": line[77],
            }
        ),
        "visibility": VisibilityObservation.model_validate(
            {
                "distance_dimension": line[78:84],
                "distance_quality_code": line[84],
                "variability_code": line[85],
                "quality_variability_code": line[86],
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
