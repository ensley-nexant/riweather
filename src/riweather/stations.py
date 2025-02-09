"""Weather station operations."""

from __future__ import annotations

import operator
import warnings
from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pyproj
import pytz
from pandas.tseries.frequencies import to_offset
from sqlalchemy import select

from riweather import MetadataSession, parser
from riweather.connection import NOAAFTPConnection, NOAAHTTPConnection
from riweather.db import models
from riweather.parser import ISDRecord, MandatoryData

if TYPE_CHECKING:
    from pydantic.main import IncEx

__all__ = (
    "zcta_to_lat_lon",
    "rank_stations",
    "select_station",
    "Station",
    "upsample",
    "rollup_starting",
    "rollup_ending",
    "rollup_midpoint",
    "rollup_instant",
)

_AGGREGABLE_FIELDS = {
    "wind": {"speed_rate"},
    "ceiling": {"ceiling_height"},
    "visibility": {"distance"},
    "air_temperature": {"temperature_c", "temperature_f"},
    "dew_point": {"temperature_c", "temperature_f"},
    "sea_level_pressure": {"pressure"},
}


def _parse_temp(s: bytes) -> float:
    return float(s) / 10.0 if s.decode("utf-8") != "+9999" else float("nan")


def upsample(data: pd.Series | pd.DataFrame, period: str = "min") -> pd.Series | pd.DataFrame:
    """Upsample and interpolate time series data.

    Args:
        data: Time series data with a datetime index
        period: Period to upsample to. Defaults to `"min"`, which is minute-level

    Returns:
        Upsampled data

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range(
        ...         "2023-01-01 00:01",
        ...         "2023-01-01 01:05",
        ...         freq="32min",
        ...     ),
        ... )
        >>> upsample(t)
        2023-01-01 00:01:00     1.00000
        2023-01-01 00:02:00     1.03125
        2023-01-01 00:03:00     1.06250
        2023-01-01 00:04:00     1.09375
        2023-01-01 00:05:00     1.12500
                                 ...
        2023-01-01 01:01:00     9.00000
        2023-01-01 01:02:00     9.25000
        2023-01-01 01:03:00     9.50000
        2023-01-01 01:04:00     9.75000
        2023-01-01 01:05:00    10.00000
        Freq: min, Length: 65, dtype: float64

        You can upsample to a different frequency if you want.

        >>> upsample(t, period="5min")
        2023-01-01 00:00:00     1.000000
        2023-01-01 00:05:00     1.166667
        2023-01-01 00:10:00     1.333333
        2023-01-01 00:15:00     1.500000
        2023-01-01 00:20:00     1.666667
        2023-01-01 00:25:00     1.833333
        2023-01-01 00:30:00     2.000000
        2023-01-01 00:35:00     3.142857
        2023-01-01 00:40:00     4.285714
        2023-01-01 00:45:00     5.428571
        2023-01-01 00:50:00     6.571429
        2023-01-01 00:55:00     7.714286
        2023-01-01 01:00:00     8.857143
        2023-01-01 01:05:00    10.000000
        Freq: 5min, dtype: float64
    """
    return data.resample(period).mean().interpolate(method="linear", limit=60, limit_direction="both")


def rollup_starting(
    data: pd.Series | pd.DataFrame, period: str = "h", *, upsample_first: bool = True
) -> pd.Series | pd.DataFrame:
    """Roll up data, labelled with the period start.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `"h"`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to calculating
            the period average.

    Returns:
        The time series rolled up to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range(
        ...         "2023-01-01 00:01",
        ...         "2023-01-01 01:05",
        ...         freq="32min",
        ...     ),
        ... )
        >>> t
        2023-01-01 00:01:00     1
        2023-01-01 00:33:00     2
        2023-01-01 01:05:00    10
        Freq: 32min, dtype: int64

        By default, the data are [upsampled][riweather.upsample] to minute-level before aggregation.

        >>> rollup_starting(t)
        2023-01-01 00:00:00    3.207627
        2023-01-01 01:00:00    9.375000
        Freq: h, dtype: float64

        The above is equivalent to upsampling and then aggregating with [Pandas `resample()`][pandas.Series.resample]:

        >>> x = upsample(t, period="min")
        >>> x
        2023-01-01 00:01:00     1.00000
        2023-01-01 00:02:00     1.03125
        2023-01-01 00:03:00     1.06250
        2023-01-01 00:04:00     1.09375
        2023-01-01 00:05:00     1.12500
                                 ...
        2023-01-01 01:01:00     9.00000
        2023-01-01 01:02:00     9.25000
        2023-01-01 01:03:00     9.50000
        2023-01-01 01:04:00     9.75000
        2023-01-01 01:05:00    10.00000
        Freq: min, Length: 65, dtype: float64
        >>> x.resample("h").mean()
        2023-01-01 00:00:00    3.207627
        2023-01-01 01:00:00    9.375000
        Freq: h, dtype: float64

        To skip upsampling and aggregate raw values only, use `upsample_first=False`.

        >>> rollup_starting(t, upsample_first=False)
        2023-01-01 00:00:00     1.5
        2023-01-01 01:00:00    10.0
        Freq: h, dtype: float64
    """
    if upsample_first:
        data = upsample(data, period="min")
    return data.resample(period, label="left", closed="left").mean()


def rollup_ending(
    data: pd.Series | pd.DataFrame, period: str = "h", *, upsample_first: bool = True
) -> pd.Series | pd.DataFrame:
    """Roll up data, labelled with the period end.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `"h"`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to calculating
            the period average.

    Returns:
        The time series rolled up to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range(
        ...         "2023-01-01 00:01",
        ...         "2023-01-01 01:05",
        ...         freq="32min",
        ...     ),
        ... )
        >>> t
        2023-01-01 00:01:00     1
        2023-01-01 00:33:00     2
        2023-01-01 01:05:00    10
        Freq: 32min, dtype: int64

        By default, the data are [upsampled][riweather.upsample] to minute-level before aggregation.

        >>> rollup_ending(t)
        2023-01-01 01:00:00    3.3
        2023-01-01 02:00:00    9.5
        Freq: h, dtype: float64

        The above is equivalent to upsampling and then aggregating with [Pandas `resample()`][pandas.Series.resample]:

        >>> x = upsample(t, period="min")
        >>> x
        2023-01-01 00:01:00     1.00000
        2023-01-01 00:02:00     1.03125
        2023-01-01 00:03:00     1.06250
        2023-01-01 00:04:00     1.09375
        2023-01-01 00:05:00     1.12500
                                 ...
        2023-01-01 01:01:00     9.00000
        2023-01-01 01:02:00     9.25000
        2023-01-01 01:03:00     9.50000
        2023-01-01 01:04:00     9.75000
        2023-01-01 01:05:00    10.00000
        Freq: min, Length: 65, dtype: float64
        >>> x.resample("h", label="right", closed="right").mean()
        2023-01-01 01:00:00    3.3
        2023-01-01 02:00:00    9.5
        Freq: h, dtype: float64

        To skip upsampling and aggregate raw values only, use `upsample_first=False`.

        >>> rollup_ending(t, upsample_first=False)
        2023-01-01 01:00:00     1.5
        2023-01-01 02:00:00    10.0
        Freq: h, dtype: float64
    """
    if upsample_first:
        data = upsample(data, period="min")
    return data.resample(period, label="right", closed="right").mean()


def rollup_midpoint(
    data: pd.Series | pd.DataFrame, period: str = "h", *, upsample_first: bool = True
) -> pd.Series | pd.DataFrame:
    """Roll up data, labelled with the period midpoint.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `"h"`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to calculating
            the period average.

    Returns:
        The time series rolled up to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range(
        ...         "2023-01-01 00:01",
        ...         "2023-01-01 01:05",
        ...         freq="32min",
        ...     ),
        ... )
        >>> t
        2023-01-01 00:01:00     1
        2023-01-01 00:33:00     2
        2023-01-01 01:05:00    10
        Freq: 32min, dtype: int64

        By default, the data are [upsampled][riweather.upsample] to minute-level before aggregation.

        >>> rollup_midpoint(t)
        2023-01-01 00:00:00    1.437500
        2023-01-01 01:00:00    5.661458
        Freq: h, dtype: float64

        The above is equivalent to upsampling, [shifting][pandas.Series.shift] the data forward
        by half of the period, and then aggregating with [Pandas `resample()`][pandas.Series.resample]:

        >>> x = upsample(t, period="min")
        >>> x
        2023-01-01 00:01:00     1.00000
        2023-01-01 00:02:00     1.03125
        2023-01-01 00:03:00     1.06250
        2023-01-01 00:04:00     1.09375
        2023-01-01 00:05:00     1.12500
                                 ...
        2023-01-01 01:01:00     9.00000
        2023-01-01 01:02:00     9.25000
        2023-01-01 01:03:00     9.50000
        2023-01-01 01:04:00     9.75000
        2023-01-01 01:05:00    10.00000
        Freq: min, Length: 65, dtype: float64
        >>> x.shift(freq="30min").resample("h").mean()
        2023-01-01 00:00:00    1.437500
        2023-01-01 01:00:00    5.661458
        Freq: h, dtype: float64

        To skip upsampling and aggregate raw values only, use `upsample_first=False`.

        >>> rollup_midpoint(t, upsample_first=False)
        2023-01-01 00:00:00    1.0
        2023-01-01 01:00:00    6.0
        Freq: h, dtype: float64
    """
    if upsample_first:
        data = upsample(data, period="min")
    half_period = to_offset(period) / 2
    return data.shift(freq=half_period).resample(period, label="left", closed="left").mean()


def rollup_instant(
    data: pd.Series | pd.DataFrame, period: str = "h", *, upsample_first: bool = True
) -> pd.Series | pd.DataFrame:
    """Roll up data, labelled with interpolated values.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `h`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to returning
            a value.

    Returns:
        The time series aligned to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range(
        ...         "2023-01-01 00:01",
        ...         "2023-01-01 01:05",
        ...         freq="32min",
        ...     ),
        ... )
        >>> t
        2023-01-01 00:01:00     1
        2023-01-01 00:33:00     2
        2023-01-01 01:05:00    10
        Freq: 32min, dtype: int64

        By default, the data are [upsampled][riweather.upsample] to minute-level before aggregation.

        >>> rollup_instant(t)
        2023-01-01 00:00:00    1.00
        2023-01-01 01:00:00    8.75
        Freq: h, dtype: float64

        The above is equivalent to upsampling and then aggregating with [Pandas `resample()`][pandas.Series.resample],
        but instead of taking the mean over each time period, taking the first value:

        >>> x = upsample(t, period="min")
        >>> x
        2023-01-01 00:01:00     1.00000
        2023-01-01 00:02:00     1.03125
        2023-01-01 00:03:00     1.06250
        2023-01-01 00:04:00     1.09375
        2023-01-01 00:05:00     1.12500
                                 ...
        2023-01-01 01:01:00     9.00000
        2023-01-01 01:02:00     9.25000
        2023-01-01 01:03:00     9.50000
        2023-01-01 01:04:00     9.75000
        2023-01-01 01:05:00    10.00000
        Freq: min, Length: 65, dtype: float64
        >>> x.resample("h").first()
        2023-01-01 00:00:00    1.00
        2023-01-01 01:00:00    8.75
        Freq: h, dtype: float64

        To skip upsampling and aggregate raw values only, use `upsample_first=False`. Notice that
        this is simply the first value in every time period (hour by default).

        >>> rollup_instant(t, upsample_first=False)
        2023-01-01 00:00:00     1
        2023-01-01 01:00:00    10
        Freq: h, dtype: int64
    """
    if upsample_first:
        data = upsample(data, period="min")
    return data.resample(period, label="left", closed="left").first()


class Station:
    """ISD Station object.

    Examples:
        >>> s = Station("720534")
        >>> s
        Station("720534")
        >>> print(s.name, s.latitude, s.longitude)
        ERIE MUNICIPAL AIRPORT 40.017 -105.05
    """

    def __init__(self, usaf_id: str, *, load_metadata_on_init: bool = True) -> None:
        """Initialize a station.

        Args:
            usaf_id: USAF identifier
            load_metadata_on_init: If `True`, station metadata will be retrieved
                from the local data store and loaded into the object as
                properties.
        """
        self.usaf_id = usaf_id

        if load_metadata_on_init:
            self._station = self._load_metadata()
        else:
            self._station = {}

    def _load_metadata(self) -> dict:
        """Retrieve station metadata from the local data store.

        Returns:
            A Station SQLAlchemy object.
        """
        stmt = select(models.Station).where(models.Station.usaf_id == self.usaf_id)
        with MetadataSession() as session:
            station = session.scalars(stmt).first()
            station_info = {col: getattr(station, col) for col in station.__table__.columns.keys()}  # noqa: SIM118
            station_info["years"] = [f.year for f in station.filecounts]

        return station_info

    @property
    def wban_ids(self) -> list[str]:
        """List of valid WBAN (Weather Bureau Army Navy) identifiers."""
        return self._station.get("wban_ids", "").split(",")

    @property
    def recent_wban_id(self) -> str:
        """Most recent WBAN (Weather Bureau Army Navy) identifier."""
        return self._station.get("recent_wban_id")

    @property
    def name(self) -> str:
        """Station name.

        Examples:
            >>> s = Station("720534")
            >>> s.name
            'ERIE MUNICIPAL AIRPORT'
        """
        return self._station.get("name")

    @property
    def icao_code(self) -> str:
        """ICAO airport code."""
        return self._station.get("icao_code")

    @property
    def latitude(self) -> float:
        """Station latitude."""
        return self._station.get("latitude")

    @property
    def longitude(self) -> float:
        """Station longitude."""
        return self._station.get("longitude")

    @property
    def elevation(self) -> float:
        """Elevation of the station, in meters."""
        return self._station.get("elevation")

    @property
    def state(self) -> str:
        """US state in which the station is located.

        Examples:
            >>> s = Station("720534")
            >>> s.state
            'CO'
        """
        return self._station.get("state")

    @property
    def years(self) -> list[int]:
        """Years for which data exists for the station."""
        return self._station.get("years", [])

    def get_filenames(self, year: int | None = None) -> list[str]:
        """Construct the names of ISD files corresponding to this station.

        Args:
            year: Limit the filenames to the one corresponding to the given year.
                If `None`, filenames for all years are returned.

        Returns:
            List of filenames

        Examples:
            >>> s = Station("720534")
            >>> print(s.get_filenames(2022))
            ['/pub/data/noaa/2022/720534-00161-2022.gz']
        """
        stmt = select(models.FileCount).where(models.FileCount.station_id == self._station.get("id"))
        if year is not None:
            stmt = stmt.where(models.FileCount.year == year)

        filename_template = "/pub/data/noaa/{2}/{0}-{1}-{2}.gz"
        filenames = []
        with MetadataSession() as session:
            for row in session.scalars(stmt):
                filenames.append(  # noqa: PERF401
                    filename_template.format(self.usaf_id, row.wban_id, row.year)
                )

        if len(filenames) == 0:
            filenames = [filename_template.format(self.usaf_id, self.recent_wban_id, year)]
            msg = (
                "A record for station {} and year {} was not found in riweather's metadata. "
                "Trying to fetch data directly from the following URL, which may not exist: {}"
            ).format(self._station.get("usaf_id"), year, filenames[0])
            warnings.warn(msg, UserWarning, stacklevel=3)

        return filenames

    def quality_report(self, year: int | None = None) -> pd.DataFrame | pd.Series:
        """Retrieve information on data quality.

        Args:
            year: Limit the report to information concerning the given year.
                If `None`, all years are included.

        Returns:
            Data quality report
        """
        stmt = select(models.FileCount).where(models.FileCount.station_id == self._station.get("id"))
        if year is not None:
            stmt = stmt.where(models.FileCount.year == year)

        with MetadataSession() as session:
            results = [
                {
                    "usaf_id": r.station.usaf_id,
                    "wban_id": r.wban_id,
                    "year": r.year,
                    "quality": r.quality,
                    "jan": r.jan,
                    "feb": r.feb,
                    "mar": r.mar,
                    "apr": r.apr,
                    "may": r.may,
                    "jun": r.jun,
                    "jul": r.jul,
                    "aug": r.aug,
                    "sep": r.sep,
                    "oct": r.oct,
                    "nov": r.nov,
                    "dec": r.dec,
                    "count": r.count,
                    "n_zero_months": r.n_zero_months,
                }
                for r in session.scalars(stmt).all()
            ]

        return pd.DataFrame(results).squeeze()

    def fetch_raw_data(self, year: int | list[int], *, use_http: bool = False) -> list[ISDRecord]:
        """Fetch data from ISD.

        Args:
            year: Year or years of data to fetch.
            use_http: Use NOAA's HTTP server instead of their FTP server. Set
                this to ``True`` if you are running into issues with FTP.

        Returns:
            A list of data records from the ISD database.
        """
        if not isinstance(year, list):
            year = [year]

        filenames = [f for y in year for f in self.get_filenames(y)]
        connector = NOAAHTTPConnection if use_http else NOAAFTPConnection

        with connector() as conn:
            return [
                parser.parse_line(line.decode("utf-8"))
                for filename in filenames
                for line in conn.read_file_as_bytes(filename)
            ]

    def fetch_data(
        self,
        year: int | list[int],
        datum: str | list[str] | None = None,
        *,
        period: str | None = None,
        rollup: str = "ending",
        upsample_first: bool = True,
        tz: str = "UTC",
        include_control: bool = False,
        include_quality_codes: bool = True,
        temp_scale: str | None = None,
        model_dump_include: IncEx | None = None,
        model_dump_exclude: IncEx | None = None,
        use_http: bool = False,
    ) -> pd.DataFrame:
        """Fetch data from ISD and return it as a [DataFrame][pandas.DataFrame].

        Args:
            year: Year or years of data to fetch.
            datum: Data elements to include in the results. Must be one or more of the
                [mandatory data fields][riweather.parser.MandatoryData]:

                * ``'wind'``
                * ``'ceiling'``
                * ``'visibility'``
                * ``'air_temperature'``
                * ``'dew_point'``
                * ``'sea_level_pressure'``

                If not specified, all data are returned.
            period: The time step at which the data will be returned. If ``None``, the default, the
                data is returned at the original times they were observed. If specified, it must be
                a frequency string recognized by [Pandas][] such as ``'h'`` for hourly and ``'15min'``
                for every 15 minutes (see the [docs on frequency strings](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects)).
                The data will be [resampled](https://pandas.pydata.org/docs/user_guide/timeseries.html#resampling)
                to the given frequency.
            rollup: How to align values to the ``period``. Defaults to ``'ending'``, meaning that values
                over the previous time period are averaged. Can also be ``'starting'``, ``'midpoint'``,
                or ``'instant'``. If ``period=None``, this value is ignored.
            upsample_first: Whether to upsample the data to the minute level prior to resampling.
                Upsampling is recommended because it gives more accurate representations of the
                weather observations, so it defaults to ``True``.
            tz: The timestamps of each observation are returned from the ISD in
                [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time). If this parameter
                is set, the data will be converted to the specified timezone. The timezone string
                should match one of the
                [standard TZ identifiers](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones),
                e.g. ``'US/Eastern'``, ``'US/Central'``, ``'US/Mountain'``, ``'US/Pacific'``, etc.
            include_control: If ``True``, include the [control data fields][riweather.parser.ControlData]
                in the results.
            include_quality_codes: If ``False``, filter out all the quality code fields from the
                results. These are columns that end in the string ``'quality_code'``.
            temp_scale: By default, when ``'air_temperature'`` or ``'dew_point'`` are specified as
                a datum, temperatures are returned in both degrees Celsius and degrees Fahrenheit.
                To only include one or the other, set ``temp_scale`` to ``'C'`` or ``'F'``. Ignored
                if no temperature values are meant to be retrieved.
            model_dump_include: Fine-grained control over the fields that are returned. Passed
                directly to [``pydantic.BaseModel.model_dump``][] as the `include` parameter; see the
                docs for details. Takes precendence over ``datum``.
            model_dump_exclude: Fine-grained control over the fields that are returned. Passed
                directly to [``pydantic.BaseModel.model_dump``][] as the `exclude` parameter; see the
                docs for details. Takes precendence over ``datum``.
            use_http: Use NOAA's HTTP server instead of their FTP server. Set this to ``True`` if
                you are running into issues with FTP.

        Returns:
            Weather observations from the station.
        """
        if datum is not None:
            if not isinstance(datum, list):
                datum = [datum]

            if not all(d in MandatoryData.model_fields for d in datum):
                msg = f"datum must be a subset of the following: {list(MandatoryData.model_fields)}"
                raise ValueError(msg)

        if rollup not in ("starting", "ending", "midpoint", "instant"):
            msg = "Invalid rollup"
            raise ValueError(msg)

        data = self.fetch_raw_data(year, use_http=use_http)
        timestamps = pd.DatetimeIndex([d.control.dt for d in data])

        if include_control:
            df_control = pd.json_normalize([d.control.model_dump(exclude={"dt"}) for d in data])
            df_control.index = timestamps
        else:
            df_control = pd.DataFrame()

        if model_dump_include is not None or model_dump_exclude is not None:
            data_pydantic_dumps = [
                d.mandatory.model_dump(include=model_dump_include, exclude=model_dump_exclude) for d in data
            ]
        else:
            data_pydantic_dumps = [d.mandatory.model_dump(include=datum) for d in data]

        df_mandatory = pd.json_normalize(data_pydantic_dumps)
        df_mandatory.index = timestamps

        df = pd.concat([df_control, df_mandatory], axis=1)
        if not include_quality_codes:
            df = df.loc[:, df.columns[~df.columns.str.contains("quality_code")]]
        if temp_scale is not None:
            if temp_scale.lower() == "c":
                df = df.loc[:, df.columns[~df.columns.str.contains("temperature_f")]]
            elif temp_scale.lower() == "f":
                df = df.loc[:, df.columns[~df.columns.str.contains("temperature_c")]]

        if period is not None:
            cols_to_keep = [f"{k}.{vv}" for k, v in _AGGREGABLE_FIELDS.items() for vv in v]
            df = df.loc[:, [c for c in df.columns if c in cols_to_keep]]

            if rollup == "starting":
                df = rollup_starting(df, period, upsample_first=upsample_first)
            elif rollup == "ending":
                df = rollup_ending(df, period, upsample_first=upsample_first)
            elif rollup == "midpoint":
                df = rollup_midpoint(df, period, upsample_first=upsample_first)
            elif rollup == "instant":
                df = rollup_instant(df, period, upsample_first=upsample_first)

        if tz != "UTC":
            df = df.tz_convert(tz)
        return df

    def fetch_raw_temp_data(
        self,
        year: int | list[int] | None = None,
        scale: str = "C",
        *,
        use_http: bool = False,
    ) -> pd.DataFrame:
        """Retrieve raw weather data from the ISD.

        !!! warning
            This has been deprecated and will be removed in a future release. Please consider using
            [`riweather.Station.fetch_data`][] instead.

        Args:
            year: Returned data will be limited to the year(s) specified. If
                `None`, data for all years is returned.
            scale: Return the temperature in Celsius (`"C"`, the default) or
                Fahrenheit (`"F"`).
            use_http: Use NOAA's HTTP server instead of their FTP server. Set
                this to ``True`` if you are running into issues with FTP.

        Returns:
            A [DataFrame][pandas.DataFrame], indexed on the timestamp, with two columns:
                air temperature and dew point temperature.

        Examples:
            >>> s = Station("720534")
            >>> print(s.fetch_raw_temp_data(2022).head(2))  # doctest: +SKIP
                                       wind_dir  wind_speed  tempC  dewC
            2022-01-01 00:15:00+00:00      80.0         4.6   -2.8  -4.0
            2022-01-01 00:35:00+00:00      60.0         4.1   -4.2  -5.5
        """
        msg = "fetch_raw_temp_data is deprecated. Please use fetch_raw_data() in the future."
        warnings.warn(DeprecationWarning(msg), stacklevel=2)

        data = []
        filenames = self.get_filenames(year)
        connector = NOAAHTTPConnection if use_http else NOAAFTPConnection

        if scale not in ("C", "F"):
            msg = 'Scale must be "C" (Celsius) or "F" (Fahrenheit).'
            raise ValueError(msg)

        with connector() as conn:
            for filename in filenames:
                datastream = conn.read_file_as_bytes(filename)
                for line in datastream.readlines():
                    date_str = line[15:27].decode("utf-8")
                    dt = pytz.UTC.localize(datetime.strptime(date_str, "%Y%m%d%H%M"))  # noqa: DTZ007
                    wind_dir = int(line[60:63]) if line[60:63].decode("utf-8") != "999" else float("nan")
                    wind_speed = float(line[65:69]) / 10.0 if line[65:69].decode("utf-8") != "9999" else float("nan")
                    tempC = _parse_temp(line[87:92])
                    dewC = _parse_temp(line[93:98])
                    data.append([dt, wind_dir, wind_speed, tempC, dewC])

        timestamps, wind_dirs, wind_speeds, temps, dews = zip(*sorted(data), strict=True)
        ts = pd.DataFrame(
            {"wind_dir": wind_dirs, "wind_speed": wind_speeds, "tempC": temps, "dewC": dews}, index=timestamps
        )

        if scale == "F":
            ts["tempF"] = ts["tempC"] * 1.8 + 32
            ts["dewF"] = ts["dewC"] * 1.8 + 32
            ts = ts.drop(["tempC", "dewC"], axis="columns")

        return ts.groupby(ts.index).mean()

    def fetch_temp_data(
        self,
        year: int | list[int] | None = None,
        value: str | None = None,
        scale: str = "C",
        period: str = "h",
        rollup: str = "ending",
        *,
        upsample_first: bool = True,
        use_http: bool = False,
    ) -> pd.DataFrame | pd.Series:
        """Retrieve temperature data from the ISD.

        !!! warning
            This has been deprecated and will be removed in a future release. Please consider using
            [`riweather.Station.fetch_data`][] instead.

        Args:
            year: Returned data will be limited to the year specified. If
                `None`, data for all years is returned.
            value: `"temperature"` to retrieve the air temperature only,
                or `"dew_point"` to retrieve the dew point temperature only.
                `None` returns both temperatures in a [DataFrame][pandas.DataFrame].
            scale: Return the value(s) in Celsius (`"C"`, the default) or
                Fahrenheit (`"F"`).
            period: The time step at which the data will be returned. Defaults
                to `"h"`, which corresponds to hourly data. Other possible
                values are `"30min"` for half-hourly data, `"15min"`
                for quarter-hourly data, and so on. See the [Pandas documentation
                on frequency strings](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects)
                for more details on possible values.
            rollup: How to align values to the `period`. Defaults to `"ending"`,
                meaning that values over the previous time period are averaged.
            upsample_first: Whether to upsample the data to the minute level prior to
                resampling. Usually results in more accurate representations of the
                true weather data.
            use_http: Use NOAA's HTTP server instead of their FTP server. Set
                this to ``True`` if you are running into issues with FTP.

        Returns:
            Either a [DataFrame][pandas.DataFrame] containing both air temperature
                and dew point temperature, or, if `value` was supplied, a
                [Series][pandas.Series] containing one or the other.

        Examples:
            >>> s = Station("720534")
            >>> print(s.fetch_temp_data(2022).head(2))  # doctest: +SKIP
                                        wind_dir  wind_speed     tempC      dewC
            2022-01-01 01:00:00+00:00  63.913043    4.197826 -4.328261 -5.539674
            2022-01-01 02:00:00+00:00  17.583333    3.656250 -6.585833 -7.717917
        """
        msg = "fetch_temp_data is deprecated. Please use fetch_data(year, datum='air_temperature') instead."
        warnings.warn(DeprecationWarning(msg), stacklevel=2)

        if value is None:
            value = "both"
        elif value not in ("temperature", "dew_point"):
            msg = 'Value must be "temperature" or "dew_point"'
            raise ValueError(msg)

        if rollup not in ("starting", "ending", "midpoint", "instant"):
            msg = "Invalid rollup"
            raise ValueError(msg)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw_ts = self.fetch_raw_temp_data(year, scale=scale, use_http=use_http)
        if rollup == "starting":
            ts = rollup_starting(raw_ts, period, upsample_first=upsample_first)
        elif rollup == "ending":
            ts = rollup_ending(raw_ts, period, upsample_first=upsample_first)
        elif rollup == "midpoint":
            ts = rollup_midpoint(raw_ts, period, upsample_first=upsample_first)
        else:  # rollup == "instant"
            ts = rollup_instant(raw_ts, period, upsample_first=upsample_first)

        if value == "temperature":
            return ts.loc[:, f"temp{scale}"]
        if value == "dew_point":
            return ts.loc[:, f"dew{scale}"]

        return ts

    def __repr__(self):
        return f'Station("{self.usaf_id}")'


def _calculate_distances(lat, lon):
    stmt = select(
        models.Station.usaf_id,
        models.Station.name,
        models.Station.latitude,
        models.Station.longitude,
    )

    with MetadataSession() as session:
        ids, names, lats, lons = zip(*session.execute(stmt), strict=True)

    target_lats = np.tile(lat, len(ids))
    target_lons = np.tile(lon, len(ids))
    geod = pyproj.Geod(ellps="WGS84")
    _, _, dists = geod.inv(
        target_lons,
        target_lats,
        lons,
        lats,
        radians=False,
    )

    data = [
        {
            "usaf_id": id_,
            "name": name,
            "distance": dist,
            "latitude": lat,
            "longitude": lon,
        }
        for id_, name, dist, lat, lon in zip(ids, names, dists, lats, lons, strict=True)
    ]

    return [data[i] for i in np.argsort(dists)]


def zcta_to_lat_lon(zcta: str) -> (float, float):
    """Convert zip code to lat/lon.

    Args:
        zcta: Five-digit zip code

    Returns:
        The center point of the ZCTA (Zip Code Tabulation Area).
    """
    with MetadataSession() as session:
        zcta = session.scalars(select(models.Zcta).where(models.Zcta.zip == zcta)).first()

    return zcta.latitude, zcta.longitude


def rank_stations(
    lat: float | None = None,
    lon: float | None = None,
    *,
    year: int | None = None,
    max_distance_m: int | None = None,
    zipcode: str | None = None,
) -> pd.DataFrame:
    """Rank stations by distance to a point.

    Args:
        lat: Site latitude
        lon: Site longitude
        year: If specified, only include stations with data for the given year(s).
        max_distance_m: If specified, only include stations within this distance
            (in meters) from the site.
        zipcode: Site zip code. If ``lat`` and/or ``lon`` are not given and ``zipcode`` is, then
            stations will be ranked according to the distance from the center point of the zip code.

    Returns:
        A [DataFrame][pandas.DataFrame] of station information.
    """
    if lat is None or lon is None:
        if zipcode is None:
            msg = "Either lat and lon must both be provided, or zipcode must be provided."
            raise ValueError(msg)
        lat, lon = zcta_to_lat_lon(zipcode)

    station_info = {info["usaf_id"]: info for info in _calculate_distances(lat, lon)}

    results = (
        select(
            models.Station.usaf_id,
            models.Station.name,
            models.FileCount.year,
            models.FileCount.quality,
        )
        .join_from(
            models.Station,
            models.FileCount,
        )
        .where(models.Station.usaf_id.in_(station_info.keys()))
    )

    data = {}
    with MetadataSession() as session:
        for row in session.execute(results):
            if row.usaf_id not in data:
                data[row.usaf_id] = {
                    **station_info[row.usaf_id],
                    "years": [],
                    "quality": [],
                }

            data[row.usaf_id]["years"].append(row.year)
            data[row.usaf_id]["quality"].append(row.quality)

    data = pd.DataFrame(sorted(data.values(), key=operator.itemgetter("distance"))).set_index("usaf_id")

    if year is not None:

        def _filter_years(x):
            if isinstance(year, list):
                return all(y in x for y in year)
            return year in x

        data = data.loc[data["years"].apply(_filter_years), :]

    if max_distance_m is not None:
        data = data.loc[data["distance"] <= max_distance_m, :]

    return data


def select_station(ranked_stations: pd.DataFrame, rank: int = 0) -> Station:
    """Return a Station object out of a ranked set of stations.

    Args:
        ranked_stations: A [DataFrame][pandas.DataFrame] returned by
            [`riweather.rank_stations`][].
        rank: Which station to return. Defaults to `rank=0`, which corresponds to
            the first (i.e. nearest) station.

    Returns:
        A [`Station`][riweather.Station] object.
    """
    if len(ranked_stations) <= rank:
        msg = "Rank too large, not enough stations"
        raise ValueError(msg)

    ranked_stations = ranked_stations.sort_values("distance")
    station = ranked_stations.iloc[rank]
    return Station(usaf_id=station.name)
