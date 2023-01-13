"""Weather station operations."""
import operator
from datetime import datetime

import numpy as np
import pandas as pd
import pyproj
import pytz
from pandas.tseries.frequencies import to_offset
from sqlalchemy import select

from riweather import MetadataSession
from riweather.connection import NOAAFTPConnection
from riweather.db import models

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


def _parse_temp(s: bytes) -> float:
    value = float(s) / 10.0 if s.decode("utf-8") != "+9999" else float("nan")
    return value


def upsample(data: pd.Series | pd.DataFrame, period: str = "T"):
    """Upsample and interpolate time series data.

    Args:
        data: Time series data with a datetime index
        period: Period to upsample to. Defaults to `"T"`, which is minute-level

    Returns:
        Upsampled data

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range("2023-01-01 00:01", "2023-01-01 01:05", freq="32T"),
        ... )
        >>> upsample(t, period="T").head()
        2023-01-01 00:01:00     1.00000
        2023-01-01 00:02:00     1.03125
        2023-01-01 00:03:00     1.06250
        2023-01-01 00:04:00     1.09375
        2023-01-01 00:05:00     1.12500
        Freq: T, Length: 65, dtype: float64
    """
    ts = (
        data.resample(period)
        .mean()
        .interpolate(method="linear", limit=60, limit_direction="both")
    )
    return ts


def rollup_starting(
    data: pd.Series | pd.DataFrame, period: str = "H", upsample_first: bool = True
):
    """Roll up data, labelled with the period start.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `"H"`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to calculating
            the period average.

    Returns:
        The time series rolled up to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range("2023-01-01 00:01", "2023-01-01 01:05", freq="32T"),
        ... )
        >>> rollup_starting(t)
        2023-01-01 00:00:00    3.207627
        2023-01-01 01:00:00    9.375000
        Freq: H, dtype: float64
    """
    if upsample_first:
        data = upsample(data, period="T")
    ts = data.resample(period, label="left", closed="left").mean()
    return ts


def rollup_ending(
    data: pd.Series | pd.DataFrame, period: str = "H", upsample_first: bool = True
):
    """Roll up data, labelled with the period end.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `"H"`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to calculating
            the period average.

    Returns:
        The time series rolled up to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range("2023-01-01 00:01", "2023-01-01 01:05", freq="32T"),
        ... )
        >>> rollup_ending(t)
        2023-01-01 01:00:00    3.3
        2023-01-01 02:00:00    9.5
        Freq: H, dtype: float64
    """
    if upsample_first:
        data = upsample(data, period="T")
    ts = data.resample(period, label="right", closed="right").mean()
    return ts


def rollup_midpoint(
    data: pd.Series | pd.DataFrame, period: str = "H", upsample_first: bool = True
):
    """Roll up data, labelled with the period midpoint.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `"H"`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to calculating
            the period average.

    Returns:
        The time series rolled up to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range("2023-01-01 00:01", "2023-01-01 01:05", freq="32T"),
        ... )
        >>> rollup_midpoint(t)
        2023-01-01 00:00:00    1.437500
        2023-01-01 01:00:00    5.661458
        Freq: H, dtype: float64
    """
    if upsample_first:
        data = upsample(data, period="T")
    half_period = to_offset(to_offset(period).delta / 2)
    ts = (
        data.shift(freq=half_period)
        .resample(period, label="left", closed="left")
        .mean()
    )
    return ts


def rollup_instant(
    data: pd.Series | pd.DataFrame, period: str = "H", upsample_first: bool = True
):
    """Roll up data, labelled with interpolated values.

    Args:
        data: Time series data with a datetime index
        period: Period to resample to. Defaults to `"H"`, which is hourly.
        upsample_first: Perform minute-level upsampling prior to returning
            a value.

    Returns:
        The time series aligned to the specified period.

    Examples:
        >>> import pandas as pd
        >>> t = pd.Series(
        ...     [1, 2, 10],
        ...     index=pd.date_range("2023-01-01 00:01", "2023-01-01 01:05", freq="32T"),
        ... )
        >>> rollup_instant(t)
        2023-01-01 00:00:00    1.00
        2023-01-01 01:00:00    8.75
        Freq: H, dtype: float64
    """
    if upsample_first:
        data = upsample(data, period="T")
    ts = data.resample(period, label="left", closed="left").first()
    return ts


class Station:  # noqa: D101
    def __init__(self, usaf_id: str, load_metadata_on_init: bool = True):
        """ISD Station object.

        Args:
            usaf_id: USAF identifier
            load_metadata_on_init: If `True`, station metadata will be retrieved
                from the local data store and loaded into the object as
                properties.

        Examples:
            >>> s = Station("720534")
            >>> print(s.name, s.latitude, s.longitude)
            ERIE MUNICIPAL AIRPORT 40.017 -105.05
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
            station_info = {
                col: getattr(station, col) for col in station.__table__.columns.keys()
            }
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
        """Station name."""
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
        """US state in which the station is located."""
        return self._station.get("state")

    @property
    def years(self) -> list[int]:
        """Years for which data exists for the station."""
        return self._station.get("years", [])

    def get_filenames(self, year: int = None) -> list[str]:
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
        stmt = select(models.FileCount).where(
            models.FileCount.station_id == self._station.get("id")
        )
        if year is not None:
            stmt = stmt.where(models.FileCount.year == year)

        filename_template = "/pub/data/noaa/{2}/{0}-{1}-{2}.gz"
        filenames = []
        with MetadataSession() as session:
            for row in session.scalars(stmt):
                filenames.append(
                    filename_template.format(self.usaf_id, row.wban_id, row.year)
                )

        return filenames

    def quality_report(self, year: int = None) -> pd.DataFrame | pd.Series:
        """Retrieve information on data quality.

        Args:
            year: Limit the report to information concerning the given year.
                If `None`, all years are included.

        Returns:
            Data quality report
        """
        stmt = select(models.FileCount).where(
            models.FileCount.station_id == self._station.get("id")
        )
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

    def fetch_raw_temp_data(self, year: int = None, scale: str = "C") -> pd.DataFrame:
        """Retrieve raw weather data from the ISD.

        Args:
            year: Returned data will be limited to the year specified. If
                `None`, data for all years is returned.
            scale: Return the temperature in Celsius (`"C"`, the default) or
                Fahrenheit (`"F"`).

        Returns:
            A [DataFrame][pandas.DataFrame], indexed on the timestamp, with two columns:
                air temperature and dew point temperature.

        Examples:
            >>> s = Station("720534")
            >>> print(s.fetch_raw_temp_data(2022).head(2))
                                       tempC  dewC
            2022-01-01 00:15:00+00:00   -2.8  -4.0
            2022-01-01 00:35:00+00:00   -4.2  -5.5
        """
        data = []
        filenames = self.get_filenames(year)

        if scale not in ("C", "F"):
            raise ValueError('Scale must be "C" (Celsius) or "F" (Fahrenheit).')

        with NOAAFTPConnection() as conn:
            for filename in filenames:
                datastream = conn.read_file_as_bytes(filename)
                for line in datastream.readlines():
                    tempC = _parse_temp(line[87:92])
                    dewC = _parse_temp(line[93:98])
                    date_str = line[15:27].decode("utf-8")
                    dt = pytz.UTC.localize(datetime.strptime(date_str, "%Y%m%d%H%M"))
                    data.append([dt, tempC, dewC])

        timestamps, temps, dews = zip(*sorted(data), strict=True)
        ts = pd.DataFrame({"tempC": temps, "dewC": dews}, index=timestamps)

        if scale == "F":
            ts["tempF"] = ts["tempC"] * 1.8 + 32
            ts["dewF"] = ts["dewC"] * 1.8 + 32
            ts = ts.drop(["tempC", "dewC"], axis="columns")

        ts = ts.groupby(ts.index).mean()
        return ts

    def fetch_temp_data(
        self,
        year: int = None,
        value: int = None,
        scale: str = "C",
        period: str = "H",
        rollup: str = "ending",
        upsample_first: bool = True,
    ) -> pd.DataFrame | pd.Series:
        """Retrieve temperature data from the ISD.

        Args:
            year: Returned data will be limited to the year specified. If
                `None`, data for all years is returned.
            value: `"temperature"` to retrieve the air temperature only,
                or `"dew_point"` to retrieve the dew point temperature only.
                `None` returns both temperatures in a [DataFrame][pandas.DataFrame].
            scale: Return the value(s) in Celsius (`"C"`, the default) or
                Fahrenheit (`"F"`).
            period: The time step at which the data will be returned. Defaults
                to `"H"`, which corresponds to hourly data. Other possible
                values are `"30T"` or `"30min"` for half-hourly data, `"15T"`/`"15min"`
                for quarter-hourly data, and so on. See the [Pandas documentation
                on frequency strings](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects)
                for more details on possible values.
            rollup: How to align values to the `period`. Defaults to `"ending"`,
                meaning that values over the previous time period are averaged.
            upsample_first: Whether to upsample the data to the minute level prior to
                resampling. Usually results in more accurate representations of the
                true weather data.

        Returns:
            Either a [DataFrame][pandas.DataFrame] containing both air temperature
                and dew point temperature, or, if `value` was supplied, a
                [Series][pandas.Series] containing one or the other.

        Examples:
            >>> s = Station("720534")
            >>> print(s.fetch_temp_data(2022).head(2))
                                          tempC      dewC
            2022-01-01 00:00:00+00:00 -4.298889 -5.512222
            2022-01-01 01:00:00+00:00 -6.555833 -7.688333
        """  # noqa
        if value is None:
            value = "both"
        elif value not in ("temperature", "dew_point"):
            raise ValueError('Value must be "temperature" or "dew_point"')

        if rollup not in ("starting", "ending", "midpoint", "instant"):
            raise ValueError("Invalid rollup")

        raw_ts = self.fetch_raw_temp_data(year, scale=scale)
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
        else:
            return ts

    def __repr__(self):
        """String representation of a Station."""
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
            "usaf_id": id,
            "name": name,
            "distance": dist,
            "latitude": lat,
            "longitude": lon,
        }
        for id, name, dist, lat, lon in zip(ids, names, dists, lats, lons, strict=True)
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
        zcta = session.scalars(
            select(models.Zcta).where(models.Zcta.zip == zcta)
        ).first()

    return zcta.latitude, zcta.longitude


def rank_stations(
    lat: float, lon: float, *, year: int = None, max_distance_m: int = None
) -> pd.DataFrame:
    """Rank stations by distance to a point.

    Args:
        lat: Site latitude
        lon: Site longitude
        year: If specified, only include stations with data for the given year(s).
        max_distance_m: If specified, only include stations within this distance
            (in meters) from the site.

    Returns:
        A [DataFrame][pandas.DataFrame] of station information.
    """
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
            if row.usaf_id not in data.keys():
                data[row.usaf_id] = {
                    **station_info[row.usaf_id],
                    "years": [],
                    "quality": [],
                }

            data[row.usaf_id]["years"].append(row.year)
            data[row.usaf_id]["quality"].append(row.quality)

    data = pd.DataFrame(
        sorted(data.values(), key=operator.itemgetter("distance"))
    ).set_index("usaf_id")

    if year is not None:

        def _filter_years(x):
            if isinstance(year, list):
                return all(y in x for y in year)
            else:
                return year in x

        data = data.loc[data["years"].apply(_filter_years), :]

    if max_distance_m is not None:
        data = data.loc[data["distance"] <= max_distance_m, :]

    return data


def select_station(ranked_stations: pd.DataFrame, rank: int = 0) -> Station:
    """Return a Station object out of a ranked set of stations.

    Args:
        ranked_stations: A [DataFrame][pandas.DataFrame] returned by
            [`riweather.rank_stations`].
        rank: Which station to return. Defaults to `rank=0`, which corresponds to
            the first (i.e. nearest) station.

    Returns:
        A [`Station`][riweather.Station] object.
    """
    if len(ranked_stations) <= rank:
        raise ValueError("Rank too large, not enough stations")

    ranked_stations = ranked_stations.sort_values("distance")
    station = ranked_stations.iloc[rank]
    return Station(usaf_id=station.name)
