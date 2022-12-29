"""Weather station operations."""
import operator
from datetime import datetime

import numpy as np
import pandas as pd
import pyproj
import pytz
from sqlalchemy import select

from riweather import MetadataSession
from riweather.connection import NOAAFTPConnection
from riweather.db import models

__all__ = (
    "zcta_to_lat_lon",
    "rank_stations",
    "select_station",
    "Station",
)


def _parse_temp(s: bytes) -> float:
    value = float(s) / 10.0 if s.decode("utf-8") != "+9999" else float("nan")
    return value


class Station:
    """ISD Station object."""

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
            self._station = None

    def _load_metadata(self) -> models.Station:
        """Retrieve station metadata from the local data store.

        Returns:
            A Station SQLAlchemy object.
        """
        stmt = select(models.Station).where(models.Station.usaf_id == self.usaf_id)
        with MetadataSession() as session:
            return session.scalars(stmt).first()

    @property
    def wban_ids(self) -> list[str]:
        """List of valid WBAN (Weather Bureau Army Navy) identifiers."""
        return self._station.wban_ids.split(",")

    @property
    def recent_wban_id(self) -> str:
        """Most recent WBAN (Weather Bureau Army Navy) identifier."""
        return self._station.recent_wban_id

    @property
    def name(self) -> str:
        """Station name."""
        return self._station.name

    @property
    def icao_code(self) -> str:
        """ICAO airport code."""
        return self._station.icao_code

    @property
    def latitude(self) -> float:
        """Station latitude."""
        return self._station.latitude

    @property
    def longitude(self) -> float:
        """Station longitude."""
        return self._station.longitude

    @property
    def elevation(self) -> float:
        """Elevation of the station, in meters."""
        return self._station.elevation

    @property
    def state(self) -> str:
        """US state in which the station is located."""
        return self._station.state

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
        stmt = select(models.File).where(models.File.station_id == self._station.id)
        if year is not None:
            stmt = stmt.where(models.File.year == year)

        filename_template = "/pub/data/noaa/{2}/{0}-{1}-{2}.gz"
        filenames = []
        with MetadataSession() as session:
            for row in session.scalars(stmt):
                filenames.append(
                    filename_template.format(
                        self._station.usaf_id, row.wban_id, row.year
                    )
                )

        return filenames

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
        self, year: int = None, value: int = None, scale: str = "C", period: str = "H"
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

        raw_ts = self.fetch_raw_temp_data(year, scale=scale)
        ts = (
            raw_ts.resample("min")
            .mean()
            .interpolate(method="linear", limit=60, limit_direction="both")
            .resample(period)
            .mean()
        )

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


def rank_stations(lat, lon, *, year=None, max_distance_m=None):
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
            models.File.year,
            models.File.size,
        )
        .join_from(
            models.Station,
            models.File,
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
                    "sizes": [],
                }

            data[row.usaf_id]["years"].append(row.year)
            data[row.usaf_id]["sizes"].append(row.size)

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


def select_station(ranked_stations, rank=0):
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
