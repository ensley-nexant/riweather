"""Management actions on the metadata database."""

from __future__ import annotations

import datetime
import pathlib
import zipfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator
    from os import PathLike

import numpy as np
import pandas as pd
import shapefile
import shapely.geometry

from riweather import MetadataSession
from riweather.connection import NOAAFTPConnection, NOAAFTPConnectionError
from riweather.db import models

_MIN_YEAR = 2005


def open_zipped_shapefile(src: str | PathLike[str]) -> shapefile.Reader:
    """Open a compressed shapefile.

    Opens a zip archive, looks for a shapefile and its dependencies, and
    returns a [shapefile.Reader] object.

    Args:
        src: Path to the location of the zip archive

    Returns:
        The opened shapefile

    Raises:
        ValueError: No shapefiles, or multiple shapefiles, found in the archive
    """
    with zipfile.ZipFile(pathlib.Path(src).resolve(), "r") as archive:
        shapefiles = [pathlib.Path(name).stem for name in archive.namelist() if name.endswith(".shp")]
        if len(shapefiles) == 0:
            msg = "No shapefiles found in archive"
            raise ValueError(msg)

        if len(shapefiles) == 1:
            sf = shapefiles[0]
        else:
            msg = "Multiple shapefiles found in archive"
            raise ValueError(msg)

        shp = archive.open(f"{sf}.shp")
        dbf = archive.open(f"{sf}.dbf")
        try:
            shx = archive.open(f"{sf}.shx")
        except ValueError:
            shx = None

    return shapefile.Reader(shp=shp, dbf=dbf, shx=shx)


def iterate_zipped_shapefile(src: str | PathLike[str]) -> Generator:
    """Iterate over the shapes and records within a zipped shapefile.

    Opens a zip archive, reads the shapefile inside, and returns each shape
    and the record associated with that shape as a dictionary.

    Args:
        src: Path to the location of the zip archive

    Yields:
        shape: A [shapely][] geometry
        record: A dictionary of data records associated with `shape`

    Examples:
        >>> for shape, record in iterate_zipped_shapefile(  # doctest: +SKIP
        ...     "path/to/shapefile.zip"
        ... ):
        ...     print(shape)
        POLYGON ((...))
    """
    with open_zipped_shapefile(pathlib.Path(src).resolve()) as sf:
        for shape_rec in sf:
            shape = shapely.geometry.shape(shape_rec.shape)
            record = shape_rec.record.as_dict()
            yield shape, record


def _map_zcta_to_state(zcta_centroid, county_metadata) -> dict:
    """Helper function to map ZCTAs to states geographically.

    Find the first instance of a county containing the given ZCTA centroid.
    If no counties are found, fall back to the first instance of a county's
    convex hull containing the ZCTA centroid. This fallback catches some, but
    probably not all, situations where the centroid is not located in any county,
    e.g. a zip code containing islands whose centroid happens to be on a lake or
    in the ocean.

    Args:
        zcta_centroid: Centroid of the target ZCTA
        county_metadata: Dictionary of county metadata (dict of dicts). Each inner
            dict must include keys "name", "state_code", "polygon", and "convex_hull".

    Returns:
        The dictionary in `county_metadata` corresponding to the matching county.
            If no match was found, the dictionary will be empty.
    """
    polygon_containment = filter(lambda x: zcta_centroid.within(x["polygon"]), county_metadata.values())
    hit = next(polygon_containment, None)

    if hit is None:
        convex_hull_containment = filter(
            lambda x: zcta_centroid.within(x["convex_hull"]),
            county_metadata.values(),
        )
        hit = next(convex_hull_containment, {})

    return hit


def assemble_zcta_metadata(src: str | PathLike[str]) -> dict:
    """Prep ZCTA data for database insertion.

    Args:
        src: Folder containing the Census data files: "cb_2020_us_county_500k.zip"
            and "cb_2020_us_zcta520_500k.zip"

    Returns:
        A dictionary of dictionaries, keyed by 5-digit ZIP code
    """
    src = pathlib.Path(src).resolve()

    county_metadata = {}
    for shape, record in iterate_zipped_shapefile(src / "cb_2020_us_county_500k.zip"):
        county_id = record["GEOID"]
        county_metadata[county_id] = {
            "county": county_id,
            "name": record["NAMELSAD"],
            "state": record["STATE_NAME"],
            "state_code": record["STUSPS"],
            "polygon": shape,
            "convex_hull": shape.convex_hull,
            "centroid": shape.centroid,
            "latitude": shape.centroid.y,
            "longitude": shape.centroid.x,
        }

    zcta_metadata = {}
    for shape, record in iterate_zipped_shapefile(src / "cb_2020_us_zcta520_500k.zip"):
        county_record = _map_zcta_to_state(shape.centroid, county_metadata)
        zcta = record["GEOID20"]
        zcta_metadata[zcta] = {
            "zcta": zcta,
            "polygon": shape,
            "centroid": shape.centroid,
            "latitude": shape.centroid.y,
            "longitude": shape.centroid.x,
            "county_id": county_record.get("county"),
            "county_name": county_record.get("name"),
            "state": county_record.get("state_code"),
        }

    return zcta_metadata


def assemble_station_metadata(src: str | PathLike[str]) -> dict:
    """Prep weather station data for database insertion.

    Args:
        src: Folder containing the NOAA metadata file "isd-history.csv"

    Returns:
        A dictionary of dictionaries, keyed by 6-digit USAF ID
    """
    src = pathlib.Path(src).resolve()
    history = pd.read_csv(src / "isd-history.csv", dtype=str, parse_dates=["BEGIN", "END"])

    for col in ["LAT", "LON", "ELEV(M)"]:
        # strip out "+" sign preceding value and convert to float
        history[col] = history[col].str.removeprefix("+").astype(float).replace(0, np.nan)

    # h2 contains the most recent row for each ID
    h2 = history.loc[history.groupby("USAF")["END"].idxmax(), :]
    # collapse all old WBAN IDs into a single list entry per row
    h2 = h2.join(history.groupby("USAF")["WBAN"].apply(list).rename("wban_ids"), on="USAF")

    # criteria for keeping a station:
    #   - USAF ID not equal to 999999
    #   - country is US
    #   - latitude and longitude are not missing
    h3 = (
        h2.loc[
            (h2["USAF"] != "999999") & (h2["CTRY"] == "US") & h2["LAT"].notnull() & h2["LON"].notnull(),
            [
                "USAF",
                "wban_ids",
                "WBAN",
                "STATION NAME",
                "ICAO",
                "LAT",
                "LON",
                "ELEV(M)",
                "STATE",
            ],
        ]
        .rename(
            columns={
                "USAF": "usaf_id",
                "WBAN": "recent_wban_id",
                "STATION NAME": "name",
                "ICAO": "icao_code",
                "LAT": "latitude",
                "LON": "longitude",
                "ELEV(M)": "elevation",
                "STATE": "state",
            }
        )
        .set_index("usaf_id")
    )

    return h3.to_dict(orient="index")


def assemble_file_metadata_from_inventory(src: str | PathLike[str], stations: list[str]) -> dict:
    """Prep NOAA data file information for database insertion.

    Args:
        src: Folder containing the NOAA metadata file "isd-inventory.csv"
        stations: List of station IDs
    """
    _today = pd.to_datetime(datetime.datetime.now(tz=datetime.timezone.utc).date())
    src = pathlib.Path(src).resolve()
    inv: pd.DataFrame = pd.read_csv(src / "isd-inventory.csv", dtype={"USAF": str, "WBAN": str})
    # convert column names to lowercase
    inv.columns = inv.columns.str.lower()
    # only keep stations that exist in the station metadata, and years >= 2005
    inv = inv.loc[inv["usaf"].isin(stations) & (inv["year"] >= _MIN_YEAR), :].copy()
    # pivot wide to long
    inv_long = inv.melt(
        id_vars=["usaf", "wban", "year"],
        var_name="month",
        value_name="count",
    ).replace(
        {
            "month": {
                "jan": 1,
                "feb": 2,
                "mar": 3,
                "apr": 4,
                "may": 5,
                "jun": 6,
                "jul": 7,
                "aug": 8,
                "sep": 9,
                "oct": 10,
                "nov": 11,
                "dec": 12,
            }
        }
    )
    # create dt column for year-month
    inv_long["day"] = 1
    inv_long["month_start_dt"] = pd.to_datetime(inv_long[["year", "month", "day"]])
    del inv_long["day"]
    # only keep year-months on or before the current month
    inv_long = inv_long.loc[inv_long["month_start_dt"] <= _today, :].copy()
    # hours in month up to today, if today falls within that month;
    # otherwise hours in total month
    inv_long["hours_in_month"] = np.where(
        (inv_long["year"] == _today.year) & (inv_long["month"] == _today.month),
        (_today - inv_long["month_start_dt"]) / pd.Timedelta(hours=1),
        inv_long["month_start_dt"].dt.days_in_month * 24,
    )
    q = inv_long.groupby(["usaf", "wban", "year"])[["count", "hours_in_month"]].agg(
        count=("count", "sum"),
        hours_in_year=("hours_in_month", "sum"),
        n_zero_months=("count", lambda x: x.eq(0).sum()),
    )
    # define quality criteria here
    num_min_zero_months = 2
    q["quality"] = pd.Categorical(
        np.where(
            (q["count"] >= 0.9 * q["hours_in_year"]) & (q["n_zero_months"] == 0),
            "high",
            np.where(
                (q["count"] >= 0.5 * q["hours_in_year"]) & (q["n_zero_months"] <= num_min_zero_months),
                "medium",
                "low",
            ),
        ),
        categories=["low", "medium", "high"],
        ordered=True,
    )
    inv = inv.join(
        q[["count", "n_zero_months", "quality"]],
        on=["usaf", "wban", "year"],
        how="left",
    )
    return inv.to_dict(orient="index")


def assemble_file_metadata_from_crawl() -> list[dict]:
    """Prep NOAA data file information for database insertion.

    Traverses the FTP server and records all data files encountered, so that we can
    tell if a certain station has data for a certain year just by referencing the
    metadata database rather than hitting the server over and over again. Does not
    download any actual weather data.

    Files on the FTP server are organized like:

    /pub/data/noaa/
    |- 1901/
    ...
    |- 2022/
    |- |- [USAF-ID]-[WBAN-ID]-[YEAR].gz

    This function only walks the file system for years dating back to 2006. This is
    for two reasons. First, we generally aren't concerned with historical weather data
    more than 5-10 years old in our applications, so going way back would be a waste
    of time. Second, data coverage and quality drop off steeply the farther back you
    go - obviously there weren't as many airports in 1901 as there were in 2022, so
    we'd like to maximize our chances of pulling in decent data only.

    Returns:
        A list of dictionaries, each corresponding to a file that is present on the
        NOAA FTP server.
    """
    file_metadata = []
    with NOAAFTPConnection() as conn:
        if conn.ftp is None:
            msg = "FTP connection could not be established."
            raise NOAAFTPConnectionError(msg) from None

        for year in range(_MIN_YEAR + 1, datetime.datetime.now(tz=datetime.timezone.utc).year + 1):
            files = conn.ftp.mlsd(f"pub/data/noaa/{year}", ["size", "type"])
            for name, facts in files:
                if facts.get("type", "") == "file":
                    name_parts = name.split(".")[0].split("-")
                    size = int(facts.get("size", 0))
                    if len(name_parts) == 3 and size > 0:  # noqa: PLR2004
                        file_metadata.append(
                            {
                                "usaf_id": name_parts[0],
                                "wban_id": name_parts[1],
                                "year": int(name_parts[2]),
                                "size": size,
                            }
                        )

    return file_metadata


def populate(src: str | PathLike[str]) -> None:
    """Populate the metadata database.

    Args:
        src: Folder containing the Census files `"cb_2020_us_county_500k.zip"` and
            `"cb_2020_us_zcta520_500k.zip"`, and the NOAA files `"isd-history.csv"` and
            `"isd-inventory.csv"`.
    """
    zcta_metadata = assemble_zcta_metadata(src)
    station_metadata = assemble_station_metadata(src)
    file_metadata = assemble_file_metadata_from_inventory(src, list(station_metadata.keys()))

    zcta_db_objs = [
        models.Zcta(
            zip=z["zcta"],
            latitude=z["latitude"],
            longitude=z["longitude"],
            county_id=z["county_id"],
            state=z["state"],
        )
        for z in zcta_metadata.values()
    ]

    station_db_objs = {
        usaf_id: models.Station(
            usaf_id=usaf_id,
            wban_ids=",".join(s["wban_ids"]),
            recent_wban_id=s["recent_wban_id"],
            name=s["name"],
            icao_code=s["icao_code"],
            latitude=s["latitude"],
            longitude=s["longitude"],
            elevation=s["elevation"],
            state=s["state"],
        )
        for usaf_id, s in station_metadata.items()
    }

    file_db_objs = [
        models.FileCount(
            station=station_db_objs[f["usaf"]],
            wban_id=f["wban"],
            year=f["year"],
            jan=f["jan"],
            feb=f["feb"],
            mar=f["mar"],
            apr=f["apr"],
            may=f["may"],
            jun=f["jun"],
            jul=f["jul"],
            aug=f["aug"],
            sep=f["sep"],
            oct=f["oct"],
            nov=f["nov"],
            dec=f["dec"],
            count=f["count"],
            n_zero_months=f["n_zero_months"],
            quality=f["quality"],
        )
        for f in file_metadata.values()
    ]

    with MetadataSession() as session:
        session.add_all(zcta_db_objs)
        session.add_all(station_db_objs.values())
        session.add_all(file_db_objs)
        session.commit()
