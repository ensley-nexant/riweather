"""Management actions on the metadata database."""
import datetime
import pathlib
import zipfile
from os import PathLike
from typing import Generator

import numpy as np
import pandas as pd
import shapefile
import shapely.geometry

from riweather import MetadataSession
from riweather.connection import NOAAFTPConnection, NOAAFTPConnectionException
from riweather.db import models


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
        shapefiles = [
            pathlib.Path(name).stem
            for name in archive.namelist()
            if name.endswith(".shp")
        ]
        if len(shapefiles) == 0:
            raise ValueError("No shapefiles found in archive")
        elif len(shapefiles) == 1:
            sf = shapefiles[0]
        else:
            raise ValueError("Multiple shapefiles found in archive")

        shp = archive.open(".".join([sf, "shp"]))
        dbf = archive.open(".".join([sf, "dbf"]))
        try:
            shx = archive.open(".".join([sf, "shx"]))
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
        >>> for shape, record in iterate_zipped_shapefile("path/to/shapefile.zip"):  # doctest: +SKIP  # noqa
        ...     print(shape)
        ...     print(record)
    POLYGON ((...))

    {...}
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
    polygon_containment = filter(
        lambda x: zcta_centroid.within(x["polygon"]), county_metadata.values()
    )
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
    history = pd.read_csv(
        src / "isd-history.csv", dtype=str, parse_dates=["BEGIN", "END"]
    )

    for col in ["LAT", "LON", "ELEV(M)"]:
        # strip out "+" sign preceding value and convert to float
        history[col] = (
            history[col].str.removeprefix("+").astype(float).replace(0, np.NaN)
        )

    # h2 contains the most recent row for each ID
    h2 = history.loc[history.groupby("USAF")["END"].idxmax(), :]
    # collapse all old WBAN IDs into a single list entry per row
    h2 = h2.join(
        history.groupby("USAF")["WBAN"].apply(list).rename("wban_ids"), on="USAF"
    )

    # criteria for keeping a station:
    #   - USAF ID not equal to 999999
    #   - country is US
    #   - latitude and longitude are not missing
    h3 = (
        h2.loc[
            (h2["USAF"] != "999999")
            & (h2["CTRY"] == "US")
            & h2["LAT"].notnull()
            & h2["LON"].notnull(),
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


def assemble_file_metadata() -> list[dict]:
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
            raise NOAAFTPConnectionException(
                "FTP connection could not be established."
            ) from None
        for year in range(2006, datetime.date.today().year + 1):
            print(year)
            files = conn.ftp.mlsd(f"pub/data/noaa/{year}", ["size", "type"])
            for name, facts in files:
                if facts.get("type", "") == "file":
                    name_parts = name.split(".")[0].split("-")
                    size = int(facts.get("size", 0))
                    if len(name_parts) == 3 and size > 0:
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
    file_metadata = assemble_file_metadata()

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

    station_db_objs = [
        models.Station(
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
    ]

    file_db_objs = []
    for f in file_metadata:
        station = next((s for s in station_db_objs if s.usaf_id == f["usaf_id"]), None)
        if station is not None:
            file_db_objs.append(
                models.File(
                    wban_id=f["wban_id"],
                    station=station,
                    year=f["year"],
                    size=f["size"],
                )
            )

    with MetadataSession() as session:
        session.add_all(zcta_db_objs)
        session.add_all(station_db_objs)
        session.add_all(file_db_objs)
        session.commit()
