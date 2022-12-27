import datetime
import pathlib
import zipfile
from os import PathLike
from typing import IO, Generator

import numpy as np
import pandas as pd
import shapefile
import shapely.geometry

from riweather import MetadataSession
from riweather.connection import NOAAFTPConnection
from riweather.db import models


def open_zipped_shapefile(src: str | PathLike[str] | IO[bytes]) -> shapefile.Reader:
    print("HERE")
    print(pathlib.Path(src).resolve())
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


def iterate_zipped_shapefile(src: str | PathLike[str] | IO[bytes]) -> Generator:
    with open_zipped_shapefile(pathlib.Path(src).resolve()) as sf:
        for shape_rec in sf:
            shape = shapely.geometry.shape(shape_rec.shape)
            record = shape_rec.record.as_dict()
            yield shape, record


def map_zcta_to_state(zcta_centroid, county_metadata) -> dict:
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


def assemble_zcta_metadata(src: str | PathLike[str] | IO[bytes]) -> dict:
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
        county_record = map_zcta_to_state(shape.centroid, county_metadata)
        zcta = record["GEOID20"]
        zcta_metadata[zcta] = {
            "zcta": zcta,
            "polygon": shape,
            "centroid": shape.centroid,
            "latitude": shape.centroid.y,
            "longitude": shape.centroid.x,
            "county_id": county_record.get("county"),
            "county_name": county_record.get("county_name"),
            "state": county_record.get("state_code"),
        }

    return zcta_metadata


def assemble_station_metadata(src: str | PathLike[str] | IO[bytes]) -> dict:
    src = pathlib.Path(src).resolve()
    history = pd.read_csv(
        src / "isd-history.csv", dtype=str, parse_dates=["BEGIN", "END"]
    )

    for col in ["LAT", "LON", "ELEV(M)"]:
        history[col] = history[col].str.lstrip("+").astype(float).replace(0, np.NaN)

    h2 = history.loc[history.groupby("USAF")["END"].idxmax(), :]
    h2 = h2.join(
        history.groupby("USAF")["WBAN"].apply(list).rename("wban_ids"), on="USAF"
    )

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
    file_metadata = []
    with NOAAFTPConnection() as conn:
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


def populate(src: str | PathLike[str] | IO[bytes]):
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
