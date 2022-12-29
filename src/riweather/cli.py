"""Command line interface for riweather."""
import functools
import pathlib
from importlib.resources import files

import click
import requests
from sqlalchemy import create_engine

from . import __version__, connection, utils


def _cli_download_metadata(filename: str, dst: pathlib.Path) -> pathlib.Path:
    """Download metadata from NCEI/NOAA."""
    file_root = filename.split("/")[-1]
    if utils.is_compressed(file_root):
        file_root = ".".join(file_root.split(".")[:-1])
    outloc = dst / file_root

    with connection.NOAAFTPConnection() as conn:
        contents = conn.read_file_as_bytes(filename)

    with open(outloc, "wb") as f:
        f.write(contents.read())

    return outloc


def _cli_download_census(
    url: str, dst: pathlib.Path, chunk_size: int = 1024
) -> pathlib.Path:
    """Download metadata from the US Census."""
    file_root = url.split("/")[-1]
    outloc = dst / file_root

    r = requests.get(url, stream=True)
    n_chunks = int(int(r.headers.get("Content-Length", 0)) / chunk_size) or None

    with open(outloc, "wb") as fd:
        with click.progressbar(
            r.iter_content(chunk_size=chunk_size), length=n_chunks
        ) as bar:
            for chunk in bar:
                fd.write(chunk)

    return outloc


@click.group(context_settings={"auto_envvar_prefix": "RIWEATHER"})
@click.version_option(version=__version__)
def main() -> None:
    """Riweather makes grabbing weather data easier."""
    pass


@main.command()
@click.option(
    "-d",
    "--dst",
    required=True,
    type=click.Path(file_okay=False, writable=True, path_type=pathlib.Path),
    help="Directory where the data will be stored.",
)
def download_metadata(dst: pathlib.Path) -> None:
    """Download weather station and US geography metadata.

    Pulls files from the Internet that are necessary to (re)build the riweather
    metadata database.
    """
    secho_inprog = functools.partial(click.secho, nl=False, fg="yellow")
    secho_complete = functools.partial(click.secho, fg="blue")

    dst.mkdir(parents=True, exist_ok=True)

    secho_inprog("Downloading NCEI station metadata...")
    outloc = _cli_download_metadata("/pub/data/noaa/isd-history.csv", dst)
    secho_complete(" ---> {}".format(outloc))

    secho_inprog("Downloading NCEI station data quality report...")
    outloc = _cli_download_metadata("/pub/data/noaa/isd-inventory.csv.z", dst)
    secho_complete(" ---> {}".format(outloc))

    secho_inprog("Downloading US Census ZCTAs...", nl=True)
    outloc = _cli_download_census(
        "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_500k.zip",
        dst,
    )
    secho_complete(" ---> {}".format(outloc))

    secho_inprog("Downloading US Census counties...", nl=True)
    outloc = _cli_download_census(
        "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip", dst
    )
    secho_complete(" ---> {}".format(outloc))


@main.command()
@click.option(
    "-s",
    "--src",
    required=True,
    type=click.Path(file_okay=False, writable=True, path_type=pathlib.Path),
    help="Directory where the data is stored.",
)
def rebuild_db(src: pathlib.Path) -> None:
    r"""Drop and recreate all tables in the metadata database.

    The database is created in a special folder in the user's home directory:
    `~/.riweather/metadata.db` on Mac/Linux, `%USERPROFILE%\\.riweather\\metadata.db`
    on Windows.
    """
    from riweather.db import Base
    from riweather.db.actions import populate

    # dbpath = pathlib.Path.home() / ".riweather" / "metadata.db"
    dbpath = files("riweather.resources").joinpath("metadata.db")

    # drop and recreate tables before populating
    metadata_engine = create_engine(f"sqlite+pysqlite:///{dbpath}")
    Base.metadata.drop_all(metadata_engine)
    Base.metadata.create_all(metadata_engine)
    populate(src)
