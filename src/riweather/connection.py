"""External connection objects."""

from __future__ import annotations

import ftplib
import gzip
import typing
from io import BytesIO

if typing.TYPE_CHECKING:
    from os import PathLike

import requests
from typing_extensions import Self

from riweather import utils


class NOAAFTPConnectionError(Exception):
    """Exception for bad FTP connections."""


class NOAAHTTPConnectionError(Exception):
    """Exception for bad HTTP connections."""


class NOAAFTPConnection:
    """Connector to NOAA's FTP data server.

    Use this as a context manager. The connection will not occur until
    the `__enter__()` method is called, i.e. when the `with` block is entered.

    Attributes:
        ftp: The raw connected [ftplib.FTP][] instance, or `None` if the connection
            hasn't been or cannot be made.

    Examples:
        >>> from riweather import NOAAFTPConnection
        >>> with NOAAFTPConnection() as conn:
        ...     welcome = conn.ftp.getwelcome()  # see docs for ftplib.FTP
        ...     contents = conn.read_file_as_bytes("/pub/data/noaa/isd-history.txt")
        ... # doctest: +SKIP
        >>> "You are accessing a U.S. Government information system" in welcome  # doctest: +SKIP
        True
        >>> contents.read(10)  # doctest: +SKIP
        b'Integrated'
    """

    _host = "ftp.ncei.noaa.gov"

    def __init__(self) -> None:
        """Initialize the FTP connection object."""
        self.ftp: ftplib.FTP | None = None

    def __enter__(self) -> Self:
        """Connect to the FTP server.

        Returns:
            self
        """
        try:
            ftp = ftplib.FTP(host=self._host, timeout=30)
            ftp.login()
            self.ftp = ftp
        except OSError as e:
            msg = f"Could not connect to the host: {self._host}."
            raise NOAAFTPConnectionError(msg) from e
        return self

    def __exit__(self, *args) -> None:
        """Close the FTP connection gracefully.

        Raises:
            Any error encountered by `ftplib` is raised here.
        """
        try:
            self.ftp.quit()
        except ftplib.all_errors as e:
            self.ftp.close()
            msg = "Error closing the connection."
            raise NOAAFTPConnectionError(msg) from e

        self.ftp = None

    def read_file_as_bytes(self, filename: str | PathLike) -> typing.IO | gzip.GzipFile:
        """Read a file off of the server and into a byte stream.

        Args:
            filename: The name/path of the file on the FTP server.

        Returns:
            The file contents. If `filename` ends with ".z" or ".gz", which is
                the case with many of NOAA's data files, then the results are
                decompressed automatically using [gzip][].
        """
        if self.ftp is None:
            msg = "FTP connection could not be established."
            raise NOAAFTPConnectionError(msg) from None
        try:
            stream = BytesIO()
            self.ftp.retrbinary(f"RETR {filename}", stream.write)
            stream.seek(0)
        except ftplib.all_errors as e:
            raise NOAAFTPConnectionError(e) from e

        if utils.is_compressed(filename):
            return gzip.open(stream, "rb")

        return stream


class NOAAHTTPConnection:
    """Connector to NOAA's data server."""

    _host = "https://www.ncei.noaa.gov"

    def __init__(self) -> None:
        """Initialize the HTTP connection object."""
        self.base_url = self._host

    def __enter__(self) -> Self:
        """Connect to the HTTP server."""
        return self

    def __exit__(self, *args) -> None:
        """Close the HTTP connection gracefully."""

    def read_file_as_bytes(self, filename: str | PathLike) -> typing.IO | gzip.GzipFile:
        """Read a file off of the server and into a byte stream."""
        stream = BytesIO()
        try:
            r = requests.get(f"{self.base_url}/{filename}", stream=True, timeout=15)
            stream.write(r.content)
            stream.seek(0)
        except requests.exceptions.RequestException as e:
            raise NOAAHTTPConnectionError(e) from e

        if utils.is_compressed(filename):
            return gzip.open(stream, "rb")

        return stream
