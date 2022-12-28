"""External connection objects."""
import ftplib
import gzip
import os
import typing
from io import BytesIO

from riweather import utils


class NOAAFTPConnectionException(Exception):
    """Exception for bad FTP connections."""

    pass


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
        ...
        >>> "You are accessing a U.S. Government information system" in welcome
        True
        >>> contents.read(10)  # connection is closed outside of the with block
        b'Integrated'
    """

    _host = "ftp.ncei.noaa.gov"

    def __init__(self) -> None:
        """Initialize the FTP connection object."""
        self.ftp: ftplib.FTP | None = None

    def __enter__(self) -> "NOAAFTPConnection":
        """Connect to the FTP server.

        Returns:
            self
        """
        try:
            ftp = ftplib.FTP(host=self._host, timeout=30)
            ftp.login()
            self.ftp = ftp
        except OSError as e:
            raise NOAAFTPConnectionException(
                f"Could not connect to the host: {self._host}."
            ) from e
        return self

    def __exit__(self, *args) -> None:
        """Close the FTP connection gracefully.

        Raises:
            Any error encountered by `ftplib` is raised here.
        """
        try:
            self.ftp.quit()
        except ftplib.all_errors as e:
            print(e)
            self.ftp.close()

        self.ftp = None

    def read_file_as_bytes(
        self, filename: str | os.PathLike
    ) -> typing.IO | gzip.GzipFile:
        """Read a file off of the server and into a byte stream.

        Args:
            filename: The name/path of the file on the FTP server.

        Returns:
            The file contents. If `filename` ends with ".z" or ".gz", which is
                the case with many of NOAA's data files, then the results are
                decompressed automatically using [gzip][].
        """
        if self.ftp is None:
            raise NOAAFTPConnectionException(
                "FTP connection could not be established."
            ) from None
        try:
            stream = BytesIO()
            self.ftp.retrbinary("RETR {}".format(filename), stream.write)
            stream.seek(0)
        except ftplib.all_errors as e:
            raise NOAAFTPConnectionException(e) from e

        if utils.is_compressed(filename):
            return gzip.open(stream, "rb")
        else:
            return stream
