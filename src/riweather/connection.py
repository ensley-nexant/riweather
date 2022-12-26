import ftplib
import gzip
import os
import typing
from io import BytesIO

from riweather import utils


class NOAAFTPConnection:
    _host = "ftp.ncei.noaa.gov"

    def __init__(self):
        self.ftp: ftplib.FTP | None = None

    def __enter__(self):
        ftp = ftplib.FTP(host=self._host, timeout=30)
        ftp.login()
        self.ftp = ftp
        return self

    def __exit__(self, *args):
        try:
            self.ftp.quit()
        except ftplib.all_errors as e:
            print(e)
            self.ftp.close()

        self.ftp = None

    def read_file_as_bytes(self, filename: str | os.PathLike) -> typing.IO:
        stream = BytesIO()
        self.ftp.retrbinary("RETR {}".format(filename), stream.write)
        stream.seek(0)

        if utils.is_compressed(filename):
            return gzip.open(stream, "rb")
        else:
            return stream
