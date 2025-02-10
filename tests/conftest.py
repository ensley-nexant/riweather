"""Test suite configuration."""

import gzip
import os
import tempfile
from pathlib import Path

import pytest

import riweather
from riweather import utils


@pytest.fixture(scope="module")
def mock_requests_get(module_mocker):
    """Mocked requests.get object."""
    mock = module_mocker.patch("requests.get")
    content = b"mock file contents"
    mock.return_value.iter_content.return_value = [content]
    mock.return_value.headers = {"Content-Length": len(content)}
    return mock


@pytest.fixture(scope="module")
def mock_ftp(module_mocker):
    """Mocked ftplib.FTP object."""

    def _side_effect(cmd, callback):
        contents = b"mock file contents"
        if utils.is_compressed(cmd):
            contents = gzip.compress(b"compressed " + contents)
        return callback(contents)

    mock = module_mocker.patch("ftplib.FTP", autospec=True)
    mock.return_value.retrbinary.side_effect = _side_effect
    mock.return_value.getwelcome.return_value = b"Welcome to the mock FTP server!"
    return mock


@pytest.fixture
def cleandir():
    """Make tests start and end in a clean temporary directory."""
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        yield
        os.chdir(old_cwd)


@pytest.fixture(scope="module")
def mock_ftp_data(module_mocker):
    """Mocked ftplib.FTP object."""

    def _side_effect(cmd, callback):
        with open(Path(__file__).parent / "data/720534-00161-2025.gz", "rb") as f:
            contents = f.read()
        return callback(contents)

    mock = module_mocker.patch("ftplib.FTP", autospec=True)
    mock.return_value.retrbinary.side_effect = _side_effect
    return mock


@pytest.fixture(scope="module")
def stn(mock_ftp_data):
    return riweather.Station("720534")


@pytest.fixture(scope="module")
def stndata(stn):
    return stn.fetch_raw_data(2025)
