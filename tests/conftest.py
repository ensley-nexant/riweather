import gzip
import os
import tempfile

import pytest

from riweather import utils


def pytest_configure(config):
    config.addinivalue_line("markers", "e2e: mark as end-to-end test")


@pytest.fixture(scope="module", autouse=True)
def mock_requests_get(module_mocker):
    mock = module_mocker.patch("requests.get")
    content = b"mock file contents"
    mock.return_value.iter_content.return_value = [content]
    mock.return_value.headers = {"Content-Length": len(content)}
    return mock


@pytest.fixture(scope="module", autouse=True)
def mock_ftp(module_mocker):
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
    with tempfile.TemporaryDirectory() as newpath:
        old_cwd = os.getcwd()
        os.chdir(newpath)
        yield
        os.chdir(old_cwd)
