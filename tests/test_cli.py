"""Test module for the command line interface."""

import os

import click.testing
import pytest

from riweather import cli


@pytest.fixture(scope="module")
def runner():
    """Run the CLI inside a test."""
    return click.testing.CliRunner()


def test_main_succeeds(runner):
    """Exits with a status code of zero."""
    result = runner.invoke(cli.main, ["--help"])
    assert result.exit_code == 0


@pytest.mark.usefixtures("cleandir")
class TestDownloadMetadata:
    """Test cases for downloading NOAA/Census metadata."""

    _true_filenames = (
        "cb_2020_us_county_500k.zip",
        "cb_2020_us_zcta520_500k.zip",
        "isd-history.csv",
        "isd-inventory.csv",
    )

    def test_succeeds(self, runner, mock_requests_get, mock_ftp):
        """Exits with a status code of zero."""
        result = runner.invoke(cli.main, ["download-metadata", "-d", "."])
        assert result.exit_code == 0

    def test_creates_files(self, runner, mock_requests_get, mock_ftp):
        """Creates the appropriate files in the correct directory."""
        runner.invoke(cli.main, ["download-metadata", "-d", "."])
        assert sorted(os.listdir(os.getcwd())) == sorted(self._true_filenames)

    def test_gets_data(self, runner, mock_ftp):
        """Retrieves the expected data for each of the files."""
        runner.invoke(cli.main, ["download-metadata", "-d", "."])
        for fn in self._true_filenames:
            with open(fn, "rb") as f:
                if fn == "isd-inventory.csv":
                    assert f.read() == b"compressed mock file contents"
                else:
                    assert f.read() == b"mock file contents"
