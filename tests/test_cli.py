import os
import pathlib
import traceback

import click.testing
import pytest

from riweather import cli


@pytest.fixture(scope="module")
def runner():
    return click.testing.CliRunner()


def test_main_succeeds(runner):
    result = runner.invoke(cli.main)
    assert result.exit_code == 0


@pytest.mark.usefixtures("cleandir")
class TestDownloadMetadata:
    true_filenames = [
        "cb_2020_us_county_500k.zip",
        "cb_2020_us_zcta520_500k.zip",
        "isd-history.csv",
        "isd-inventory.csv",
    ]

    def test_succeeds(self, runner):
        result = runner.invoke(cli.main, ["download-metadata", "-d", "."])
        assert result.exit_code == 0

    def test_creates_files(self, runner):
        runner.invoke(cli.main, ["download-metadata", "-d", "."])
        assert os.listdir(os.getcwd()) == self.true_filenames

    def test_gets_data(self, runner):
        runner.invoke(cli.main, ["download-metadata", "-d", "."])
        for fn in self.true_filenames:
            with open(fn, "rb") as f:
                if fn == "isd-inventory.csv":
                    assert f.read() == b"compressed mock file contents"
                else:
                    assert f.read() == b"mock file contents"


@pytest.mark.e2e
def test_populate_succeeds_in_production(runner):
    runner.invoke(cli.download_metadata, ["--dst", "../data"])
    result = runner.invoke(cli.rebuild_db, ["--src", "../data"])
    traceback.print_exception(*result.exc_info)
    assert result.exit_code == 0
    assert (pathlib.Path.home() / ".riweather" / "metadata.db").exists()
