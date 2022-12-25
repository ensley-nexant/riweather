import click.testing
import pytest

from riweather import cli


@pytest.fixture
def runner():
    return click.testing.CliRunner()


def test_main_succeeds(runner):
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
