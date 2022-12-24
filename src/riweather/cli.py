import click

from . import __version__


@click.group(context_settings={"auto_envvar_prefix": "RIWEATHER"})
@click.version_option(version=__version__)
def main():
    """riweather makes grabbing weather data easier."""
    pass
