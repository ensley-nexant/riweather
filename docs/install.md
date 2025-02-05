# Installation

## With pip

```bash
pip install riweather
```

The basic installation method does not install the packages required for plotting
stations with [`riweather.plot_stations`][]. If you want to make plots, install the
optional `plots` dependencies:

```bash
pip install "riweather[plots]"
```

## Within a `pipx` Jupyter environment

If you have a [`pipx`](https://pipx.pypa.io/stable/) environment for local development
with [Jupyter](https://docs.jupyter.org/en/latest/), then you can [inject](https://pipx.pypa.io/stable/#inject-a-package)
`riweather` into that environment and it will be available to your notebooks.

```bash
pipx inject jupyter riweather
```

To update `riweather` to the latest version published on [PyPI](https://pypi.org/project/riweather/),
you can _uninject_ it (uninstalling it from your environment) and inject it again:

```bash
pipx uninject jupyter riweather
pipx inject jupyter riweather
```

## If data appears to be missing for the current year

If you are trying to fetch weather data for a certain year (usually it will be the
current year) and you are receiving no results, it could be that the station metadata
that is shipped with the `riweather` package needs to be refreshed. See 
[the CLI reference page](cli.md) for more details about why this needs to be 
done.
In a nutshell, you can refresh it manually by running the following from the command
line:

```bash
riweather download-metadata -d tmp_riweather_data
riweather rebuild-db -s tmp_riweather_data
```

This may take a few minutes to run. After it is complete, you can delete the 
`tmp_riweather_data` directory that was created.
