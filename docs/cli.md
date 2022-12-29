# Command Line Interface

`riweather` uses a small database to keep track of the weather stations that exist in
[NOAA's Integrated Surface Database (ISD)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database),
as well as the years for which there is data at each station. The command line
interface lets you rebuild this database from scratch, straight from sources present
on the ISD server itself.

It's a good idea to do this periodically because NOAA updates the ISD regularly, and
the database in `riweather` will eventually get out of date as new years and/or weather
stations are added.

Completely rebuilding the database requires two steps. First, `riweather` must
download a few files from the Internet. These files are:

* `isd-history.csv`, containing information about each weather station, including
  ID, name, elevation, and location.
* `isd-inventory.csv`, which records the number of weather observations per station
  and per year.
* `cb_2020_us_county_500k.zip` and `cb_2020_us_zcta520_500k.zip`, which are
  [US Census shapefiles](https://www2.census.gov/geo/tiger/GENZ2020/shp/) describing
  the boundaries of counties and [Zip Code Tabulation Areas (ZCTAs)](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html),
  respectively, in the United States.

These files are downloaded to a directory on your computer that you specify when you
run `riweather download-metadata`. The second step is to provide this same directory
to `riweather rebuild-db`. The files will be read and assembled into a SQLite database,
also located on your computer, which the package can then use to fetch station
information and the like.

## Example

To rebuild the database, first run

```shell
riweather download-metadata -d tmp_riweather_data
```

This will create a directory named `tmp_riweather_data` and download the files into it.
Next, run

```shell
riweather rebuild-db -s tmp_riweather_data
```

__This could take around 10 minutes to run, so please be patient.__ Once it finishes,
you can delete the `tmp_riweather_data` folder if you'd like -- those files are no
longer needed at this point.

# CLI Reference

::: mkdocs-click
    :module: riweather.cli
    :command: main
    :prog_name: riweather
