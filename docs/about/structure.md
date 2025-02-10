# Structure of `riweather` Data

The primary way to retrieve data using `riweather` is by using the [`riweather.Station.fetch_data`][] function. It 
returns data as a [Pandas DataFrame][pandas.DataFrame] for easy integration with other datasets and processing 
pipelines.

## Column names in the output

The columns of the retrieved data match the structure returned by 
[the `riweather` parser](integrated_surface_dataset.md#data-contents) and also that which is laid out in the 
[ISD Data Documentation](https://www.ncei.noaa.gov/pub/data/noaa/isd-format-document.pdf). Each column is named like

```text
df["<observation type>.<attribute name>"]
```

where `<observation type>` is one of the [fields of the mandatory data section][riweather.parser.MandatoryData] such as
`wind`, `air_temperature`, or `dew_point`, and `<attribute name>` is one of the child attributes like `speed_rate` or
`temperature_c`. So, if we fetch air temperature data from a station:

```pycon
>>> s = riweather.Station("720534")
>>> df = s.fetch_data(2024, "air_temperature")
```

That DataFrame will contain three columns, one for each of the 
[attributes of `AirTemperatureObservation`][riweather.parser.AirTemperatureObservation].

```pycon
>>> df.columns.to_list()
['air_temperature.temperature_c',
 'air_temperature.quality_code',
 'air_temperature.temperature_f']
>>> df["air_temperature.temperature_f"]
2024-01-01 00:15:00+00:00    34.16
2024-01-01 00:35:00+00:00    33.62
2024-01-01 00:55:00+00:00    32.00
2024-01-01 01:15:00+00:00    30.74
2024-01-01 01:35:00+00:00    29.48
Name: air_temperature.temperature_f, dtype: float64
```

The long column names are a lot of typing, but this was done to ensure there are no conflicts between similar types of 
observations. For example, `'air_temperature'` and `'dew_point'` are both `AirTemperatureObservations`, so they both 
have `temperature_c` as attributes.

```pycon
>>> df = s.fetch_data(2024, ["dew_point", "air_temperature"])
>>> df.columns.to_list()
['air_temperature.temperature_c',
 'air_temperature.quality_code',
 'air_temperature.temperature_f',
 'dew_point.temperature_c',
 'dew_point.quality_code',
 'dew_point.temperature_f']
```

We recommend renaming columns to something shorter once you have retrieved the data.

```pycon
>>> df = df.rename(columns={"air_temperature.temperature_f": "tempF", 
...                         "dew_point.temperature_f": "dewF"})
>>> df[["tempF", "dewF"]].head()
                           tempF   dewF
2024-01-01 00:15:00+00:00  34.16  20.48
2024-01-01 00:35:00+00:00  33.62  20.48
2024-01-01 00:55:00+00:00  32.00  19.40
2024-01-01 01:15:00+00:00  30.74  19.22
2024-01-01 01:35:00+00:00  29.48  19.22
```

## Time zones

!!! danger
    **All timestamps are reported in UTC by default.** If you are aligning weather data to another data set, make sure 
    you convert the weather data to the proper time zone first!

Timestamps in the Integrated Surface Dataset are always stored in UTC. This is great news as consumers of the data, 
because it eliminates any ambiguity around daylight savings conversions. However, it does mean that the weather 
observations need to be converted to local time before they can be aligned with other datasets.

You can do this very easily with [Pandas](https://pandas.pydata.org/docs/user_guide/timeseries.html#working-with-time-zones)
after you have retrieved the data.

```pycon
>>> s = riweather.Station("720534")
>>> df = s.fetch_data(2024, "air_temperature", temp_scale="F", include_quality_codes=False)
>>> df.head()  # note the timestamps ending in +00:00, indicating UTC
                           air_temperature.temperature_f
2024-01-01 00:15:00+00:00                          34.16
2024-01-01 00:35:00+00:00                          33.62
2024-01-01 00:55:00+00:00                          32.00
2024-01-01 01:15:00+00:00                          30.74
2024-01-01 01:35:00+00:00                          29.48
>>> df = df.tz_convert("US/Mountain")
                           air_temperature.temperature_f
2023-12-31 17:15:00-07:00                          34.16
2023-12-31 17:35:00-07:00                          33.62
2023-12-31 17:55:00-07:00                          32.00
2023-12-31 18:15:00-07:00                          30.74
2023-12-31 18:35:00-07:00                          29.48
```

Or, even easier, [`riweather.Station.fetch_data`][] will do it for you if you pass the `tz` parameter.

```pycon
>>> s = riweather.Station("720534")
>>> df = s.fetch_data(2024, "air_temperature", tz="US/Mountain",
...                   temp_scale="F", include_quality_codes=False)
                           air_temperature.temperature_f
2023-12-31 17:15:00-07:00                          34.16
2023-12-31 17:35:00-07:00                          33.62
2023-12-31 17:55:00-07:00                          32.00
2023-12-31 18:15:00-07:00                          30.74
2023-12-31 18:35:00-07:00                          29.48
```

## Quality codes

ISD data has lots of shorthand codes indicating the source(s) of the weather observations, the method by which they 
were collected, and the quality or reliability of the data. These codes make the data more complicated to look at 
and increase the number of columns by quite a bit, but they can nevertheless be helpful when determining how 
trustworthy the observations are. See [the Shorthand Codes page](shorthand_codes.md) for informative descriptions of 
what these codes mean. 

`riweather` returns these codes so that you can inspect them, but also provides some ways to suppress them from the 
output if you'd rather not clutter up the results. For example, specify `include_quality_codes=False` to prevent any 
column ending in `quality_code` from ending up in the output. 

```pycon
>>> s = riweather.Station("720534")
>>> df = s.fetch_data(2024, "air_temperature")
>>> df.columns.to_list()
['air_temperature.temperature_c',
 'air_temperature.quality_code',
 'air_temperature.temperature_f']
>>> df = s.fetch_data(2024, "air_temperature", include_quality_codes=False)
>>> df.columns.to_list()
['air_temperature.temperature_c',
 'air_temperature.temperature_f']
```

Of course, you could always drop the quality code columns yourself.

```pycon
>>> s = riweather.Station("720534")
>>> df = s.fetch_data(2024, "air_temperature")
>>> df.columns.to_list()
['air_temperature.temperature_c',
 'air_temperature.quality_code',
 'air_temperature.temperature_f']
>>> df = df.drop(columns=["air_temperature.quality_code"])
>>> df.columns.to_list()
['air_temperature.temperature_c',
 'air_temperature.temperature_f']
```
