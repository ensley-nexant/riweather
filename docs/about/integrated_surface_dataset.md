`riweather` sources its data from NOAA's
[Integrated Surface Data](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) (ISD).
The ISD contains surface observations from weather stations around the world. The raw data is freely accessible at
this web address:

<https://www.ncei.noaa.gov/pub/data/noaa/>

The data files are organized into subfolders by year, and each file contains observations from one station and one
year. The URLs are structured like the following:

```text
https://www.ncei.noaa.gov/pub/data/noaa/<year>/<usaf_id>-<wban_id>-<year>.gz
```

where `<year>` is the calendar year, and `<usaf_id>` and `<wban_id>` are two identifiers representing the station. 
For example, one such URL is below.

!!! warning
    The link below is a direct link to a data file. Clicking it will download the file to your computer.

<https://www.ncei.noaa.gov/pub/data/noaa/2025/720534-00161-2025.gz>

You could download the appropriate files and work with them yourself, but you will need to determine the ID of
the correct station first. `riweather` makes that process easier by showing you the IDs and names of the closest 
stations to a given latitude and longitude, constructing the URLs properly, and downloading and uncompressing the 
files for you.

## Structure of the ISD data

Each ISD file is a compressed, plaintext, fixed-width file. As an example of what they look like, here are the first 
few lines of one of them.

```text
0184720534001612024010100154+40017-105050FM-15+156499999V0200201N001512200059N016093199+00121-00641999999ADDGD12991+0365819GE19AGL   +99999+99999GF104995999999036581999999MA1102031999999REMMET075METAR KEIK 010015Z AUTO 02003KT 10SM SCT120 01/M06 A3013 RMK AO2 T00121064=EQDD01      0ADE726
0125720534001612024010100354+40017-105050FM-15+156499999V0209999C000012200019N016093199+00091-00641999999ADDGF100991999999999999999999MA1102071999999REMMET072METAR KEIK 010035Z AUTO 00000KT 10SM CLR 01/M06 A3014 RMK AO2 T00091064=
0125720534001612024010100554+40017-105050FM-15+156499999V0209999C000012200019N016093199+00001-00701999999ADDGF100991999999999999999999MA1102071999999REMMET072METAR KEIK 010055Z AUTO 00000KT 10SM CLR 00/M07 A3014 RMK AO2 T00001070=
0126720534001612024010101154+40017-105050FM-15+156499999V0209999C000012200019N016093199-00071-00711999999ADDGF100991999999999999999999MA1102101999999REMMET073METAR KEIK 010115Z AUTO 00000KT 10SM CLR M01/M07 A3015 RMK AO2 T10071071=
0126720534001612024010101354+40017-105050FM-15+156499999V0209999C000012200019N016093199-00141-00711999999ADDGF100991999999999999999999MA1102101999999REMMET073METAR KEIK 010135Z AUTO 00000KT 10SM CLR M01/M07 A3015 RMK AO2 T10141071=
```

This is not very useful on its own. The 
[ISD Data Documentation](https://www.ncei.noaa.gov/pub/data/noaa/isd-format-document.pdf) defines the structure of each 
line:

* Characters 5-10: the USAF ID
* Characters 11-15: the WBAN ID
* Characters 16-23: the date of the observation in the format YYYYMMDD
* Characters 24-27: the time of the observation ([UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time)) in the format HHMM
* and so on

Looking at the first line in the sample, we see that the observation is from the station with USAF ID 720534, WBAN 
ID 00161, and occurred on 2024-01-01 00:15 UTC.

In addition to making stations easier to locate, the second advantage of `riweather` is that it parses these data 
fields out of the text files and returns them in a labeled, easy-to-use format ready for further processing and 
analysis.

## Data contents

According to the data documentation, every ISD observation contains three sections.

### Control data

The first 60 characters of each record provide information about where the record came from. This includes

* Station identifiers (USAF and WBAN)
* Date and time of the observation
* An indicator for the source or combination of sources used in creating the observation
* Latitude and longitude of the station
* Type of observation, such as the [National Solar Radiation Database](https://nsrdb.nrel.gov/) or a [METAR Aviation 
  routine weather report](https://en.wikipedia.org/wiki/METAR)
* Elevation of the observation relative to Mean Sea Level, in meters
* Call letters assigned to the station
* Type of quality control process applied to the observation

```pycon
>>> from riweather import parser
>>> line = "0184720534001612024010100154+40017-105050FM-15+156499999V0200201N001512200059N016093199+00121-00641999999"
>>> record = parser.parse_line(line)
>>> record.control
ControlData(total_variable_characters=184, usaf_id='720534', wban_id='00161', dt=datetime.datetime(2024, 1, 1, 0, 15, tzinfo=datetime.timezone.utc), data_source_flag='4', latitude=40.017, longitude=-105.05, report_type_code='FM-15', elevation=1564, call_letter_id=None, qc_process_name='V020')

```

### Mandatory data

The next 45 characters of each record contain basic meteorological information that is present in most observations. 
This includes

* Wind direction (degree angle relative to true north)
* Wind observation type, such as "calm", "normal", or "variable"
* Wind speed (meters per second)
* Sky ceiling height, the height above ground level of the lowest cloud cover, in meters
* A code representing the method of determining the ceiling, such as "estimated", "measured", or "aircraft"
* Visibility distance, in meters
* Air temperature, in degrees Celsius
* Dew point temperature, in degrees Celsius
* Sea level pressure, in hectopascals (hPa)

```pycon
>>> from riweather import parser
>>> line = "0184720534001612024010100154+40017-105050FM-15+156499999V0200201N001512200059N016093199+00121-00641999999"
>>> record = parser.parse_line(line)
>>> record.mandatory
MandatoryData(
    wind=WindObservation(
        direction_angle=20, 
        direction_quality_code='1', 
        type_code='N', 
        speed_rate=1.5, 
        speed_quality_code='1',
    ), 
    ceiling=SkyConditionObservation(
        ceiling_height=22000, 
        ceiling_quality_code='5', 
        ceiling_determination_code=None, 
        cavok_code='N',
    ), 
    visibility=VisibilityObservation(
        distance=16093, 
        distance_quality_code='1', 
        variability_code=None, 
        variability_quality_code='9',
    ), 
    air_temperature=AirTemperatureObservation(
        temperature_c=1.2,
        quality_code='1', 
        temperature_f=34.16,
    ), 
    dew_point=AirTemperatureObservation(
        temperature_c=-6.4, 
        quality_code='1', 
        temperature_f=20.48,
    ), 
    sea_level_pressure=AtmosphericPressureObservation(
        pressure=None, 
        quality_code='9',
    ),
)
```

### Additional data

The remaining characters of each record contain "additional data". This is data of variable length that may or may 
not be present and/or is recorded with varying degrees of frequency at the station.

!!! note
    Currently, `riweather` only parses the control and mandatory data sections. Support for the additional data 
    section is considered a future enhancement.
