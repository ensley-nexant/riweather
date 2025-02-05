# `riweather`

`riweather` makes acquiring weather data easier. At its core, it is little more than a wrapper around 
[NOAA's Integrated Surface Database (ISD)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database), which provides hourly surface observations from data sources around 
the world.

With `riweather`, you can locate the nearest suitable weather station to a certain geographical location and fetch data 
from that station for one year or several years. You can opt to retrieve the raw observations, which sometimes occur at 
irregular intervals, or you can let `riweather` do some interpolating and resampling to fit the observations to a 
regular time interval of your choosing. Either way, the data is returned in the form of a 
[Pandas DataFrame][pandas.DataFrame] or [Series][pandas.Series] so that you can easily incorporate it into the rest of 
your analysis pipeline.

Move to the [next page](install.md) for installation instructions, and then see it in action on the 
[Quick How-To page](how_to.ipynb).
