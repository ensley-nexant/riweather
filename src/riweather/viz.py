"""Visualization module for weather stations."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas as pd


def _get_extent(lat, lon, tlats, tlons, buffer=0.05, max_aspect_ratio=1.5):
    tlats.append(lat)
    tlons.append(lon)
    x_min, x_max = min(tlons) - buffer, max(tlons) + buffer
    y_min, y_max = min(tlats) - buffer, max(tlats) + buffer

    while (x_max - x_min) / (y_max - y_min) > max_aspect_ratio:
        y_min -= buffer
        y_max += buffer
    while (y_max - y_min) / (x_max - x_min) > max_aspect_ratio:
        x_min -= buffer
        x_max += buffer
    return x_min, x_max, y_min, y_max


def _get_zoom_level(distance_m, n_tiles=600):
    km = distance_m / 1000.0
    return int(np.log2(128 * n_tiles / km))


def _calculate_distance_labels(distance_m, distance_unit):
    if distance_unit not in ("m", "km", "mi"):
        msg = "Invalid distance unit. Must be m, km, or mi"
        raise ValueError(msg)

    if distance_unit == "m":
        distance = distance_m
        dist_str = f"{distance:,.0f} m"
    elif distance_unit == "km":
        distance = distance_m / 1000.0
        dist_str = f"{distance:,.1f} km"
    elif distance_unit == "mi":
        distance = distance_m / 1609.344
        dist_str = f"{distance:,.1f} mi"
    else:
        msg = "Invalid distance unit. Must be m, km, or mi"
        raise ValueError(msg)

    return dist_str


def plot_stations(
    lat: float,
    lon: float,
    ranked_stations: pd.DataFrame,
    *,
    n: int | None = None,
    distance_unit: str = "m",
):
    """Plot stations relative to a location.

    Raises:
        ImportError: If [matplotlib][] and
            [folium](https://python-visualization.github.io/folium/latest) are not installed.

    Args:
        lat: Site latitude
        lon: Site longitude
        ranked_stations: Ranked stations
        n: The ``n`` top-ranked stations of ``ranked_stations`` will be plotted
        distance_unit: Distance unit to use on the plot. Must be meters (``m``),
            kilometers (``km``), or miles (``mi``)
    """
    try:
        import matplotlib.pyplot as plt  # noqa
    except ImportError:
        msg = "Plotting stations requires matplotlib"
        raise ImportError(msg) from None

    try:
        import folium
    except ImportError:
        msg = "Plotting stations requires folium"
        raise ImportError(msg) from None

    if n is None:
        n = ranked_stations.shape[0]
    station_info = ranked_stations.head(n)

    m = folium.Map(location=[lat, lon])
    folium.Marker([lat, lon], popup="Site").add_to(m)
    for row in station_info.itertuples():
        folium.Marker(
            [row.latitude, row.longitude],
            popup=row.name,
            icon=folium.Icon(icon="cloud"),
        ).add_to(m)
        folium.PolyLine(
            [[lat, lon], [row.latitude, row.longitude]],
            popup=_calculate_distance_labels(row.distance, distance_unit),
        ).add_to(m)

    return m
