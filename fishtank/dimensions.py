"""
Helper functions to help handle the dimensions of the fishtank.
"""

import h3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon

from fishtank.db import get_engine
from psycopg2.errors import UndefinedTable


"""
For our spatial data we're going to take a slightly different tack
than we do for some of the other dimensions. Because we know the
complete set of possible values for this dimension, we're going to
precompute the set of possible values and store them in the database
in advance. 

Also we're going to take advantage of uber h3 library so we can generate
all our keys without ever having to reference the database. In the following
if we say "h3_index" that's a string and if we say "h3_key" that's an integer.
"""

H3_RESOLUTIONS = [2, 4, 6]
H3_TABLE_PREFIX = "h3_resolution_"


def spatial_index_to_key(spatial_index):
    return int(spatial_index, 16)


def spatial_key_to_index(spatial_key):
    return hex(spatial_key)[2:]


def get_coords(h3_index):
    coords = h3.h3_to_geo_boundary(h3_index, True)
    coords = tuple((lon, lat) for lon, lat in coords)
    lons = [lon for lon, _ in coords]
    if max(lons) - min(lons) > 180:
        coords = tuple(
            (lon if lon > 0 else 180 + (180 + lon), lat) for lon, lat in coords
        )
    return coords


def add_spatial_keys_to_facts(dataframe, lon_col="lon", lat_col="lat"):
    """
    Returns a dataframe with the h3 keys for the given resolutions
    """
    for resolution in H3_RESOLUTIONS:
        dataframe[f"h3_key_{resolution}"] = dataframe.apply(
            lambda row: spatial_index_to_key(
                h3.geo_to_h3(row[lat_col], row[lon_col], resolution)
            ),
            axis=1,
        )


def build_spatial_dimension_addition(dataframe, resolution):
    assert resolution in H3_RESOLUTIONS

    keys = set(dataframe[f"h3_key_{resolution}"])
    keys_filter = ",".join([str(key) for key in keys])

    sql = f"""
    select distinct 
        h3_key_{resolution} 
    from 
        {H3_TABLE_PREFIX}{resolution}
    where
        h3_key_{resolution} in ({keys_filter})
    """
    try:
        existing_keys = set(pd.read_sql(sql, get_engine())[f"h3_key_{resolution}"])
    except UndefinedTable:
        existing_keys = set()

    new_keys = keys - existing_keys

    dataframe = gpd.GeoDataFrame(
        [
            {
                f"h3_key_{resolution}": key,
                "geometry": Polygon(get_coords(spatial_key_to_index(key))),
            }
            for key in new_keys
        ]
    )
    return dataframe


def append_spatial_dimension_addition(dataframe, resolution):
    assert resolution in H3_RESOLUTIONS
    assert dataframe.shape[0] > 0

    dataframe.to_postgis(
        f"{H3_TABLE_PREFIX}{resolution}",
        get_engine(),
        if_exists="append",
        index=False,
    )


"""
Likewise for dates we're going to have pregenerated keys. Specifically,
we'll just use the epoch of 12:00:00 AM on that day as the key.
"""

DATE_TABLE = "dates"


def add_date_keys_to_facts(dataframe, date_col="date"):
    dataframe["date_key"] = dataframe[date_col].astype("datetime64[s]").astype(int)
    dataframe["date_key"] = dataframe["date_key"] - dataframe["date_key"] % 86400


def build_date_dimension_addition(dataframe):
    keys = set(dataframe[f"date_key"])
    keys_filter = ",".join([str(key) for key in keys])

    sql = f"""
    select distinct 
        date_key
    from 
        {DATE_TABLE}
    where
        date_key in ({keys_filter})
    """
    try:
        existing_keys = set(pd.read_sql(sql, get_engine())[f"date_key"])
    except UndefinedTable:
        existing_keys = set()

    new_keys = keys - existing_keys

    dataframe = pd.DataFrame(new_keys, columns=["date_key"])
    dataframe["date"] = pd.to_datetime(dataframe["date_key"], unit="s")
    dataframe["year"] = dataframe["date"].dt.year
    dataframe["month"] = dataframe["date"].dt.month
    dataframe["day"] = dataframe["date"].dt.day

    return dataframe


def append_date_dimension_addition(dataframe):
    assert dataframe.shape[0] > 0

    dataframe.to_sql(
        f"{DATE_TABLE}",
        get_engine(),
        if_exists="append",
        index=False,
    )
