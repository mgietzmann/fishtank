import ee
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fishtank.dimensions.spatial import (
    add_spatial_keys_to_facts,
    build_spatial_dimension_addition,
    append_spatial_dimension_addition,
    H3_RESOLUTIONS,
)
from fishtank.dimensions.dates import (
    add_date_keys_to_facts,
    build_date_dimension_addition,
    append_date_dimension_addition,
)
from fishtank.db import get_engine


if __name__ == "__main__":
    ee.Authenticate()
    ee.Initialize(project="ee-marcelsanders96")

    start = datetime(2018, 1, 15)
    end = datetime(2018, 12, 15)
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += relativedelta(months=1)

    roi = ee.Geometry.BBox(-179, 34, -120, 79)
    for date in tqdm(dates):
        window_start = date - relativedelta(days=3)
        window_end = date + relativedelta(days=3)
        dataset = (
            ee.ImageCollection("NOAA/CDR/SST_PATHFINDER/V53")
            .filterDate(
                window_start.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")
            )
            .select(["sea_surface_temperature"])
        )
        pixel_info = dataset.getRegion(roi, 26000).getInfo()
        df = pd.DataFrame(pixel_info[1:], columns=pixel_info[0])
        df = df[~np.isnan(df["sea_surface_temperature"])]
        df["temperature_c"] = 0.01 * (df["sea_surface_temperature"] + 273.15)
        del df["sea_surface_temperature"]
        # average over time
        df = (
            df.groupby(["longitude", "latitude"])
            .agg({"temperature_c": "mean"})
            .reset_index()
        )

        # average by h3 key
        max_h3_key = f"h3_key_{max(H3_RESOLUTIONS)}"
        add_spatial_keys_to_facts(df, lat_col="latitude", lon_col="longitude")
        gdf = (
            df.groupby([max_h3_key])
            .agg({"temperature_c": "mean", "latitude": "mean", "longitude": "mean"})
            .reset_index()
        )
        del gdf[max_h3_key]

        # add spatial keys
        add_spatial_keys_to_facts(gdf, lat_col="latitude", lon_col="longitude")
        for resolution in H3_RESOLUTIONS:
            dimension = build_spatial_dimension_addition(gdf, resolution)
            if dimension.shape[0] > 0:
                append_spatial_dimension_addition(dimension, resolution)

        gdf["date"] = date
        add_date_keys_to_facts(gdf, date_col="date")
        dimension = build_date_dimension_addition(gdf)
        if dimension.shape[0] > 0:
            append_date_dimension_addition(dimension)

        gdf.to_sql(
            "sea_surface_temperature",
            get_engine(),
            if_exists="append",
            index=False,
        )
