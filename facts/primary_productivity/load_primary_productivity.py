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

    start = datetime(2018, 3, 15)
    end = datetime(2018, 12, 15)
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += relativedelta(months=1)

    roi = ee.Geometry.BBox(-179, 34, -120, 79)
    for date in tqdm(dates):
        window_start = date - relativedelta(days=7)
        window_end = date + relativedelta(days=7)
        dataset = (
            ee.ImageCollection("JAXA/GCOM-C/L3/OCEAN/CHLA/V2")
            .filterDate(
                window_start.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")
            )
            .filter(ee.Filter.eq("SATELLITE_DIRECTION", "D"))
            .select(["CHLA_AVE"])
        )

        pixel_info = dataset.getRegion(roi, 26000).getInfo()
        df = pd.DataFrame(pixel_info[1:], columns=pixel_info[0])
        df = df[~np.isnan(df["CHLA_AVE"])]
        # average over time
        df = (
            df.groupby(["longitude", "latitude"])
            .agg({"CHLA_AVE": "mean"})
            .reset_index()
        )

        # average by h3 key
        max_h3_key = f"h3_key_{max(H3_RESOLUTIONS)}"
        add_spatial_keys_to_facts(df, lat_col="latitude", lon_col="longitude")
        gdf = (
            df.groupby([max_h3_key])
            .agg({"CHLA_AVE": "mean", "latitude": "mean", "longitude": "mean"})
            .reset_index()
        )
        gdf["log_chla_ave"] = np.log(gdf["CHLA_AVE"])
        del gdf[max_h3_key]
        del gdf["CHLA_AVE"]

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
            "primary_productivity",
            get_engine(),
            if_exists="append",
            index=False,
        )
