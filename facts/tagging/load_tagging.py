import pandas as pd
from fishtank.dimensions.dates import (
    add_date_keys_to_facts,
    build_date_dimension_addition,
    append_date_dimension_addition,
)
from fishtank.db import get_engine

if __name__ == "__main__":
    # load up most likely tracks data
    tracks = pd.read_csv("data/HHM_Most_Likely_Tracks_CSV_Marcel_2.12.2024.csv")
    tracks = tracks.rename(
        {
            "Ptt": "ptt",
            "Most.Likely.Latitude": "latitude",
            "Most.Likely.Longitude": "longitude",
        },
        axis=1,
    )
    add_date_keys_to_facts(tracks, date_col="Date")
    tracks["ptt"] = tracks["ptt"].astype(str)
    del tracks["Date"]

    dimension = build_date_dimension_addition(tracks)
    if dimension.shape[0] > 0:
        append_date_dimension_addition(dimension)

    tracks.to_sql("tag_tracks", get_engine(), if_exists="replace", index=False)

    # load up the context
    inventory = pd.read_csv("data/HMM.Inventory_CSV_Marcel_2.12.2024.csv")

    inventory = inventory.rename(
        {
            "Ptt": "ptt",
            "tag.model": "tag_model",
            "time.series.resolution.min": "time_resolution_min",
            "fork.length.cm": "fork_length_cm",
            "deploy.latitude": "deploy_latitude",
            "deploy.longitude": "deploy_longitude",
            "End.Latitude": "end_latitude",
            "End.Longitude": "end_longitude",
            "hypothetical.data.retrieved": "hypothetical_data_retrieved",
            "data.type": "data_type",
            "deploy.date.GMT": "deploy_date",
            "end.date.time.GMT": "end_date",
            "Region": "region",
        },
        axis=1,
    )
    inventory["ptt"] = inventory["ptt"].astype(str)
    inventory["deploy_date"] = pd.to_datetime(inventory["deploy_date"])
    inventory["end_date"] = pd.to_datetime(inventory["end_date"])

    inventory.to_sql("tag_context", get_engine(), if_exists="replace", index=False)

    # pull the time series data
    time_series = pd.read_csv("data/HMM_Time_Series_Data_Marcel_2.12.2024.csv")

    time_series = time_series.rename(
        {
            "Ptt": "ptt",
            "depth.m": "depth_m",
            "temp.c": "temperature_c",
        },
        axis=1,
    )
    time_series["datetime"] = pd.to_datetime(time_series["date.time.GMT"])
    del time_series["date.time.GMT"]
    time_series["ptt"] = time_series["ptt"].astype(str)
    add_date_keys_to_facts(time_series, date_col="datetime")

    dimension = build_date_dimension_addition(time_series)
    if dimension.shape[0] > 0:
        append_date_dimension_addition(dimension)

    time_series.to_sql("tag_data", get_engine(), if_exists="replace", index=False)
