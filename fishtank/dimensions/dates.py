"""
For dates we're going to have pregenerated keys. Specifically,
we'll just use the epoch of 12:00:00 AM on that day as the key.
"""

import pandas as pd
import sqlalchemy as sa
from psycopg2 import errors, errorcodes
from fishtank.db import get_engine

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
    except sa.exc.ProgrammingError as e:
        try:
            raise e.orig
        except errors.lookup(errorcodes.UNDEFINED_TABLE):
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
