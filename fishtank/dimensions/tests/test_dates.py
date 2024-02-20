import unittest
import unittest.mock as mock
import pandas as pd
from datetime import datetime
from psycopg2.errors import UndefinedTable


from fishtank.dimensions.dates import (
    add_date_keys_to_facts,
    build_date_dimension_addition,
)


class TestAddDateKeysToFacts(unittest.TestCase):
    def test_base_case(self):
        dataframe = pd.DataFrame(
            [
                {"my_date": datetime(1970, 1, 1, 0, 0, 0), "fact": "a cool fact"},
                {"my_date": datetime(1970, 1, 1, 3, 0, 0), "fact": "a less cool fact"},
                {"my_date": datetime(1970, 1, 2, 3, 0, 0), "fact": "a boring fact"},
            ]
        )
        add_date_keys_to_facts(dataframe, "my_date")
        assert set(dataframe.columns) == set(["my_date", "fact", "date_key"])
        assert dataframe.shape[0] == 3
        assert dataframe["date_key"].dtype == int
        assert (dataframe["date_key"].values == [0, 0, 24 * 3600]).all()


class TestBuildDateDimensionAddition(unittest.TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame(
            [
                {
                    "fact": "a cool fact",
                    "date_key": 0,
                },
                {
                    "fact": "a less cool fact",
                    "date_key": 0,
                },
                {
                    "fact": "a boring fact",
                    "date_key": 24 * 3600,
                },
            ]
        )

    def test_all_new_keys(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.return_value = pd.DataFrame(
                [
                    {"date_key": 2 * 24 * 3600},
                ]
            )
            results = build_date_dimension_addition(self.dataframe)
        assert results.shape[0] == 2
        assert set(results["year"]) == set([1970])
        assert set(results["month"]) == set([1])
        assert set(results["day"]) == set([1, 2])
        assert set(results.columns) == set(["date_key", "date", "year", "month", "day"])

    def test_some_new_keys(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.return_value = pd.DataFrame(
                [
                    {"date_key": 1 * 24 * 3600},
                ]
            )
            results = build_date_dimension_addition(self.dataframe)
        assert results.shape[0] == 1
        assert set(results.columns) == set(["date_key", "date", "year", "month", "day"])

    def test_no_new_keys(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.return_value = pd.DataFrame(
                [
                    {"date_key": 1 * 24 * 3600},
                    {"date_key": 0 * 24 * 3600},
                ]
            )
            results = build_date_dimension_addition(self.dataframe)
        assert results.shape[0] == 0

    def test_table_does_not_exist(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.side_effect = UndefinedTable
            results = build_date_dimension_addition(self.dataframe)
        assert results.shape[0] == 2
        assert set(results.columns) == set(["date_key", "date", "year", "month", "day"])
