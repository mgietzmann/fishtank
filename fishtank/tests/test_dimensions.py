import unittest
import h3
import unittest.mock as mock
import pandas as pd
from psycopg2.errors import UndefinedTable

from fishtank.dimensions import (
    get_coords,
    spatial_index_to_key,
    spatial_key_to_index,
    add_spatial_keys_to_facts,
    build_spatial_dimension_addition,
    H3_RESOLUTIONS,
)


class TestGetCoords(unittest.TestCase):
    def test_get_coords_cross_meridian(self):
        # this example normally causes problems
        # by crossing the meridian
        h3_index = h3.geo_to_h3(0, -180, 2)
        # normally h3 will return a list of coordinates
        # where one of the longitudes is extremely far away
        # thus ruining our polygons
        # this function is supposed to fix that
        coords = get_coords(h3_index)
        lons = [lon for lon, _ in coords]
        assert max(lons) - min(lons) < 180

    def test_get_coords_no_cross_meridian(self):
        h3_index = h3.geo_to_h3(0, 0, 2)
        coords = get_coords(h3_index)
        assert coords == h3.h3_to_geo_boundary(h3_index, True)


class TestSpatialKeyToIndex(unittest.TestCase):
    def test_base_case(self):
        h3_index = h3.geo_to_h3(0, 0, 2)
        spatial_key = spatial_index_to_key(h3_index)
        assert isinstance(spatial_key, int)
        assert spatial_key_to_index(spatial_key) == h3_index


class TestAddSpatialKeysToFacts(unittest.TestCase):
    def test_base_case(self):
        dataframe = pd.DataFrame(
            [
                {"lon": 0, "lat": 0, "fact": "a cool fact"},
                {"lon": 0, "lat": 25, "fact": "a less cool fact"},
                {"lon": 25, "lat": 0, "fact": "a boring fact"},
            ]
        )
        add_spatial_keys_to_facts(dataframe)
        assert set(dataframe.columns) == set(
            ["lon", "lat", "fact"]
            + [f"h3_key_{resolution}" for resolution in H3_RESOLUTIONS]
        )
        assert dataframe.shape[0] == 3


class TestBuildSpatialDimensionAddition(unittest.TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame(
            [
                {
                    "lon": 0,
                    "lat": 0,
                    "fact": "a cool fact",
                    "h3_key_2": spatial_index_to_key(h3.geo_to_h3(0, 0, 2)),
                    "h3_key_4": spatial_index_to_key(h3.geo_to_h3(0, 0, 4)),
                    "h3_key_6": spatial_index_to_key(h3.geo_to_h3(0, 0, 6)),
                },
                {
                    "lon": 0,
                    "lat": 25,
                    "fact": "a less cool fact",
                    "h3_key_2": spatial_index_to_key(h3.geo_to_h3(25, 0, 2)),
                    "h3_key_4": spatial_index_to_key(h3.geo_to_h3(25, 0, 4)),
                    "h3_key_6": spatial_index_to_key(h3.geo_to_h3(25, 0, 6)),
                },
                {
                    "lon": 25,
                    "lat": 0,
                    "fact": "a boring fact",
                    "h3_key_2": spatial_index_to_key(h3.geo_to_h3(0, 25, 2)),
                    "h3_key_4": spatial_index_to_key(h3.geo_to_h3(0, 25, 4)),
                    "h3_key_6": spatial_index_to_key(h3.geo_to_h3(0, 25, 6)),
                },
            ]
        )

    def test_all_new_keys(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.return_value = pd.DataFrame(
                [
                    {"h3_key_2": 0},
                ]
            )
            results = build_spatial_dimension_addition(self.dataframe, 2)
        assert results.shape[0] == 3
        assert set(results.columns) == set(["h3_key_2", "geometry"])

    def test_some_new_keys(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.return_value = pd.DataFrame(
                [
                    {"h3_key_2": spatial_index_to_key(h3.geo_to_h3(0, 25, 2))},
                ]
            )
            results = build_spatial_dimension_addition(self.dataframe, 2)
        assert results.shape[0] == 2
        assert set(results.columns) == set(["h3_key_2", "geometry"])

    def test_no_new_keys(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.return_value = pd.DataFrame(
                [
                    {"h3_key_2": spatial_index_to_key(h3.geo_to_h3(0, 0, 2))},
                    {"h3_key_2": spatial_index_to_key(h3.geo_to_h3(25, 0, 2))},
                    {"h3_key_2": spatial_index_to_key(h3.geo_to_h3(0, 25, 2))},
                ]
            )
            results = build_spatial_dimension_addition(self.dataframe, 2)
        assert results.shape[0] == 0

    def test_table_does_not_exist(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.side_effect = UndefinedTable
            results = build_spatial_dimension_addition(self.dataframe, 2)
        assert results.shape[0] == 3
        assert set(results.columns) == set(["h3_key_2", "geometry"])

    def test_resolution_arg(self):
        with mock.patch("pandas.read_sql") as read_sql:
            read_sql.return_value = pd.DataFrame(
                [
                    {"h3_key_4": spatial_index_to_key(h3.geo_to_h3(0, 0, 4))},
                    {"h3_key_4": spatial_index_to_key(h3.geo_to_h3(25, 0, 4))},
                ]
            )
            results = build_spatial_dimension_addition(self.dataframe, 4)
        assert results.shape[0] == 1
        assert set(results.columns) == set(["h3_key_4", "geometry"])
