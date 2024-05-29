import datetime

import geopandas as gpd
import numpy as np
import pandas as pd

import gtfsutils
import gtfsutils.filter

GTFS_SAMPLE_FEED = "tests/data/full_greater_sydney_gtfs_parent_tsn_1.zip"


def test__gtfsutils_load_gtfs():
    # Sample feed from:
    # https://opendata.transport.nsw.gov.au/dataset/temporary-andor-sample-gtfs-data/resource/5fc873b6-85d6-4c39-bdd5-6c3a102a703f
    filepath = GTFS_SAMPLE_FEED
    df_dict = gtfsutils.load_gtfs(filepath)

    assert isinstance(df_dict, dict)
    for key in df_dict:
        assert isinstance(df_dict[key], pd.DataFrame)


def test__gtfsutils_get_bounding_box():
    filepath = GTFS_SAMPLE_FEED
    bbox = gtfsutils.get_bounding_box(filepath)

    assert isinstance(bbox, list) or isinstance(bbox, np.ndarray)
    assert len(bbox) == 4
    assert (bbox[0] <= bbox[2]) and (bbox[1] <= bbox[3])


def test__gtfsutils_get_calendar_date_range():
    filepath = GTFS_SAMPLE_FEED
    min_data, max_date = gtfsutils.get_calendar_date_range(filepath)

    assert isinstance(min_data, datetime.datetime) and isinstance(
        max_date, datetime.datetime
    )
    assert min_data <= max_date


def test__cast_gtfs_file_ids():
    filepath = GTFS_SAMPLE_FEED
    df_dict = gtfsutils.load_gtfs(filepath)
    gtfsutils.cast_gtfs_ids(df_dict, "agency")
    gtfsutils.cast_gtfs_ids(df_dict, "routes")
    gtfsutils.cast_gtfs_ids(df_dict, "trips")
    gtfsutils.cast_gtfs_ids(df_dict, "calendar")
    gtfsutils.cast_gtfs_ids(df_dict, "stops")
    gtfsutils.cast_gtfs_ids(df_dict, "shapes")

    assert isinstance(df_dict["agency"]["agency_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["routes"]["agency_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["routes"]["route_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["trips"]["route_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["trips"]["service_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["trips"]["shape_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["trips"]["trip_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["calendar"]["service_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["stops"]["stop_id"].dtype, pd.StringDtype)
    assert isinstance(df_dict["shapes"]["shape_id"].dtype, pd.StringDtype)


def test__get_stop_ids_including_stations_within_geometry():
    filepath = GTFS_SAMPLE_FEED
    df_dict = gtfsutils.load_gtfs(filepath)
    bounds = gpd.read_file("tests/data/sydney_subset.geojson").geometry.unary_union

    stop_ids = gtfsutils.filter.get_stop_ids_including_stations_within_geometry(
        df_dict, bounds
    )
    assert len(stop_ids) == 47
    assert sorted(stop_ids) == sorted(
        [
            "200020",
            "2000147",
            "20002",
            "200020.0",
            "200020_CNC1",
            "200020_CNC2",
            "200020_DP1",
            "200020_DP10",
            "200020_DP11",
            "200020_DP12",
            "200020_DP13",
            "200020_DP14",
            "200020_DP15",
            "200020_DP2",
            "200020_DP3",
            "200020_DP4",
            "200020_DP5",
            "200020_DP6",
            "200020_DP7",
            "200020_DP8",
            "200020_DP9",
            "200020_ENT1",
            "200020_ENT2",
            "200020_ENT3",
            "200020_ENT4",
            "200020_ENT5",
            "2000212",
            "2000215",
            "2000216",
            "2000217",
            "2000223",
            "2000225",
            "2000227",
            "2000229",
            "2000234",
            "2000235",
            "2000274",
            "20003",
            "2000351",
            "2000352",
            "20004",
            "2000449",
            "2000450",
            "2000451",
            "20005",
            "20006",
            "200065",
        ]
    )
