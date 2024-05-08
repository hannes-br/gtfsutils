import datetime

import numpy as np
import pandas as pd

import gtfsutils


def test__gtfsutils_load_gtfs():
    # Sample feed from:
    # https://developers.google.com/transit/gtfs/examples/gtfs-feed
    # https://developers.google.com/static/transit/gtfs/examples/sample-feed.zip
    filepath = "tests/data/sample-feed.zip"
    df_dict = gtfsutils.load_gtfs(filepath)

    assert isinstance(df_dict, dict)
    for key in df_dict:
        assert isinstance(df_dict[key], pd.DataFrame)


def test__gtfsutils_get_bounding_box():
    filepath = "tests/data/sample-feed.zip"
    bbox = gtfsutils.get_bounding_box(filepath)

    assert isinstance(bbox, list) or isinstance(bbox, np.ndarray)
    assert len(bbox) == 4
    assert (bbox[0] <= bbox[2]) and (bbox[1] <= bbox[3])


def test__gtfsutils_get_calendar_date_range():
    filepath = "tests/data/sample-feed.zip"
    min_data, max_date = gtfsutils.get_calendar_date_range(filepath)

    assert isinstance(min_data, datetime.datetime) and isinstance(
        max_date, datetime.datetime
    )
    assert min_data <= max_date


def test__cast_gtfs_file_ids():
    filepath = "tests/data/sample-feed.zip"
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
