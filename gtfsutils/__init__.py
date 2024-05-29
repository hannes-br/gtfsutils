import datetime
import logging
import os
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import shapely.geometry

__version__ = "0.0.5"

logger = logging.getLogger(__name__)


REQUIRED_GTFS_FILES = [
    "agency",
    "stops",
    "routes",
    "trips",
    "calendar",
    "stop_times",
    # 'shapes',
    # 'frequencies',
    # 'feedinfo'
]

# https://developers.google.com/transit/gtfs/reference
AVAILABLE_GTFS_FILES = [
    "agency",
    "stops",
    "routes",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
    "fare_attributes",
    "fare_rules",
    "shapes",
    "frequencies",
    "transfers",
    "pathways",
    "levels",
    "feed_info",
    "translations",
    "attributions",
]

ROUTE_TYPE_MAP = {
    0: "tram, light_rail",
    1: "subway",
    2: "rail, railway, train",
    3: "bus, ex-bus",
    4: "ferry",
    5: "cableCar",
    6: "gondola",
    7: "funicular",
}

COLUMNS_DEPENDENCY_DICT = {
    "agency": ("agency_id", ["routes"]),
    "routes": ("route_id", ["trips"]),
    "trips": ("trip_id", ["stop_times"]),
    "stops": ("stop_id", ["stop_times"]),
    "calendar": ("service_id", ["calendar_dates"]),
    "shapes": ("shape_id", ["trips"]),
}

GTFS_ENUM_DATA_TYPES = {
    "attributions": {"is_producer": "Int64"},
    "calendar": {
        "monday": "Int64",
        "tuesday": "Int64",
        "wednesday": "Int64",
        "thursday": "Int64",
        "friday": "Int64",
        "saturday": "Int64",
        "sunday": "Int64",
    },
    "calendar_dates": {"exception_type": "Int64"},
    "routes": {
        "route_type": "Int64",
        "continuous_pickup": "Int64",
        "continuous_drop_off": "Int64",
    },
    "stop_times": {
        "pickup_type": "Int64",
        "drop_off_type": "Int64",
        "continuous_pickup": "Int64",
        "continuous_drop_off": "Int64",
        "timepoint": "Int64",
    },
    "stops": {"location_type": "Int64", "wheelchair_boarding": "Int64"},
    "transfers": {"transfer_type": "Int64"},
    "trips": {
        "direction_id": "Int64",
        "wheelchair_accessible": "Int64",
        "bikes_allowed": "Int64",
    },
}

GTFS_IDS = [
    "agency_id",
    "service_id",
    "level_id",
    "stop_id",
    "parent_station",
    "zone_id",
    "level_id",
    "route_id",
    "fare_id",
    "origin_id",
    "destination_id",
    "contains_id",
    "shape_id",
    "trip_id",
    "block_id",
    "from_stop_id",
    "to_stop_id",
    "from_route_id",
    "to_stop_id",
    "pathway_id",
    "record_id",
    "record_sub_id",
]


def cast_lat_lon(df_dict):
    df_dict["stops"]["stop_lat"] = df_dict["stops"]["stop_lat"].astype(float)
    df_dict["stops"]["stop_lon"] = df_dict["stops"]["stop_lon"].astype(float)


def cast_gtfs_enum_columns(df_dict, filekey):
    if filekey in GTFS_ENUM_DATA_TYPES.keys():
        for column, dtype in GTFS_ENUM_DATA_TYPES[filekey].items():
            if column in df_dict[filekey].columns:
                df_dict[filekey][column] = df_dict[filekey][column].astype(dtype)


def cast_gtfs_ids(df_dict, filekey):
    for id in GTFS_IDS.items():
        if id in df_dict[filekey].columns:
            df_dict[filekey][id] = df_dict[filekey][id].to_string()


def load_gtfs(filepath, subset=None):
    df_dict = {}
    if os.path.isdir(filepath):
        for filename in os.listdir(filepath):
            filekey = filename.split(".txt")[0]
            if (subset is None) or (filekey in subset):
                try:
                    df_dict[filekey] = pd.read_csv(
                        os.path.join(filepath, filename), low_memory=False
                    )
                    cast_gtfs_enum_columns(df_dict, filekey)
                    cast_gtfs_ids(df_dict, filekey)
                    if filekey == "stops":
                        cast_lat_lon(df_dict)
                except Exception as e:
                    logger.error(f"[{e.__class__.__name__}] {e} for {filename}")
    else:
        with ZipFile(filepath) as z:
            for filename in z.namelist():
                filekey = filename.split(".txt")[0]
                if (subset is None) or (filekey in subset):
                    try:
                        df_dict[filekey] = pd.read_csv(
                            z.open(filename), low_memory=False
                        )
                        cast_gtfs_enum_columns(df_dict, filekey)
                        cast_gtfs_ids(df_dict, filekey)
                        if filekey == "stops":
                            cast_lat_lon(df_dict)
                    except Exception as e:
                        logger.error(f"[{e.__class__.__name__}] {e} for {filename}")

    return df_dict


def load_stops(src):
    if isinstance(src, str):
        df_dict = load_gtfs(src, subset=["stops"])
    elif isinstance(src, dict):
        df_dict = src
    else:
        raise ValueError(f"Data type not supported: {type(src)}")

    stops = df_dict["stops"].copy()
    geoms = gpd.points_from_xy(stops.stop_lon, stops.stop_lat)

    return gpd.GeoDataFrame(stops, geometry=geoms, crs="EPSG:4326")


def load_shapes(src, geom_type="linestring"):
    if isinstance(src, str):
        df_dict = load_gtfs(src, subset=["shapes"])
    elif isinstance(src, dict):
        df_dict = src
    else:
        raise ValueError(f"Data type not supported: {type(src)}")

    if "shapes" not in df_dict:
        raise ValueError("shapes.txt not found in GTFS")

    if geom_type == "linestring":
        items = []
        for shape_id, g in df_dict["shapes"].groupby("shape_id"):
            g = g.sort_values("shape_pt_sequence")
            coords = g[["shape_pt_lon", "shape_pt_lat"]].values

            if len(coords) > 1:
                items.append(
                    {"shape_id": shape_id, "geom": shapely.geometry.LineString(coords)}
                )

        gdf = gpd.GeoDataFrame(items, geometry="geom", crs="EPSG:4326")

    elif geom_type == "point":
        shapes = df_dict["shapes"].copy()
        geoms = gpd.points_from_xy(shapes.shape_pt_lon, shapes.shape_pt_lat)

        gdf = gpd.GeoDataFrame(shapes, geometry=geoms, crs="EPSG:4326")

    else:
        raise ValueError(f"Geometry type {geom_type} not supported!")

    return gdf


def save_gtfs(df_dict, filepath, ignore_required=False, overwrite=False):
    if not ignore_required and not all(
        key in df_dict.keys() for key in REQUIRED_GTFS_FILES
    ):
        raise ValueError("Not all required GTFS files in dictionary")

    if overwrite or not os.path.exists(filepath):
        with ZipFile(filepath, "w") as zf:
            for filekey, df in df_dict.items():
                buffer = df.to_csv(index=False)
                zf.writestr(filekey + ".txt", buffer)


def get_bounding_box(src):
    if isinstance(src, str):
        df_dict = load_gtfs(src, subset=["stops"])
    elif isinstance(src, dict):
        df_dict = src
    else:
        raise ValueError(f"Data type not supported: {type(src)}")

    return [
        df_dict["stops"]["stop_lon"].min(),
        df_dict["stops"]["stop_lat"].min(),
        df_dict["stops"]["stop_lon"].max(),
        df_dict["stops"]["stop_lat"].max(),
    ]


def get_calendar_date_range(src):
    if isinstance(src, str):
        df_dict = load_gtfs(src, subset=["calendar"])
    elif isinstance(src, dict):
        df_dict = src
    else:
        raise ValueError(f"Data type not supported: {type(src)}")

    if "calendar" in df_dict:
        min_date = min(
            df_dict["calendar"]["start_date"].min(),
            df_dict["calendar"]["end_date"].min(),
        )
        max_date = max(
            df_dict["calendar"]["start_date"].max(),
            df_dict["calendar"]["end_date"].max(),
        )
        min_date = datetime.datetime.strptime(str(min_date), "%Y%m%d")
        max_date = datetime.datetime.strptime(str(max_date), "%Y%m%d")
    else:
        raise ValueError("calendar.txt missing")

    return min_date, max_date


def print_info(src):
    if isinstance(src, str):
        df_dict = load_gtfs(src)
    elif isinstance(src, dict):
        df_dict = src
    else:
        raise ValueError(f"Data type not supported: {type(src)}")

    print("\nGTFS files:")
    for key in sorted(df_dict.keys()):
        print(f"  {key + '.txt':<20s} {len(df_dict[key]):12,d} rows")

    min_date, max_date = get_calendar_date_range(df_dict)
    print(
        "\nCalender date range:\n  "
        f"{min_date.strftime('%d.%m.%Y')} - "
        f"{max_date.strftime('%d.%m.%Y')}"
    )

    bounds = get_bounding_box(df_dict)
    print(f"\nBounding box:\n  {bounds}\n")
