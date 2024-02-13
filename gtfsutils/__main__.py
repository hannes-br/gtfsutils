import json
import time
import logging
import argparse
import geopandas as gpd
from datetime import datetime

import gtfsutils
import gtfsutils.filter

logger = logging.getLogger(__name__)


def prepare_bounds(bounds):
    if bounds.startswith("["):
        return json.loads(bounds)
    else:
        return gpd.read_file(bounds).geometry.unary_union




def main():
    parser = argparse.ArgumentParser(description="GTFS Utilities")
    subparsers = parser.add_subparsers(dest="method", help="Method")

    # Filter method
    parser_filter = subparsers.add_parser("filter", help="Filter method")
    parser_filter.add_argument(dest="src", help="Input GTFS filepath")
    parser_filter.add_argument(dest="dst", help="Output GTFS filepath")
    parser_filter.add_argument("-b", "--bounds",
        dest="bounds",
        help="Filter boundary")
    parser_filter.add_argument("-t", "--target",
        dest='target',
        help="Filter target (stops, shapes, calendar)",
        default="stops")
    parser_filter.add_argument("-o", "--operation",
        dest='operation',
        help="Filter operation (within, intersects)",
        default="within")
    parser_filter.add_argument("-sd", "--start_date",
        dest="start_date",
        help="Filter start date (ISO 8601 format: e.g. '20240213')",
        default=None)
    parser_filter.add_argument("-ed", "--end_date",
        dest="end_date",
        help="Filter end date (ISO 8601 format: e.g. '20240213')",
        default=None)
    parser_filter.add_argument("--overwrite", action='store_true',
        dest='overwrite', help="Overwrite if exists")
    parser_filter.add_argument('-v', '--verbose', action='store_true',
        dest='verbose', default=False,
        help="Verbose output")

    # Bounds method
    parser_bounds = subparsers.add_parser("bounds", help="Bounds method")
    parser_bounds.add_argument(dest="src", help="Input GTFS filepath")

    # Info method
    parser_info = subparsers.add_parser("info", help="Info method")
    parser_info.add_argument(dest="src", help="Input GTFS filepath")

    # Version method
    subparsers.add_parser("version", help="Print version")

    args = parser.parse_args()

    # # Verbose output
    if 'verbose' in args:
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=log_level)

    if args.method == "version":
        print(f"{gtfsutils.__name__} {gtfsutils.__version__}")

    elif args.method == "filter":
        assert args.src is not None, "No input file specified"
        assert args.dst is not None, "No output file specified"

        if args.target in ["stops", "shapes"]:
            assert args.bounds is not None, "No bounds defined"

        elif args.target == "calendar":
            assert args.start_date is not None, "No start date defined"
            assert args.end_date is not None, "No end date defined"

        # Load GTFS
        t = time.time()
        df_dict = gtfsutils.load_gtfs(args.src)
        duration = time.time() - t
        logger.debug(f"Loaded {args.src} in {duration:.2f}s")

        # Filter GTFS
        if args.target == 'stops':
            bounds = prepare_bounds(args.bounds)
            t = time.time()
            gtfsutils.filter.spatial_filter_by_stops(
                df_dict, bounds)
        elif args.target == 'shapes':
            bounds = prepare_bounds(args.bounds)
            t = time.time()
            gtfsutils.filter.spatial_filter_by_shapes(
                df_dict, bounds, operation=args.operation)
        elif args.target == 'calendar':
            # Prepare start_date and end_date
            start_date = datetime.strptime(args.start_date, "%Y%m%d")
            end_date = datetime.strptime(args.end_date, "%Y%m%d")
            t = time.time()
            gtfsutils.filter.filter_by_calendar(
                df_dict, start_date, end_date)
            import sys
            sys.exit()
        else:
            raise ValueError(
                f"Target {args.target} not supported!")
        duration = time.time() - t
        logger.debug(f"Filtered {args.src} in {duration:.2f}s")

        # Save filtered GTFS
        t = time.time()
        gtfsutils.save_gtfs(df_dict, args.dst, ignore_required=True, overwrite=args.overwrite)
        duration = time.time() - t
        logger.debug(f"Saved to {args.dst} in {duration:.2f}s")
    
    elif args.method == "bounds":
        assert args.src is not None, "No input file specified"

        bounds = gtfsutils.get_bounding_box(args.src)
        print(bounds)
    
    elif args.method == "info":
        assert args.src is not None, "No input file specified"
        gtfsutils.print_info(args.src)

    elif args.method == "merge":
        raise NotImplementedError("Merge not implemented")


if __name__ == '__main__':
    main()
