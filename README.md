# gtfsutils

GTFS command-line tool and Python GTFS utility library

# Installation

To install the development version from GitHub:

```bash
git clone git@github.com:hannes-br/gtfsutils.git
cd gtfsutils
pip install -e .  # Install in editable mode
```

# Usage

## Python package

The usage of the Python package is illustrated in [quickstart.ipynb](quickstart.ipynb).

## Command-line tool

The package can be also used as a command-line tool. There are three sub-tools available.

### Filter

The filter tool applies a filter to a GTFS file. You can either spatially filter based on stop locations or on trip shapes or filter by a daterange considering calendar and calendar_dates. The spatial filter can be specified either as a bounding box (xmin, ymin, xmax, ymax) or as a file path (e.g. to a GeoJSON or GPKG file). The daterange filter can be specified by a start and end date ('{"start_date": "YYYYMMDD", "end_date": "YYYYMMDD"}').

Here is how to spatially filter a GTFS file based on stop locations, using a bounding box:

```bash
gtfsutils filter -t stops data/vienna.gtfs.zip data/vienna-filtered.gtfs.zip "[16.197, 47.999, 16.549, 48.301]"
```

Here is how to spatially filter a GTFS file based on trip shapes, using a GeoJSON file:

```bash
gtfsutils filter -t shapes data/vienna.gtfs.zip data/vienna-filtered.gtfs.zip data/area.geojson
```

Here is how to filter a GTFS file based on calendar and calendar_dates, using a start and end date:

```bash
gtfsutils filter -t calendar data/vienna.gtfs.zip data/vienna-filtered.gtfs.zip '{"start_date": "20240121", "end_date": "20240129"}'
```

For more information, type:

```bash
gtfsutils filter --help
```

```
usage: gtfsutils filter [-h] [-t TARGET] [-o OPERATION] [--overwrite] [-v] src dst bounds

positional arguments:
  src                   Input GTFS filepath
  dst                   Output GTFS filepath
  bounds                Filter boundary

optional arguments:
  -h, --help            show this help message and exit
  -t TARGET, --target TARGET
                        Filter target (stops, shapes)
  -o OPERATION, --operation OPERATION
                        Filter operation (within, intersects)
  --overwrite           Overwrite if exists
  -v, --verbose         Verbose output
```

### Bounds

The bounds tool computes the bounding box (based on stop locations) of a GTFS file:

```bash
gtfsutils bounds data/vienna.gtfs.zip
```

```bash
[16.1977025532707, 47.9995020902886, 16.5494019702052, 48.3011051975429]
```

For more information, type:

```bash
gtfsutils bounds --help
```

```
usage: gtfsutils bounds [-h] src

positional arguments:
  src         Input GTFS filepath

optional arguments:
  -h, --help  show this help message and exit
```

### Info

The info tool print general information about a GTFS file:

```bash
gtfsutils info data/vienna.gtfs.zip
```

```bash
GTFS files:
  agency.txt                      2 rows
  calendar.txt                  186 rows
  calendar_dates.txt          9,575 rows
  routes.txt                    441 rows
  shapes.txt                401,371 rows
  stop_times.txt          3,733,781 rows
  stops.txt                   4,510 rows
  trips.txt                 201,042 rows

Calender date range:
  11.12.2022 - 09.12.2023

Bounding box:
  [16.1977025532707, 47.9995020902886, 16.5494019702052, 48.3011051975429]
```

For more information, type:

```bash
gtfsutils info --help
```

```
usage: gtfsutils info [-h] src

positional arguments:
  src         Input GTFS filepath

optional arguments:
  -h, --help  show this help message and exit
```

# Testing

Prepare dev environment with:

```bash
# Create virtual environement
python -m venv ./venv

# Install dependencies
pip install -r requirements-dev.txt
pip install -r requirements.txt

# Activate virtual environment
source venv/bin/activate
```

To run unit tests, type:

```bash
pytest -v
```

# License

This project is licensed under the MIT license. See the [LICENSE](LICENSE) for details.
