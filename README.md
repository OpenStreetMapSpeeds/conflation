                                 __     _              _        _                    
       __      ___    _ _       / _|   | |    __ _    | |_     (_)     ___    _ _    
      / _|    / _ \  | ' \     |  _|   | |   / _` |   |  _|    | |    / _ \  | ' \   
      \__|_   \___/  |_||_|   _|_|_   _|_|_  \__,_|   _\__|   _|_|_   \___/  |_||_|  
    _|"""""|_|"""""|_|"""""|_|"""""|_|"""""|_|"""""|_|"""""|_|"""""|_|"""""|_|"""""| 
    "`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-' 

Conflation is an open source project / script within OpenStreetMapSpeeds that aims to approximate driving speeds on
location-specific road classes using public GPS trace data. It utilizes other open-source projects and APIs such
as [Mapillary](https://www.mapillary.com/) and [Valhalla](https://github.com/valhalla/valhalla).

## License

Conflation uses the [MIT License](COPYING).

## Overview

This project was inspired by this [issue from the Valhalla project](https://github.com/valhalla/valhalla/issues/3021).
To summarize, Valhalla is capable of building routing tilesets and running routes on those tiles. However, the estimated
time of arrival can be fairly inaccurate, especially in urban areas with high traffic. The estimated speeds were more
akin to driving alone at night, which is not ideal since most people drive in the day in urbanized areas.

Conflation aims to provide a *statistical approach* to estimating of driving speeds. Specifically, this project
estimates speeds across different road classes using open-source GPS trace data. The road classes used will be taken
from [OpenStreetMap (OSM)](https://www.openstreetmap.org/), which delineates between motorways, trunks, primary roads,
secondary roads, residential, etc. To further refine our estimates, we will also split up our results by geographic
area (country and region), as well as by urban, suburban, and rural settings.

### Method

A high-level methodology of this script is as follows:

1. Create an `output/` folder where results will be stored.
    1. This script store intermediate results on disk, so that if a run is interrupted (either intentionally or
       accidentally) it can automatically pick up from where it left off. Since we eventually intend to have this script
       runnable on the entire planet, not having to make repeated API calls or repeat calculations will be useful.
    2. This script also will store the final results in this output folder (`output/results/`).
2. Break up the given bounding box into smaller sections (to make it more manageable on the API).
3. Make API calls to the specified GPS trace source, pulling trace data on all the bounding box sections.
4. Filter the trace data.
    1. The driving sequence needs to contain enough trace points.
    2. The total distance traveled must exceed some threshold.
    3. The total time elapsed must exceed some threshold.
    4. The average speed needs to be within some threshold, which tells us that the user is likely driving as opposed to
       walking, biking, riding on a train, etc.
    5. The adjacent trace points should be within some small distance of each other (i.e. there shouldn't be any large
       gaps in the sequence data).
5. Perform [map matching using the Valhalla service](https://github.com/valhalla/valhalla/blob/master/docs/api/map-matching/api-reference.md).
   This provides us a set of per-edge speed approximations. These per-edge approximations will be stored on disk using 
   the country and region as keys. 
6. Filter the map matching results.
    1. The results need to have a threshold percentage of successful matches.
7. An overall result will be derived from the per-edge measurements from Step 5, using a statistical approach.

## Structure

There is a specific JSON structure that this script outputs. Here is an example:

```json
[{
  "iso3166-1": "",
  "iso3166-2": "",
  "urban": {
    "way": [80,40,30,30,25,20,15,10],
    "link_exiting": [60,40,40,35,30],
    "link_turning": [60,30,25,25,20],
    "roundabout": [25,25,20,20,20,20,15,15],
    "driveway": 15,
    "alley": 10,
    "parking_aisle": 10,
    "drive-through": 10
  },
  "rural": {
    "way": [95,60,50,40,35,25,20,10],
    "link_exiting": [55,45,40,40,35],
    "link_turning": [50,35,35,30,25],
    "roundabout": [45,35,25,25,20,20,20,10],
    "driveway": 15,
    "alley": 10,
    "parking_aisle": 15,
    "drive-through": 10
  }
}]
```

## Running

### Quickstart Example

This project uses Python 3.9. The script can be run by setting up a virtualenv, installing modules from 
`requirements.txt`, and calling the `conflation/main.py` script.

Here is an example setup and run across a wide area in Manhattan NYC, with Mapillary client ID redacted (assuming you 
have Python 3.9 installed using `python3`):

```bash
python3 -m venv venv
source venv/bin/activate
pip install .
conflation --bbox=-74.01763916015625,40.71135347314246,-73.97266387939453,40.74556629114773 --traces-source {\"provider\":\"mapillary\",\"client_id\":\"client_id\"}
# or
python3 -m conflation --bbox=...
```

### Arguments

 There are a few args that need to be specified:

| Argument | Behavior |
|----------|----------|
| `--bbox` | Filter by the bounding box on the map, given as `min_longitude,min_latitude,max_longitude,max_latitude` |
| `--concurrency` | The number of processes to use while running the script (default your # of cpus) |
| `--trace-config` | Config JSON for the GPS trace provider; see below for more details |
| `--map-matching-config` | Config JSON for the map matching provider; see below for more details |

For the `--trace-config` and `--map-matching-config` arguments, JSONs needs to be specified as the value. Here are the
keys accepted by both JSONs:

####--trace-config

Currently, only Mapillary is supported as an API trace provider. The JSON can hold the following keys:

| Key | Behavior |
|----------|----------|
| `provider` | Should be set to `mapillary` |
| `client_id` | The Mapillary client ID that should be used for the OAuth flow. [More details here](https://www.mapillary.com/developer/api-documentation#authentication) |
| `client_secret` | The Mapillary client secret that should be used for the OAuth flow. [More details here](https://www.mapillary.com/developer/api-documentation#authentication) |
| `start_date` | Optional - Only traces older than this date will be pulled. Default = 5 years ago |
| `max_sequences_per_bbox_section` | Optional - Number of Mapillary sequences that should be pulled for each bbox section (i.e each zoom 14 tile). Default = 500 |
| `skip_if_fewer_imgs_than` | Optional - Skip a Mapillary sequence if it has fewer Mapillary images than this value. Default = 30 |

####--map-matching-config

Currently, only Valhalla is supported as a map matching provider. The JSON can hold the following keys:

| Key | Behavior |
|----------|----------|
| `provider` | Should be set to `valhalla` |
| `base_url` | The base URL of your running Valhalla service (example format: `https://aws.my.valhalla.com/`)  |
| `headers` | Optional - Headers JSON that will be passed along in each call to Valhalla |


## Contributing

We welcome contributions to Conflation. If you would like to report an issue, or even better fix an existing one, please
use the [Conflation issue tracker](https://github.com/OpenStreetMapSpeeds/conflation/issues) on GitHub.

To install the project in development mode plus the needed libraries, do a `pip install -e ".[dev]"`.

We encourage you to install the pre-commit hooks by typing `pre-commit install` which will run the following commands to lint and style-check your code before committing:
```shell script
flake8 .
black .
```

### Tests

TODO

