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
area (country and region), as well as by urban and rural settings.

The main idea

### Method

A rough list of steps this script makes is as follows:

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
[
  {
    "iso3166-1": "us",
    "iso3166-2": "pa",
    "urban": {
      "way": [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8
      ],
      "link_exiting": [
        9,
        10,
        11,
        12,
        13
      ],
      "link_turning": [
        15,
        16,
        17,
        18,
        19
      ],
      "roundabout": [
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28
      ],
      "driveway": 29,
      "alley": 30,
      "parking_aisle": 31,
      "drive-through": 32
    },
    "rural": {
      "way": [
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40
      ],
      "link_exiting": [
        41,
        42,
        43,
        44,
        45
      ],
      "link_turning": [
        47,
        48,
        49,
        50,
        51
      ],
      "roundabout": [
        53,
        54,
        55,
        56,
        57,
        58,
        59,
        60
      ],
      "driveway": 61,
      "alley": 62,
      "parking_aisle": 63,
      "drive-through": 64
    }
  }
]
```

## Running

This project can be run by calling the `get_trace_data.py` script. There are a few args that need to be specified:

| Argument | Behavior |
|----------|----------|
| `--bbox` | Filter by the bounding box on the map, given as `min_longitude,min_latitude,max_longitude,max_latitude` |
| `--concurrency` | The number of processes to use while running the script (default your # of cpus) |
| `--traces-source` | See below |

For the `--traces-source` argument, a JSON needs to be specified as the value. This JSON holds information on where and
how to pull the API trace data.

Currently, only Mapillary is supported as an API trace source. The `--traces-source` for Mapillary can hold the
following keys:

| Key | Behavior |
|----------|----------|
| `provider` | Should be set to `mapillary` |
| `client_id` | The Mapillary Client ID that should be used with the API calls. [More details here](https://www.mapillary.com/developer/api-documentation/#client-id) |
| `sequences_per_page` | Optional - Number of [Mapillary sequences](https://www.mapillary.com/developer/api-documentation/#sequences) that should be pulled in each API call |
| `skip_if_fewer_images_than` | Optional - Skip a Mapillary sequence if it has fewer Mapillary images than this value |

Here is an example setup and run across a wide area in Manhattan NYC, with Mapillary client ID redacted (assuming you
have Python 3.9 installed using `python3`):

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python ./get_trace_data.py --bbox=-74.01763916015625,40.71135347314246,-73.97266387939453,40.74556629114773 --traces-source {\"provider\":\"mapillary\",\"client_id\":\"client_id\"}
```

## Contributing

We welcome contributions to Conflation. If you would like to report an issue, or even better fix an existing one, please
use the [Conflation issue tracker](https://github.com/OpenStreetMapSpeeds/conflation/issues) on GitHub.

### Tests

TODO

