#!/usr/bin/env python3
import argparse
import json
import multiprocessing

from conflation import aggregation, util
from conflation.map_matching import valhalla
from conflation.trace_fetching import mapillary, mapillary_v3


def main():
    arg_parser = argparse.ArgumentParser()
    # TODO: Make this optional and do the planet if so?
    arg_parser.add_argument(
        "--bbox",
        type=str,
        help="Filter by the bounding box on the map, given as `min_longitude,min_latitude,max_longitude,max_latitude`",
        required=True,
    )
    arg_parser.add_argument(
        "--trace-config",
        type=str,
        help='JSON of configurable settings for where / how to pull the GPS trace. See .README for specific fields. E.g. {"provider":"mapillary","client_id":"xxx","sequences_per_page":50,"skip_if_fewer_images_than":5, "start_date":"2020-01-01"}',
        required=True,
    )
    arg_parser.add_argument(
        "--map-matching-config",
        type=str,
        help='JSON of configurable settings for where / how to perform map matching. See .README for specific fields. E.g. {"provider":"valhalla","base_url":"https://www.my-valhalla.com/","headers":{"some-header-name":"some-header-value"}}',
        required=True,
    )
    arg_parser.add_argument(
        "--concurrency",
        type=int,
        help="The number of processes to use to make requests, by default your # of cpus",
        default=multiprocessing.cpu_count(),
    )

    # TODO: Change print() to use logger and add logging level as arg

    parsed_args = arg_parser.parse_args()

    # Create dirs
    bbox = parsed_args.bbox
    traces_dir, tmp_dir, map_matches_dir, results_dir = util.initialize_dirs(bbox)

    print("Pulling trace data from API...")
    # Determine source of trace data specified by config
    try:
        trace_config = json.loads(parsed_args.trace_config)
    except json.decoder.JSONDecodeError:
        print("ERROR: Could not parse --trace-config JSON={}".format(parsed_args.trace_config))
        raise

    # Pull and filter trace data
    if trace_config["provider"] == "mapillary":
        mapillary.run(
            parsed_args.bbox, traces_dir, tmp_dir, trace_config, parsed_args.concurrency
        )
    if trace_config["provider"] == "mapillary_v3":
        mapillary_v3.run(
            parsed_args.bbox, traces_dir, tmp_dir, trace_config, parsed_args.concurrency
        )
    else:
        raise NotImplementedError(
            'Trace data source "{}" not supported. Currently supported: ["mapillary", "mapillary_v3"]'.format(
                trace_config["provider"]
            )
        )

    print("Trace data pulled, map matching...")
    # Determine source of map matching specified by config
    try:
        map_matching_config = json.loads(parsed_args.map_matching_config)
    except json.decoder.JSONDecodeError:
        print(
            "ERROR: Could not parse --map-matching-config JSON={}".format(
                parsed_args.map_matching_config
            )
        )
        raise

    if map_matching_config["provider"] == "valhalla":
        valhalla.run(traces_dir, map_matches_dir, parsed_args.concurrency, map_matching_config)
    else:
        raise NotImplementedError(
            'Map matching source "{}" not supported. Currently supported: ["valhalla"]'.format(
                map_matching_config["provider"]
            )
        )

    # Next step: directories grouped by country, files grouped by region, files will be .pickles of lists where
    # each row is a per-edge measurement
    print("Map matching complete, aggregating data into final .json output files...")
    aggregation.run(map_matches_dir, results_dir)

    print("Done!")


if __name__ == "__main__":
    main()
