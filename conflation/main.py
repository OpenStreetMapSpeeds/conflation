#!/usr/bin/env python3
import argparse
import json
import multiprocessing
import util

from conflation import mapillary, map_matching


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
        "--config",
        type=str,
        help='JSON of configurable settings for where / how to pull the GPS trace, e.g. {"provider":"mapillary","client_id":"xxx","sequences_per_page":50,"skip_if_fewer_images_than":5, "start_date":"2020-01-01"}',
        required=True,
    )
    arg_parser.add_argument(
        "--concurrency",
        type=int,
        help="The number of processes to use to make requests, by default your # of cpus",
        default=multiprocessing.cpu_count(),
    )
    arg_parser.add_argument(
        "--valhalla_url",
        type=str,
        help="Base URL for an active Valhalla service",
        required=True,
    )
    arg_parser.add_argument(
        "--valhalla_headers",
        type=str,
        help="Additional http headers to send with the requests. Follows the http header spec, eg. some-header-name: some-header-value",
        action="append",
        nargs="*",
    )

    # TODO: Change print() to use logger and add logging level as arg

    parsed_args = arg_parser.parse_args()

    # Create dirs
    bbox = parsed_args.bbox
    traces_dir, tmp_dir, map_matches_dir, results_dir = util.initialize_dirs(bbox)

    # Determine source of trace data specified by config
    try:
        config = json.loads(parsed_args.config)
    except json.decoder.JSONDecodeError:
        print("ERROR: Could not parse --config JSON={}".format(parsed_args.config))
        raise

    # Pull and filter trace data
    print("Pulling trace data from API...")
    if config["provider"] == "mapillary":
        mapillary.run(parsed_args.bbox, traces_dir, tmp_dir, config, parsed_args.concurrency)
    else:
        raise NotImplementedError(
            'Trace data source "{}" not supported. Currently supported: ["mapillary"]'.format(
                config["source"]
            )
        )

    # TODO: See if introducing a Queue / iterator here makes sense so we can continue next steps while pulling from API
    print("Trace data pulled, map matching...")
    # Pulling Valhalla headers from args
    valhalla_headers = {
        k: v for k, v in [h.split(": ") for hs in parsed_args.valhalla_headers for h in hs]
    }
    map_matching.run(
        traces_dir,
        map_matches_dir,
        parsed_args.concurrency,
        parsed_args.valhalla_url,
        valhalla_headers,
    )

    # Next step: directories grouped by country, files grouped by region, files will be .pickles of lists where
    # each row is a per-edge measurement
    print("Map matching complete, aggregating data into final .json output files...")
    # aggregation.run(
    #     map_matches_dir,
    #     results_dir,
    #     parsed_args.concurrency,
    # )

    print("Done!")


if __name__ == "__main__":
    main()
