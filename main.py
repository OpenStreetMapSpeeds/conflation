#!/usr/bin/env python3
import argparse
import json
import logging
import multiprocessing
import shutil
import time

from conflation import aggregation, util
from conflation.map_matching import valhalla
from conflation.trace_fetching import mapillary, mapillary_z5, mapillary_v3, auth_server


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
        help="JSON of configurable settings for where / how to pull the GPS trace. See .README for specific fields.",
        required=True,
    )
    arg_parser.add_argument(
        "--map-matching-config",
        type=str,
        help="JSON of configurable settings for where / how to perform map matching. See .README for specific fields.",
        required=True,
    )
    arg_parser.add_argument(
        "--concurrency",
        type=int,
        help="The number of processes to use to make requests, by default your # of cpus",
        default=multiprocessing.cpu_count(),
    )
    arg_parser.add_argument(
        "--logging",
        type=str,
        help='The logging level from ["debug", "info", "warning", "error", "critical"], by default "info"',
        default="info",
    )

    # Record start time for tracking runtimes
    start = time.time()

    parsed_args = arg_parser.parse_args()

    # Create dirs
    bbox = parsed_args.bbox
    traces_dir, tmp_dir, map_matches_dir, results_dir, log_filename = util.initialize_dirs(
        bbox
    )

    # Set up logging (we do this after creating dirs so we can put the logs in a file under the dir we created)
    logging.basicConfig(
        level=getattr(logging, parsed_args.logging.upper(), None),
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
        handlers=[logging.FileHandler(log_filename, mode="w"), logging.StreamHandler()],
    )

    logging.info("Pulling trace data from API...")
    # Determine source of trace data specified by config
    try:
        trace_config = json.loads(parsed_args.trace_config)
    except json.decoder.JSONDecodeError:
        logging.critical(
            "Could not parse --trace-config JSON={}".format(parsed_args.trace_config)
        )
        raise

    # Pull and filter trace data
    if trace_config["provider"] == "mapillary":
        # Do a quick check to see if user specified the mandatory 'client_id' and 'client_secret' in config JSON
        if "client_id" not in trace_config:
            raise KeyError(
                'Missing "client_id" (Mapillary Client ID) key in --trace-config JSON.'
            )
        if "client_secret" not in trace_config:
            raise KeyError(
                'Missing "client_secret" (Mapillary Client ID) key in --trace-config JSON.'
            )
        access_token = auth_server.run(
            trace_config["client_id"], trace_config["client_secret"]
        )

        # Puts a small delay here to address a problem with Mapillary not registering the access_token immediately after
        # distributing it
        time.sleep(2)

        mapillary.run(
            parsed_args.bbox,
            traces_dir,
            tmp_dir,
            trace_config,
            parsed_args.concurrency,
            access_token,
        )
    elif trace_config["provider"] == "mapillary_z5":
        # Do a quick check to see if user specified the mandatory 'client_id' and 'client_secret' in config JSON
        if "client_id" not in trace_config:
            raise KeyError(
                'Missing "client_id" (Mapillary Client ID) key in --trace-config JSON.'
            )
        if "client_secret" not in trace_config:
            raise KeyError(
                'Missing "client_secret" (Mapillary Client ID) key in --trace-config JSON.'
            )
        access_token = auth_server.run(
            trace_config["client_id"], trace_config["client_secret"]
        )

        # Puts a small delay here to address a problem with Mapillary not registering the access_token immediately after
        # distributing it
        time.sleep(2)

        mapillary_z5.run(
            eval(parsed_args.bbox),
            traces_dir,
            tmp_dir,
            trace_config,
            parsed_args.concurrency,
            access_token,
        )
    elif trace_config["provider"] == "mapillary_v3":
        mapillary_v3.run(
            parsed_args.bbox, traces_dir, tmp_dir, trace_config, parsed_args.concurrency
        )
    else:
        raise NotImplementedError(
            'Trace data source "{}" not supported. Currently supported: ["mapillary", "mapillary_v3"]'.format(
                trace_config["provider"]
            )
        )

    logging.info("Trace data pulled, map matching...")
    # Determine source of map matching specified by config
    try:
        map_matching_config = json.loads(parsed_args.map_matching_config)
    except json.decoder.JSONDecodeError:
        logging.critical(
            "Could not parse --map-matching-config JSON={}".format(
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

    logging.info("Map matching complete, aggregating data into final .json output files...")
    aggregation.run(map_matches_dir, results_dir)

    # Delete the tmp dir since we are finished with the run
    shutil.rmtree(tmp_dir)

    # Print out the time elapsed for this entire run
    end = time.time()
    logging.info("Script finished run in {} seconds.".format(round(end - start, 4)))


if __name__ == "__main__":
    main()
