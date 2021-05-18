#!/usr/bin/env python3
import argparse
import multiprocessing
import json
import mapillary

import util

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    # TODO: Make this optional and do the planet if so?
    arg_parser.add_argument('--bbox', type=str,
                            help='Filter by the bounding box on the map, given as `min_longitude,min_latitude,'
                                 'max_longitude,max_latitude`', required=True)
    arg_parser.add_argument('--traces-source', type=str,
                            help='JSON of configurable settings for where / how to pull the GPS trace, '
                                 'e.g. {\"provider\":\"mapillary\",\"client_id\":\"xxx\",\"sequences_per_page\":50,'
                                 '\"skip_if_fewer_images_than\":5, \"start_date\":\"2020-01-01\"}',
                            required=True)
    arg_parser.add_argument('--concurrency', type=int,
                            help='The number of processes to use to make requests, by default your # of cpus',
                            default=multiprocessing.cpu_count())
    # TODO: Change print() to use logger and add logging level as arg

    parsed_args = arg_parser.parse_args()

    # Create dirs
    bbox = parsed_args.bbox
    output_dir, output_tmp_dir = util.initialize_dirs(bbox)

    # Determine source of trace data specified by config
    try:
        traces_source = json.loads(parsed_args.traces_source)
    except json.decoder.JSONDecodeError:
        print('ERROR: Could not parse --traces-source JSON={}'.format(parsed_args.traces_source))
        raise

    # Pull and filter trace data
    print('Pulling trace data from API...')
    if traces_source['provider'] == 'mapillary':
        mapillary.run(parsed_args.bbox, output_dir, output_tmp_dir, traces_source, parsed_args.concurrency)
    else:
        raise NotImplementedError(
            'Trace data source "{}" not supported. Currently supported: ["mapillary"]'.format(traces_source['source']))

    # TODO: See if introducing a Queue / iterator here makes sense so we can continue next steps while pulling from API
    print('Trace data pulled, map matching...')
    # map_matching.run(output_dir, parsed_args.concurrency)

    print('Done!')
