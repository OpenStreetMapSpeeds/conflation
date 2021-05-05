#!/usr/bin/env python3
import argparse
import multiprocessing
import requests
import os
import time
from dateutil import parser
import pickle
import json
from typing import Callable

OUTPUT_DIR = 'output'
TEMP_DIR = 'tmp'
SECTIONS_PICKLE_FILENAME = 'sections.pickle'

SEQUENCES_PER_PAGE_DEFAULT = 50  # How many sequences to receive on each page of the API call
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = 5  # We will skip any sequences if they have fewer than this number of images
MAX_FILES_IN_DIR = 500  # Maximum number of files we will put in one directory
SEQUENCE_URL = 'https://a.mapillary.com/v3/sequences_without_images?client_id={}&bbox={}&per_page={}'
IMAGES_URL = 'https://a.mapillary.com/v3/images?client_id={}&sequence_keys={}'


def initialize_dirs(bbox: str) -> tuple[str, str]:
    """
    Creates all dirs needed for run if they don't exist
    :param bbox: bbox string from arg
    :return: tuple of (output dir name, tmp dir name for any tmp pickle files)
    """
    output_dir = os.path.join(
        os.getcwd(),
        OUTPUT_DIR,
        bbox
    )
    output_tmp_dir = os.path.join(
        os.getcwd(),
        OUTPUT_DIR,
        bbox,
        TEMP_DIR
    )
    # Make the output and tmp dirs if it does not exist yet
    if not os.path.exists(output_tmp_dir):
        os.makedirs(output_tmp_dir)  # Makes all dirs recursively, so we know output_dir will also now exist

    return output_dir, output_tmp_dir


def split_bbox(output_dir: str, bbox: str, to_bbox_str: Callable[[float, float, float, float], str],
               section_size: float = 0.05) -> list[str]:
    """
    Takes the given bbox and splits it up into smaller sections, with the smaller bbox chunks having long/lat sizes =
    section_size. Also writes the bbox sections to disk so we can pick up instructions from previous runs (may be
    removed)
    :param output_dir: output dir name
    :param bbox: bbox string from arg
    :param to_bbox_str: function that takes (min_long, min_lat, max_long, max_lat) bbox definition coordinates, and
    returns a string that we will feed into the next function. Should be the same format as the API source expects
    :param section_size: the smaller bbox sections will have max_long-min_long = max_lat-min_lat = section_size
    :return: list of bbox section strings, whose format will be dictated by the to_bbox_str function
    """
    sections_filename = os.path.join(output_dir, SECTIONS_PICKLE_FILENAME)

    try:
        print('Reading bbox_sections from disk...')
        bbox_sections = pickle.load(open(sections_filename, 'rb'))
    except (OSError, IOError):
        print('bbox_sections pickle not found. Creating and writing to disk...')
        min_long, min_lat, max_long, max_lat = [float(s) for s in bbox.split(',')]

        # Perform a check to see how many sections would be generated
        num_files = ((max_long - min_long) // section_size + 1) * ((max_lat - min_lat) // section_size + 1)
        if num_files > MAX_FILES_IN_DIR:
            # TODO: Check len of bbox_sections, if over some size limit, we split things up
            print('WARNING: {} bbox sections will be generated and a .pickle file will be created for all of them, '
                  'violating the MAX_FILES_IN_DIR={}'.format(num_files, MAX_FILES_IN_DIR))
        else:
            print('{} bbox sections will be generated...'.format(num_files))

        bbox_sections = []
        prev_long = min_long
        while prev_long < max_long:
            cur_long = min(prev_long + section_size, max_long)
            prev_lat = min_lat
            while prev_lat < max_lat:
                cur_lat = min(prev_lat + section_size, max_lat)
                bbox_sections.append(to_bbox_str(prev_long, prev_lat, cur_long, cur_lat))
                prev_lat += section_size
            prev_long += section_size

        pickle.dump(bbox_sections, open(sections_filename, 'wb'))

    return bbox_sections


def process_bbox_sections(output_dir: str, output_tmp_dir: str, session: requests.Session, map_client_id: str,
                          bbox_sections: list[str], conf: any) -> None:
    for bbox in bbox_sections:
        result_filename = os.path.join(output_dir, bbox + '.pickle')  # The file on disk where we will store trace data

        if os.path.exists(result_filename):  # FIXME
            print('Seq for bbox={} exists on disk, reading...')
            try:
                # TODO: Remove this, next step just needs the trace data on disk
                trace_data = pickle.load(open(os.path.join(output_dir, bbox + '.pickle'), 'rb'))
                print('\n\n##################\nPulled from disk seqs for bbox={}'.format(bbox))
                print('{}\n##################\n\n'.format(trace_data))
                continue
            except (OSError, IOError) as e:
                print('ERROR: bbox={} was marked as completed, but error while pulling data from disk: {}'.format(bbox,
                                                                                                                  e))

        # We haven't pulled API trace data for this bbox section yet
        trace_data = get_trace_data_for_bbox(session, map_client_id, bbox, conf)

        # Avoids potential partial write issues by writing to a temp file and then as a final operation, then renaming
        # to the real location
        temp_filename = os.path.join(output_tmp_dir, bbox + '.pickle')
        pickle.dump(trace_data, open(temp_filename, 'wb'))
        os.rename(temp_filename, result_filename)


def get_trace_data_for_bbox(session: requests.Session, map_client_id: str, bbox: str, conf: any) -> dict[str, list]:
    result, elapsed = {}, 0
    # try:
    start = time.time()
    # Check to see if user specified any overrides in conf JSON
    seq_per_page = conf['sequences_per_page'] if 'sequences_per_page' in conf else SEQUENCES_PER_PAGE_DEFAULT
    skip_if_fewer_imgs_than = conf[
        'skip_if_fewer_images_than'] if 'skip_if_fewer_images_than' in conf else SKIP_IF_FEWER_IMAGES_THAN_DEFAULT

    print('Getting seq for bbox={}'.format(bbox))
    next_url = SEQUENCE_URL.format(map_client_id, bbox, seq_per_page)
    page = 1
    while next_url:
        print('Page {}, url={}'.format(page, next_url))
        sequence_resp = session.get(next_url)
        sequence_keys = []
        for seq_f in sequence_resp.json()['features']:
            # Skip sequences that have too few images
            if len(seq_f['geometry']['coordinates']) >= skip_if_fewer_imgs_than:
                sequence_keys.append(seq_f['properties']['key'])
        # TODO: Paginate images
        images_resp = session.get(IMAGES_URL.format(map_client_id, ','.join(sequence_keys)))
        for img_f in images_resp.json()['features']:
            if img_f['properties']['sequence_key'] not in result:
                result[img_f['properties']['sequence_key']] = []
            result[img_f['properties']['sequence_key']].append((
                parser.isoparse(img_f['properties']['captured_at']).timestamp(),  # Epoch time
                img_f['geometry']['coordinates']
            ))

        # Check if there is a next page or if we are finished with this bbox
        next_url = sequence_resp.links['next']['url'] if 'next' in sequence_resp.links else None
        page += 1

    stop = time.time()
    elapsed = stop - start
    print('\n\n##################\nFinished processing seqs for bbox={}, elapsed time: {}'.format(bbox, elapsed))
    print('{}\n##################\n\n'.format(result))
    # with response_count.get_lock():
    #     response_count.value += 1
    # except Exception as e:
    #     print(e)

    return result


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    # TODO: Make this optional and do the planet if so
    arg_parser.add_argument('--bbox', type=str, help='Filter by the bounding box on the map, given as `min_longitude,'
                                                     'min_latitude,max_longitude,max_latitude`', required=True)
    arg_parser.add_argument('--conf', type=str,
                            help='JSON of configurable settings for this script, e.g. {\"source\":\"mapillary\",\"mcid\":\"xxx\",\"sequences_per_page\":50,\"skip_if_fewer_images_than\":5}',
                            required=True)
    # arg_parser.add_argument('--concurrency', type=int,
    #                         help='The number of processes to use to make requests, by default '
    #                              'your # of cpus',
    #                         default=multiprocessing.cpu_count())
    # arg_parser.add_argument('--output-dir', type=str, help='Optional custom name for the directory in which to place '
    #                                                        'the result of each request (default is the bbox string)')
    # TODO: Change print() to use logger and add logging level as arg

    parsed_args = arg_parser.parse_args()

    # Determine source of trace data specified by config
    try:
        conf = json.loads(parsed_args.conf)
    except json.decoder.JSONDecodeError:
        print('ERROR: Could not parse --conf JSON={}'.format(parsed_args.conf))
        raise

    if conf['source'] == 'mapillary':
        # TODO: Actually split up logic btwn source-generic and Mapillary

        if 'mcid' not in conf:
            raise KeyError('Missing "mcid" (Mapillary Client ID) key  in --conf JSON.')
        mcid = conf['mcid']

        # Create dirs
        output_dir, output_tmp_dir = initialize_dirs(parsed_args.bbox)


        def mapillary_to_bbox(llo, lla, mlo, mla):
            return ','.join([str(llo), str(lla), str(mlo), str(mla)])


        # Break the bbox into sections and save it to a pickle file
        bbox_sections = split_bbox(output_dir, parsed_args.bbox, mapillary_to_bbox)

        req_session = requests.Session()
        process_bbox_sections(output_dir, output_tmp_dir, req_session, mcid, bbox_sections, conf)

        print('Finished successfully!')
    else:
        raise NotImplementedError(
            'Trace data source "{}" not supported. Currently supported: ["mapillary"]'.format(conf['source']))
