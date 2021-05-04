#!/usr/bin/env python3
import argparse
import multiprocessing
import requests
import os
import time
from dateutil import parser
import pickle

OUTPUT_DIR = 'output'
SECTIONS_PICKLE_FILENAME = 'sections.pickle'

SEQUENCES_PER_PAGE = 50  # How many sequences to receive on each page of the API call
SKIP_IF_FEWER_IMAGES_THAN = 5  # We will skip any sequences if they have fewer than this number of images
SEQUENCE_URL = 'https://a.mapillary.com/v3/sequences_without_images?client_id={}&bbox={}&per_page={}'
IMAGES_URL = 'https://a.mapillary.com/v3/images?client_id={}&sequence_keys={}'


def get_sections_pkl_filename(output_dir: str):
    return os.path.join(output_dir, SECTIONS_PICKLE_FILENAME)


def split_bbox(output_dir: str, bbox: str, section_size: float = 0.05) -> dict[str, bool]:
    to_bbox = lambda llo, lla, mlo, mla: ','.join([str(llo), str(lla), str(mlo), str(mla)])
    sections_filename = get_sections_pkl_filename(output_dir)

    try:
        print('Reading bbox_sections from disk...')
        bbox_sections = pickle.load(open(sections_filename, 'rb'))
    except (OSError, IOError) as e:
        print('bbox_sections pickle not found. Creating and writing to disk...')
        bbox_sections = {}
        min_long, min_lat, max_long, max_lat = [float(s) for s in bbox.split(',')]
        prev_long = min_long
        while prev_long < max_long:
            cur_long = min(prev_long + section_size, max_long)
            prev_lat = min_lat
            while prev_lat < max_lat:
                cur_lat = min(prev_lat + section_size, max_lat)
                bbox_sections[to_bbox(prev_long, prev_lat, cur_long, cur_lat)] = False
                prev_lat += section_size
            prev_long += section_size

        pickle.dump(bbox_sections, open(sections_filename, 'wb'))

    return bbox_sections


def process_bbox_sections(output_dir: str, session: requests.Session, map_client_id: str,
                          bbox_sections: dict[str, bool]):
    sections_filename = get_sections_pkl_filename(output_dir)

    for bbox, completed in bbox_sections.items():
        if completed:
            print('Seq for bbox={} exists on disk, reading...')
            try:
                trace_data = pickle.load(open(os.path.join(output_dir, bbox + '.pickle'), 'rb'))
                print('\n\n##################\nPulled from disk seqs for bbox={}'.format(bbox))
                print('{}\n##################\n\n'.format(trace_data))
                continue
            except (OSError, IOError) as e:
                print('ERROR: bbox={} was marked as completed, but error while pulling data from disk: {}'.format(bbox,
                                                                                                                  e))

        trace_data, _ = get_trace_data_for_bbox(session, map_client_id, bbox)

        pickle.dump(trace_data, open(os.path.join(output_dir, bbox + '.pickle'), 'wb'))

        bbox_sections[bbox] = True
        pickle.dump(bbox_sections, open(sections_filename, 'wb'))

    return 'Done'


def get_trace_data_for_bbox(session: requests.Session, map_client_id: str, bbox: str):
    result, elapsed = {}, 0
    # try:
    start = time.time()

    print('Getting seq for bbox={}'.format(bbox))
    next_url = SEQUENCE_URL.format(map_client_id, bbox, SEQUENCES_PER_PAGE)
    page = 1
    while next_url:
        print('Page {}, url={}'.format(page, next_url))
        sequence_resp = session.get(next_url)
        sequence_keys = []
        for seq_f in sequence_resp.json()['features']:
            # Skip sequences that have too few images
            if len(seq_f['geometry']['coordinates']) >= SKIP_IF_FEWER_IMAGES_THAN:
                sequence_keys.append(seq_f['properties']['key'])
        images_resp = session.get(IMAGES_URL.format(map_client_id, ','.join(sequence_keys)))
        for img_f in images_resp.json()['features']:
            if img_f['properties']['sequence_key'] not in result:
                result[img_f['properties']['sequence_key']] = []
            result[img_f['properties']['sequence_key']].append((
                parser.isoparse(img_f['properties']['captured_at']).timestamp(),
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

    return result, elapsed


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    # TODO: Make this optional and do the planet if so
    arg_parser.add_argument('--bbox', type=str, help='Filter by the bounding box on the map, given as `min_longitude,'
                                                     'min_latitude,max_longitude,max_latitude`', required=True)
    arg_parser.add_argument('--mcid', type=str, help='Mapillary Client ID; obtained by registering an app at '
                                                     'https://www.mapillary.com/dashboard/developers', required=True)
    arg_parser.add_argument('--concurrency', type=int,
                            help='The number of processes to use to make requests, by default '
                                 'your # of cpus',
                            default=multiprocessing.cpu_count())
    arg_parser.add_argument('--output-dir', type=str, help='Optional custom name for the directory in which to place '
                                                           'the result of each request (default is the bbox string)')
    # TODO: Change print() to use logger and add logging level as arg
    parsed_args = arg_parser.parse_args()

    output_dir = os.path.join(
        os.getcwd(),
        OUTPUT_DIR,
        parsed_args.output_dir if parsed_args.output_dir is not None else parsed_args.bbox
    )
    # Make the output directory if it does not exist yet
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Break the bbox into sections and save it to a pickle file
    bbox_sections = split_bbox(output_dir, parsed_args.bbox)

    req_session = requests.Session()
    process_bbox_sections(output_dir, req_session, parsed_args.mcid, bbox_sections)
