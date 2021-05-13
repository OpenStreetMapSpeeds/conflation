import time
import requests
import os
import pickle
import multiprocessing
from dateutil import parser

import util

SEQUENCES_PER_PAGE_DEFAULT = 50  # How many sequences to receive on each page of the API call
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = 10  # We will skip any sequences if they have fewer than this number of images
# TODO: Make use of the start_date parameter
SEQUENCE_URL = 'https://a.mapillary.com/v3/sequences_without_images?client_id={}&bbox={}&per_page={}'
IMAGES_URL = 'https://a.mapillary.com/v3/images?client_id={}&sequence_keys={}'


def run(bbox: str, output_dir: str, output_tmp_dir: str, traces_source: any, processes: int):
    # Do a quick check to see if user specified the mandatory 'client_id' in traces_source JSON
    if 'client_id' not in traces_source:
        raise KeyError('Missing "client_id" (Mapillary Client ID) key in --traces-source JSON.')

    # Break the bbox into sections and save it to a pickle file
    bbox_sections = util.split_bbox(output_dir, bbox, to_bbox)

    finished_bbox_sections = multiprocessing.Value('i', 0)
    with multiprocessing.Pool(initializer=initialize_multiprocess,
                              initargs=(output_dir, output_tmp_dir, traces_source, finished_bbox_sections),
                              processes=processes) as pool:
        result = pool.map_async(pull_and_save_trace_for_bbox, bbox_sections)

        print('Placing {} results in {}...'.format(len(bbox_sections), output_dir))
        progress = 0
        increment = 5
        while not result.ready():
            result.wait(timeout=5)
            next_progress = int(finished_bbox_sections.value / len(bbox_sections) * 100)
            if int(next_progress / increment) > progress:
                print('Current progress: {}%'.format(next_progress))
                progress = int(next_progress / increment)
        if progress != 100 / increment:
            print('Current progress: 100%')

        # TODO: Delete the tmp dir after run?


def to_bbox(llo, lla, mlo, mla):
    return ','.join([str(llo), str(lla), str(mlo), str(mla)])


def initialize_multiprocess(global_output_dir_: str, global_output_tmp_dir_: str, global_traces_source_: any,
                            finished_bbox_sections_: multiprocessing.Value):
    # For persistent connections
    global session
    session = requests.Session()

    # So each process knows the output / tmp dirs
    global global_output_dir
    global_output_dir = global_output_dir_
    global global_output_tmp_dir
    global_output_tmp_dir = global_output_tmp_dir_

    # So each process knows the conf provided
    global global_traces_source
    global_traces_source = global_traces_source_

    global finished_bbox_sections
    finished_bbox_sections = finished_bbox_sections_


def pull_and_save_trace_for_bbox(bbox: str) -> None:
    try:
        # The file on disk where we will store trace data
        result_filename = os.path.join(global_output_dir, bbox + '.pickle')

        if os.path.exists(result_filename):
            print('Seq already exists on disk for bbox={}. Skipping...'.format(bbox))
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        # We haven't pulled API trace data for this bbox section yet
        trace_data = make_trace_data_requests(session, bbox, global_traces_source)

        # Avoids potential partial write issues by writing to a temp file and then as a final operation, then renaming
        # to the real location
        temp_filename = os.path.join(global_output_tmp_dir, bbox + '.pickle')
        pickle.dump(trace_data, open(temp_filename, 'wb'))
        os.rename(temp_filename, result_filename)

        with finished_bbox_sections.get_lock():
            finished_bbox_sections.value += 1
    except Exception as e:
        print('ERROR: Failed to pull trace data: {}'.format(repr(e)))


def make_trace_data_requests(session_: requests.Session, bbox: str, conf: any) -> dict[str, list]:
    result, elapsed = {}, 0
    start = time.time()

    map_client_id = conf['client_id']  # The Mapillary client ID, mandatory key of conf

    # Check to see if user specified any overrides in conf JSON
    seq_per_page = conf['sequences_per_page'] if 'sequences_per_page' in conf else SEQUENCES_PER_PAGE_DEFAULT
    skip_if_fewer_imgs_than = conf[
        'skip_if_fewer_images_than'] if 'skip_if_fewer_images_than' in conf else SKIP_IF_FEWER_IMAGES_THAN_DEFAULT

    print('Getting seq for bbox={}'.format(bbox))
    next_url = SEQUENCE_URL.format(map_client_id, bbox, seq_per_page)
    page = 1
    while next_url:
        print('Page {}, url={}'.format(page, next_url))
        sequence_resp = session_.get(next_url)
        sequence_keys = []
        for seq_f in sequence_resp.json()['features']:
            # Skip sequences that have too few images
            if len(seq_f['geometry']['coordinates']) >= skip_if_fewer_imgs_than:
                sequence_keys.append(seq_f['properties']['key'])
        # TODO: Paginate images
        images_resp = session_.get(IMAGES_URL.format(map_client_id, ','.join(sequence_keys)))
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

    return result
