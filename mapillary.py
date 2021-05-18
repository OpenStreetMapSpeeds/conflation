import datetime
import requests
import os
import pickle
import multiprocessing
from dateutil import parser

import util
import trace_filter

SEQUENCES_PER_PAGE_DEFAULT = 50  # How many sequences to receive on each page of the API call
# We only look for sequences beyond the start date, by default a year ago
SEQUENCE_START_DATE_DEFAULT = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = 10  # We will skip any sequences if they have fewer than this number of images
SEQUENCE_URL = 'https://a.mapillary.com/v3/sequences_without_images?client_id={}&bbox={}&per_page={}&start_date={}'
IMAGES_URL = 'https://a.mapillary.com/v3/images?client_id={}&sequence_keys={}'


def run(bbox: str, output_dir: str, output_tmp_dir: str, traces_source: dict, processes: int) -> None:
    """
    Entrypoint for pulling trace date from Mapillary APIs. Will pull all trace data in the given bbox and store it in
    the output_dir, using the number of processes specified and any conf values from traces_source.

    :param bbox: Bounding box we are searching over, in the format of 'min_lon,min_lat,max_lon,max_lat'
    :param output_dir: Dir where trace data will be pickled to
    :param output_tmp_dir: Dir where temp output files will be stored (should be empty upon completion)
    :param traces_source: Dict of configs, see the conf param of make_trace_data_requests()
    :param processes: Number of threads to use
    """
    # Do a quick check to see if user specified the mandatory 'client_id' in traces_source JSON
    if 'client_id' not in traces_source:
        raise KeyError('Missing "client_id" (Mapillary Client ID) key in --traces-source JSON.')

    # Break the bbox into sections and save it to a pickle file
    bbox_sections = util.split_bbox(output_dir, bbox, to_bbox)

    finished_bbox_sections = multiprocessing.Value('i', 0)
    with multiprocessing.Pool(initializer=initialize_multiprocess,
                              initargs=(output_dir, output_tmp_dir, traces_source, finished_bbox_sections),
                              processes=processes) as pool:
        result = pool.map_async(pull_filter_and_save_trace_for_bbox, bbox_sections)

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


def to_bbox(llo: float, lla: float, mlo: float, mla: float) -> str:
    """
    Given (min_lon, min_lat, max_lon, max_lat) bounding box values, returns a string representation understood by
    Mapillary APIs.
    """
    return ','.join([str(llo), str(lla), str(mlo), str(mla)])


def initialize_multiprocess(global_output_dir_: str, global_output_tmp_dir_: str, global_traces_source_: any,
                            finished_bbox_sections_: multiprocessing.Value) -> None:
    """
    Initializes global variables referenced / updated by all threads of the multiprocess API requests.
    """
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


def pull_filter_and_save_trace_for_bbox(bbox_section: tuple[str, str]) -> None:
    """
    Checks to see if a bbox section already has trace data pulled onto disk. If not, pulls it from Mapillary by calling
    make_trace_data_requests(), filters it using trace_filer.run(), and saves it to disk. Writes to a temp file first
    to avoid issues if script crashes during the pickle dump. Meant to be run in a multi-threaded manner and references
    global vars made by initialize_multiprocess().

    :param bbox_section: Tuple of (str representation of bbox to feed into Mapillary API, filename where filtered result
        should be stored)
    """
    try:
        bbox, result_filename = bbox_section

        if os.path.exists(result_filename):
            print('Seq already exists on disk for bbox={}. Skipping...'.format(bbox))
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        # We haven't pulled API trace data for this bbox section yet
        trace_data = make_trace_data_requests(session, bbox, global_traces_source)

        # Perform some simple filters to weed out bad trace data
        trace_data = trace_filter.run(trace_data)
        print(trace_data)

        # Avoids potential partial write issues by writing to a temp file and then as a final operation, then renaming
        # to the real location
        temp_filename = os.path.join(global_output_tmp_dir, bbox + '.pickle')
        pickle.dump(trace_data, open(temp_filename, 'wb'))
        os.rename(temp_filename, result_filename)

        with finished_bbox_sections.get_lock():
            finished_bbox_sections.value += 1
    except Exception as e:
        print('ERROR: Failed to pull trace data: {}'.format(repr(e)))


def make_trace_data_requests(session_: requests.Session, bbox: str, conf: any) -> list[list[dict]]:
    """
    Makes the actual calls to Mapillary API to pull trace data for a given bbox string.

    :param session_: requests.Session() to persist session across API calls
    :param bbox: String representation of bbox that Mapillary API understands, i.e. 'min_lon,min_lat,max_lon,max_lat'
    :param conf: Dict of configs. Mandatory keys are ['client_id']. Optional keys are ['sequences_per_page',
        'skip_if_fewer_images_than', 'start_date']
    :return: List of trace data sequences. Trace data is in format understood by Valhalla map matching process, i.e. it
        has 'lon', 'lat', 'time', and optionally 'radius' keys
    """

    # We will use this dict to group trace points by sequence ID
    sequences_by_id = {}

    map_client_id = conf['client_id']  # The Mapillary client ID, mandatory key of conf

    # Check to see if user specified any overrides in conf JSON
    seq_per_page = conf['sequences_per_page'] if 'sequences_per_page' in conf else SEQUENCES_PER_PAGE_DEFAULT
    skip_if_fewer_imgs_than = conf[
        'skip_if_fewer_images_than'] if 'skip_if_fewer_images_than' in conf else SKIP_IF_FEWER_IMAGES_THAN_DEFAULT
    start_date = conf['start_date'] if 'start_date' in conf else SEQUENCE_START_DATE_DEFAULT

    print('@ MAPILLARY: Getting seq for bbox={}'.format(bbox))
    next_url = SEQUENCE_URL.format(map_client_id, bbox, seq_per_page, start_date)
    page = 1
    while next_url:
        print('@ MAPILLARY: Page {}, url={}'.format(page, next_url))
        sequence_resp = session_.get(next_url)
        sequence_keys = []
        for seq_f in sequence_resp.json()['features']:
            # Skip sequences that have too few images
            if len(seq_f['geometry']['coordinates']) >= skip_if_fewer_imgs_than:
                sequence_keys.append(seq_f['properties']['key'])
        # TODO: Paginate images. Keep in mind we need to reverse the Mapillary trace sequences
        images_resp = session_.get(IMAGES_URL.format(map_client_id, ','.join(sequence_keys)))
        # Mapillary returns their trace data in reverse chronological order (latest image first), so we reverse that
        # back to get the order the images were taken, which is what map matching needs
        for img_f in reversed(images_resp.json()['features']):
            if img_f['properties']['sequence_key'] not in sequences_by_id:
                sequences_by_id[img_f['properties']['sequence_key']] = []
            sequences_by_id[img_f['properties']['sequence_key']].append({
                'time': parser.isoparse(img_f['properties']['captured_at']).timestamp(),  # Epoch time
                'lon': img_f['geometry']['coordinates'][0],
                'lat': img_f['geometry']['coordinates'][1]
            })

        # Check if there is a next page or if we are finished with this bbox
        next_url = sequence_resp.links['next']['url'] if 'next' in sequence_resp.links else None
        page += 1

    # We don't care about the sequence IDs anymore (just using it as a method to group trace data), so we just return
    # values
    return list(sequences_by_id.values())
