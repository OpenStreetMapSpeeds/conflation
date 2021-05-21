import datetime
import requests
import os
import pickle
import multiprocessing
from dateutil import parser
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import util
import trace_filter

SEQUENCES_PER_PAGE_DEFAULT = 10  # How many sequences to receive on each page of the API call
IMAGES_PER_PAGE_DEFAULT = 1000  # How many sequences to receive on each page of the API call
# We only look for sequences beyond the start date, by default a year ago
SEQUENCE_START_DATE_DEFAULT = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = 10  # We will skip any sequences if they have fewer than this number of images
SEQUENCE_URL = 'https://a.mapillary.com/v3/sequences_without_images?client_id={}&bbox={}&per_page={}&start_date={}'
IMAGES_URL = 'https://a.mapillary.com/v3/images?client_id={}&sequence_keys={}&per_page={}'


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


def is_within_bbox(lon: float, lat: float, bbox: list[float]) -> bool:
    """
    Checks if lon / lat coordinate is within a bbox in the format of [min_lon, min_lat, max_lon, max_lat]
    """
    return bbox[0] <= lon < bbox[2] and bbox[1] <= lat < bbox[3]


def initialize_multiprocess(global_output_dir_: str, global_output_tmp_dir_: str, global_traces_source_: any,
                            finished_bbox_sections_: multiprocessing.Value) -> None:
    """
    Initializes global variables referenced / updated by all threads of the multiprocess API requests.
    """
    # For persistent connections and timeout settings
    global session
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=3
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    # So each process knows the output / tmp dirs
    global global_output_dir
    global_output_dir = global_output_dir_
    global global_output_tmp_dir
    global_output_tmp_dir = global_output_tmp_dir_

    # So each process knows the conf provided
    global global_traces_source
    global_traces_source = global_traces_source_

    # Integer counter of num of finished bbox_sections
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
        print('Before filter: lens: {}'.format([len(t) for t in trace_data]))

        # Perform some simple filters to weed out bad trace data
        trace_data = trace_filter.run(trace_data)
        print('After filter: lens: {}'.format([len(t) for t in trace_data]))

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
    bbox_as_list = [float(d) for d in bbox.split(',')]

    # We will use this dict to group trace points by sequence ID
    sequences_by_id = {}

    map_client_id = conf['client_id']  # The Mapillary client ID, mandatory key of conf

    # Check to see if user specified any overrides in conf JSON
    seq_per_page = conf['sequences_per_page'] if 'sequences_per_page' in conf else SEQUENCES_PER_PAGE_DEFAULT
    img_per_page = conf['images_per_page'] if 'images_per_page' in conf else IMAGES_PER_PAGE_DEFAULT
    skip_if_fewer_imgs_than = conf[
        'skip_if_fewer_images_than'] if 'skip_if_fewer_images_than' in conf else SKIP_IF_FEWER_IMAGES_THAN_DEFAULT
    start_date = conf['start_date'] if 'start_date' in conf else SEQUENCE_START_DATE_DEFAULT

    # Paginate sequences within this bbox
    print('@ MAPILLARY: Getting seq for bbox={}'.format(bbox))
    seq_next_url = SEQUENCE_URL.format(map_client_id, bbox, seq_per_page, start_date)
    seq_page = 1
    while seq_next_url:
        print('@@ MAPILLARY: Seq Page {}, url={}'.format(seq_page, seq_next_url))
        seq_resp = session_.get(seq_next_url, timeout=10)
        seq_ids = []
        for seq_f in seq_resp.json()['features']:
            seq_id = seq_f['properties']['key']

            # Only process sequences that originated from this bbox. This prevents us from processing sequences twice
            origin_lon, origin_lat = seq_f['geometry']['coordinates'][0]
            if not is_within_bbox(origin_lon, origin_lat, bbox_as_list):
                print('@@@ MAPILLARY: Skipping seq b/c origin ({}, {}) not in bbox {}'.format(origin_lon, origin_lat,
                                                                                              bbox))
                continue

            # Skip sequences that have too few images
            if len(seq_f['geometry']['coordinates']) < skip_if_fewer_imgs_than:
                continue

            seq_ids.append(seq_id)

        if len(seq_ids) > 0:
            # Paginate images within these sequences
            img_next_url = IMAGES_URL.format(map_client_id, ','.join(seq_ids), img_per_page)
            img_page = 1
            while img_next_url:
                print('@@@ MAPILLARY: Image Page {}, url={}'.format(img_page, img_next_url))
                img_resp = session_.get(img_next_url, timeout=10)
                for img_f in img_resp.json()['features']:
                    if img_f['properties']['sequence_key'] not in sequences_by_id:
                        sequences_by_id[img_f['properties']['sequence_key']] = []
                    sequences_by_id[img_f['properties']['sequence_key']].append({
                        'time': parser.isoparse(img_f['properties']['captured_at']).timestamp(),  # Epoch time
                        'lon': img_f['geometry']['coordinates'][0],
                        'lat': img_f['geometry']['coordinates'][1]
                    })

                # Check if there is a next image page or if we are finished with this sequence
                img_next_url = img_resp.links['next']['url'] if 'next' in img_resp.links else None
                img_page += 1

        # Check if there is a next sequence page or if we are finished with this bbox
        seq_next_url = seq_resp.links['next']['url'] if 'next' in seq_resp.links else None
        seq_page += 1

    print('Keys: {}'.format(list(sequences_by_id.keys())))

    # We don't care about the sequence IDs anymore (just using it as a method to group trace data), so we just return
    # values
    sequences = list(sequences_by_id.values())

    # Mapillary returns their trace data in reverse chronological order (latest image first), so we reverse that
    # back to get the order the images were taken, which is what map matching needs
    [s.reverse() for s in sequences]

    return sequences
