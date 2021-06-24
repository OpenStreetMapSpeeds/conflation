import datetime
import math
import multiprocessing
import os
import pickle
import requests
from dateutil import parser
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from conflation import util, trace_filter

SEQUENCES_PER_PAGE_DEFAULT = 25  # How many sequences to receive on each page of the API call
IMAGES_PER_PAGE_DEFAULT = 1000  # How many images to receive on each page of the API call
MAX_SEQUENCES_PER_BBOX_SECTION_DEFAULT = (  # How many sequences to process for each bbox section
    500
)
SEQUENCE_START_DATE_DEFAULT = (  # By default we only consider sequences up to a year old
    datetime.date.today() - datetime.timedelta(days=365)
).isoformat()
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = (
    10  # We will skip any sequences if they have fewer than this number of images
)

TILES_URL = (
    "https://tiles.mapillary.com/maps/vtp/mly_map_feature_point/2/{}/{}/{}?access_token={}"
)

SEQUENCE_URL = "https://a.mapillary.com/v3/sequences_without_images?client_id={}&bbox={}&per_page={}&start_date={}"
IMAGES_URL = "https://a.mapillary.com/v3/images?client_id={}&sequence_keys={}&per_page={}"
MAX_FILES_IN_DIR = 500  # Maximum number of files we will put in one directory


def run(
    bbox: str, traces_dir: str, tmp_dir: str, config: dict, processes: int, access_token: str
) -> int:
    """
    Entrypoint for pulling trace date from Mapillary APIs. Will pull all trace data in the given bbox and store it in
    the traces_dir, using the number of processes specified and any conf values from the `config` JSON.

    :param bbox: Bounding box we are searching over, in the format of 'min_lon,min_lat,max_lon,max_lat'
    :param traces_dir: Dir where trace data will be pickled to
    :param tmp_dir: Dir where temp output files will be stored (should be empty upon completion)
    :param config: Dict of configs, see the .README or the conf param of make_trace_data_requests()
    :param processes: Number of threads to use
    :param access_token: Mapillary access token for API calls
    """
    # Break the bbox into sections and save it to a pickle file
    bbox_sections = split_bbox(traces_dir, bbox)

    finished_bbox_sections = multiprocessing.Value("i", 0)
    with multiprocessing.Pool(
        initializer=initialize_multiprocess,
        initargs=(tmp_dir, config, finished_bbox_sections, access_token),
        processes=processes,
    ) as pool:
        result = pool.map_async(pull_filter_and_save_trace_for_bbox, bbox_sections)

        print("Placing {} results in {}...".format(len(bbox_sections), traces_dir))
        progress = 0
        increment = 5
        while not result.ready():
            result.wait(timeout=5)
            next_progress = int(finished_bbox_sections.value / len(bbox_sections) * 100)
            if int(next_progress / increment) > progress:
                print("Current progress: {}%".format(next_progress))
                progress = int(next_progress / increment)
        if progress != 100 / increment:
            print("Current progress: 100%")

        # TODO: Delete the tmp dir after run?
        return 1


def is_within_bbox(lon: float, lat: float, bbox: list[float]) -> bool:
    """
    Checks if lon / lat coordinate is within a bbox in the format of [min_lon, min_lat, max_lon, max_lat]
    """
    return bbox[0] <= lon < bbox[2] and bbox[1] <= lat < bbox[3]


def initialize_multiprocess(
    global_tmp_dir_: str,
    global_config_: dict,
    finished_bbox_sections_: multiprocessing.Value,
    access_token_: str,
) -> None:
    """
    Initializes global variables referenced / updated by all threads of the multiprocess API requests.
    """
    # For persistent connections and timeout settings
    global session
    session = requests.Session()
    retry_strategy = Retry(
        total=5, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=3
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # So each process knows the output / tmp dirs
    global global_tmp_dir
    global_tmp_dir = global_tmp_dir_

    # So each process knows the conf provided
    global global_config
    global_config = global_config_

    # Integer counter of num of finished bbox_sections
    global finished_bbox_sections
    finished_bbox_sections = finished_bbox_sections_

    global access_token
    access_token = access_token_


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
        bbox, trace_filename = bbox_section
        processed_trace_filename = util.get_processed_trace_filename(trace_filename)

        # If either we have already pulled trace data to disk, or if it's been pulled AND processed by map_matching,
        # don't pull it again.
        if os.path.exists(trace_filename) or os.path.exists(processed_trace_filename):
            print("Seq already exists on disk for bbox={}. Skipping...".format(bbox))
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        # We haven't pulled API trace data for this bbox section yet
        trace_data = make_trace_data_requests(session, bbox, global_config)
        print("Before filter: lens: {}".format([len(t) for t in trace_data]))

        # Perform some simple filters to weed out bad trace data
        trace_data = trace_filter.run(trace_data)
        print("After filter: lens: {}".format([len(t) for t in trace_data]))

        # Avoids potential partial write issues by writing to a temp file and then as a final operation, then renaming
        # to the real location
        temp_filename = os.path.join(global_tmp_dir, bbox + ".pickle")
        pickle.dump(trace_data, open(temp_filename, "wb"))
        os.rename(temp_filename, trace_filename)

        with finished_bbox_sections.get_lock():
            finished_bbox_sections.value += 1
    except Exception as e:
        print("ERROR: Failed to pull trace data: {}".format(repr(e)))


def make_trace_data_requests(
    session_: requests.Session, bbox: str, conf: any
) -> list[list[dict]]:
    """
    Makes the actual calls to Mapillary API to pull trace data for a given bbox string.

    :param session_: requests.Session() to persist session across API calls
    :param bbox: String representation of bbox that Mapillary API understands, i.e. 'min_lon,min_lat,max_lon,max_lat'
    :param conf: Dict of configs. Mandatory keys are ['client_id']. Optional keys are ['sequences_per_page',
        'skip_if_fewer_images_than', 'start_date']
    :return: List of trace data sequences. Trace data is in format understood by Valhalla map matching process, i.e. it
        has 'lon', 'lat', 'time', and optionally 'radius' keys
    """
    bbox_as_list = [float(d) for d in bbox.split(",")]

    # We will use this dict to group trace points by sequence ID
    sequences_by_id = {}

    # Check to see if user specified any overrides in conf JSON
    seq_per_page = (
        conf["sequences_per_page"]
        if "sequences_per_page" in conf
        else SEQUENCES_PER_PAGE_DEFAULT
    )
    img_per_page = (
        conf["images_per_page"] if "images_per_page" in conf else IMAGES_PER_PAGE_DEFAULT
    )
    skip_if_fewer_imgs_than = (
        conf["skip_if_fewer_images_than"]
        if "skip_if_fewer_images_than" in conf
        else SKIP_IF_FEWER_IMAGES_THAN_DEFAULT
    )
    max_sequences_per_bbox_section = (
        conf["max_sequences_per_bbox_section"]
        if "max_sequences_per_bbox_section" in conf
        else MAX_SEQUENCES_PER_BBOX_SECTION_DEFAULT
    )
    start_date = conf["start_date"] if "start_date" in conf else SEQUENCE_START_DATE_DEFAULT

    # Paginate sequences within this bbox
    print("@ MAPILLARY: Getting seq for bbox={}".format(bbox))
    seq_next_url = SEQUENCE_URL.format(access_token, bbox, seq_per_page, start_date)
    seq_page = 1
    while seq_next_url:
        print("@@ MAPILLARY: Seq Page {}, url={}".format(seq_page, seq_next_url))
        seq_resp = session_.get(seq_next_url, timeout=10)
        seq_ids = []
        for seq_f in seq_resp.json()["features"]:
            seq_id = seq_f["properties"]["key"]

            # If we've already processed this seq_id before, skip it, otherwise we will be writing duplicate image data
            if seq_id in sequences_by_id:
                print(
                    "@@@ MAPILLARY: Skipping seq_id={} b/c we've already seen it on a previous page".format(
                        seq_id
                    )
                )
                continue

            # Only process sequences that originated from this bbox. This prevents us from processing sequences twice
            origin_lon, origin_lat = seq_f["geometry"]["coordinates"][0]
            if not is_within_bbox(origin_lon, origin_lat, bbox_as_list):
                print(
                    "@@@ MAPILLARY: Skipping seq b/c origin ({}, {}) not in bbox {}".format(
                        origin_lon, origin_lat, bbox
                    )
                )
                continue

            # Skip sequences that have too few images
            if len(seq_f["geometry"]["coordinates"]) < skip_if_fewer_imgs_than:
                continue

            seq_ids.append(seq_id)

        if len(seq_ids) > 0:
            # Paginate images within these sequences
            img_next_url = IMAGES_URL.format(access_token, ",".join(seq_ids), img_per_page)
            img_page = 1
            while img_next_url:
                print("@@@ MAPILLARY: Image Page {}, url={}".format(img_page, img_next_url))
                img_resp = session_.get(img_next_url, timeout=10)
                for img_f in img_resp.json()["features"]:
                    if img_f["properties"]["sequence_key"] not in sequences_by_id:
                        sequences_by_id[img_f["properties"]["sequence_key"]] = []
                    sequences_by_id[img_f["properties"]["sequence_key"]].append(
                        {
                            "time": parser.isoparse(
                                img_f["properties"]["captured_at"]
                            ).timestamp(),  # Epoch time
                            "lon": img_f["geometry"]["coordinates"][0],
                            "lat": img_f["geometry"]["coordinates"][1],
                        }
                    )

                # Check if there is a next image page or if we are finished with this sequence
                img_next_url = (
                    img_resp.links["next"]["url"] if "next" in img_resp.links else None
                )
                img_page += 1

        # Already collected enough sequences. Move onto the next bbox section
        if len(sequences_by_id) > max_sequences_per_bbox_section:
            print(
                "## Already collected {} seqs for this bbox section, greater than max_sequences_per_bbox_section={}. "
                "Continuing...".format(len(sequences_by_id), max_sequences_per_bbox_section)
            )
            break

        # Check if there is a next sequence page or if we are finished with this bbox
        seq_next_url = seq_resp.links["next"]["url"] if "next" in seq_resp.links else None
        seq_page += 1

    print("Keys: {}".format(list(sequences_by_id.keys())))

    # We don't care about the sequence IDs anymore (just using it as a method to group trace data), so we just return
    # values
    sequences = list(sequences_by_id.values())

    # Mapillary returns their trace data in reverse chronological order (latest image first), so we reverse that
    # back to get the order the images were taken, which is what map matching needs
    [s.reverse() for s in sequences]

    return sequences


def split_bbox(
    traces_dir: str,
    bbox: str,
    zoom: int = 5,
) -> list[tuple[str, str]]:
    """
    Takes the given bbox and splits it up into smaller sections, with the smaller bbox chunks being tiles at a specific
    zoom level. TODO

    :param traces_dir: name of dir where traces should be stored
    :param bbox: bbox string from arg
    :param zoom: TODO
    :return: list of tuples, 0 index: bbox section strings, whose format will be dictated by the to_bbox_str
        function, 1 index: the filename where the pulled trace data should be stored
    """
    sections_filename = util.get_sections_filename(traces_dir)

    try:
        print("Reading bbox_sections from disk...")
        bbox_sections: list[tuple[str, str]] = pickle.load(open(sections_filename, "rb"))
    except (OSError, IOError):
        print("bbox_sections pickle not found. Creating and writing to disk...")
        min_long, min_lat, max_long, max_lat = [float(s) for s in bbox.split(",")]
        # Small sanity checks
        if max_long <= min_long or max_lat <= min_lat:
            raise ValueError(
                "Bounding box {} not well defined. Must be in the format `min_longitude,min_latitude,max_longitude,"
                "max_latitude`.".format(bbox)
            )

        """
        # Perform a check to see how many sections would be generated
        num_files = int(
            ((max_long - min_long) // section_size + 1)
            * ((max_lat - min_lat) // section_size + 1)
        )
        if num_files > MAX_FILES_IN_DIR:
            # TODO: Split up the bbox sections further into 'pages', and use these as different dirs to put output
            #  in, that way we won't ever have too many files in one dir
            print(
                "WARNING: {} bbox sections will be generated and a .pickle file will be created for all of them, "
                "violating the MAX_FILES_IN_DIR={}".format(num_files, MAX_FILES_IN_DIR)
            )
        else:
            print("{} bbox sections will be generated...".format(num_files))
        """
        bbox_sections = []

        tile1 = get_tile_from_lon_lat(min_long, min_lat, zoom)
        tile2 = get_tile_from_lon_lat(max_long, max_lat, zoom)

        start_x, end_x = min(tile1[0], tile2[0]), max(tile1[0], tile2[0])
        start_y, end_y = min(tile1[1], tile2[1]), max(tile1[1], tile2[1])

        for x in range(start_x, end_x + 1):
            for y in range(start_y, end_y + 1):
                print(x, y, zoom)
                # TODO

        """
        prev_long = min_long
        while prev_long < max_long:
            cur_long = min(prev_long + section_size, max_long)
            prev_lat = min_lat
            while prev_lat < max_lat:
                cur_lat = min(prev_lat + section_size, max_lat)

                # Convert the long / lat bbox bounds to a string that the trace source API can understand (using the
                # given lambda)
                bbox_str = to_bbox_str(prev_long, prev_lat, cur_long, cur_lat)

                # The file on disk where we will store trace data
                trace_filename = os.path.join(
                    traces_dir, util.get_sha1_truncated_id(bbox_str) + ".pickle"
                )

                bbox_sections.append((bbox_str, trace_filename))
                prev_lat += section_size
            prev_long += section_size
        """

        pickle.dump(bbox_sections, open(sections_filename, "wb"))

    return bbox_sections


def get_tile_from_lon_lat(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    """
    Turns a lon/lat measurement into a Slippy map tile at a given zoom.
    """
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return xtile, ytile
