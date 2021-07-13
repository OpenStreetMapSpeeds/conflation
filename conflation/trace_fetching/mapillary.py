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
from conflation.trace_fetching import vector_tile_pb2

# IMAGES_PER_PAGE_DEFAULT = 1000  # How many images to receive on each page of the API call
MAX_SEQUENCES_PER_BBOX_SECTION_DEFAULT = (  # How many sequences to process for each bbox section
    500
)
SEQUENCE_START_DATE_DEFAULT = (  # By default we only consider sequences up to a year old
    datetime.datetime.now() - datetime.timedelta(days=365)
)
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = (  # We will skip any sequences if they have fewer than this number of images
    30
)
COVERAGE_TILES_URL = (
    "https://tiles.mapillary.com/maps/vtp/mly1_public/2/{}/{}/{}?access_token={}"
)
SEQUENCE_URL = "https://graph.mapillary.com/image_ids?fields=id&sequence_id={}&access_token={}"
IMAGES_URL = "https://graph.mapillary.com/images?fields=captured_at,geometry&image_ids={}&access_token={}"

# Mapillary v4 supports "coverage" search over a zoom level from 0 to 5. We perform the coverage search at zoom 5
COVERAGE_ZOOM = 5
# We then perform the actual search for trace sequences at zoom 14
BBOX_SECTION_ZOOM = 14


def run(
    bbox: str, traces_dir: str, tmp_dir: str, config: dict, processes: int, access_token: str
) -> None:
    """
    Entrypoint for pulling trace date from Mapillary APIs. Will pull all trace data in the given bbox and store it in
    the traces_dir, using the number of processes specified and any conf values from the `config` JSON.

    :param bbox: Bounding box we are searching over, in the format of 'min_lon,min_lat,max_lon,max_lat'
    :param traces_dir: Dir where trace data will be pickled to
    :param tmp_dir: Dir where temp output files will be stored (should be empty upon completion)
    :param config: Dict of configs. Mandatory keys are ['client_id', 'client_secret']. Optional keys are
        ['start_date', 'max_sequences_per_bbox_section', 'skip_if_fewer_imgs_than']
    :param processes: Number of threads to use
    :param access_token: Mapillary v4 access token (obtained through OAuth)
    """

    # Requests session for persistent connections and timeout settings
    session = requests.Session()
    retry_strategy = Retry(
        total=5, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=3
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # We only want to consider recent sequences, so we take `start_date` as an optional param, and only consider
    # sequences dated past this given date
    start_date = (
        parser.parse(config["start_date"])
        if "start_date" in config
        else SEQUENCE_START_DATE_DEFAULT
    )
    # Convert to epoch milliseconds, since Mapillary's captured_at is in epoch milliseconds
    start_date_epoch = (
        start_date - datetime.datetime.utcfromtimestamp(0)
    ).total_seconds() * 1000.0

    # Break the bbox into sections and save it to a pickle file
    bbox_sections = split_bbox(session, traces_dir, bbox, access_token, start_date_epoch)

    finished_bbox_sections = multiprocessing.Value("i", 0)

    # Adding these "temporary" counters so that we can make some estimates of how much time is wasted due to seeing
    # duplicate sequences across different z14 tiles
    pulled_sequences = multiprocessing.Value("i", 0)
    skipped_sequences_due_to_origin_coordinate = multiprocessing.Value("i", 0)
    skipped_sequences_due_to_filters = multiprocessing.Value("i", 0)

    with multiprocessing.Pool(
        initializer=initialize_multiprocess,
        initargs=(
            session,
            access_token,
            tmp_dir,
            config,
            finished_bbox_sections,
            start_date_epoch,
            pulled_sequences,
            skipped_sequences_due_to_origin_coordinate,
            skipped_sequences_due_to_filters,
        ),
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

        print(
            "Pulled {} sequences. Skipped {} of these sequences because they were duplicates (not originating in "
            "the current-processing bbox). Skipped {} of these sequences because of filters.".format(
                pulled_sequences.value,
                skipped_sequences_due_to_origin_coordinate.value,
                skipped_sequences_due_to_filters.value,
            )
        )

        # TODO: Delete the tmp dir after run?
        return


def initialize_multiprocess(
    session_: requests.Session,
    access_token_: str,
    global_tmp_dir_: str,
    global_config_: dict,
    finished_bbox_sections_: multiprocessing.Value,
    start_date_epoch_: int,
    pulled_sequences_: multiprocessing.Value,
    skipped_sequences_due_to_origin_coordinate_: multiprocessing.Value,
    skipped_sequences_due_to_filters_: multiprocessing.Value,
) -> None:
    """
    Initializes global variables referenced / updated by all threads of the multiprocess API requests.
    """
    global session
    session = session_

    global access_token
    access_token = access_token_

    # So each process knows the output / tmp dirs
    global global_tmp_dir
    global_tmp_dir = global_tmp_dir_

    # So each process knows the conf provided
    global global_config
    global_config = global_config_

    # Integer counter of num of finished bbox_sections
    global finished_bbox_sections
    finished_bbox_sections = finished_bbox_sections_

    global start_date_epoch
    start_date_epoch = start_date_epoch_

    global pulled_sequences
    pulled_sequences = pulled_sequences_

    global skipped_sequences_due_to_origin_coordinate
    skipped_sequences_due_to_origin_coordinate = skipped_sequences_due_to_origin_coordinate_

    global skipped_sequences_due_to_filters
    skipped_sequences_due_to_filters = skipped_sequences_due_to_filters_


def pull_filter_and_save_trace_for_bbox(bbox_section: tuple[int, int, str]) -> None:
    """
    Checks to see if a bbox section already has trace data pulled onto disk. If not, pulls it from Mapillary by calling
    make_trace_data_requests(), filters it using trace_filer.run(), and saves it to disk. Writes to a temp file first
    to avoid issues if script crashes during the pickle dump. Meant to be run in a multi-threaded manner and references
    global vars made by initialize_multiprocess().

    :param bbox_section: tuple where [0:1] indices: [x,y] coordinate of the zoom 14 tile, [2] index: the filename where
        the pulled trace data should be stored
    """
    try:
        tile, trace_filename = bbox_section[0:2], bbox_section[2]
        processed_trace_filename = util.get_processed_trace_filename(trace_filename)

        # If either we have already pulled trace data to disk, or if it's been pulled AND processed by map_matching,
        # don't pull it again.
        if os.path.exists(trace_filename) or os.path.exists(processed_trace_filename):
            print("Seq already exists on disk for tile={}. Skipping...".format(tile))
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        # We haven't pulled API trace data for this bbox section yet
        trace_data = make_trace_data_requests(session, tile, global_config)
        before_filter_num_sequences = len(trace_data)

        # Perform some simple filters to weed out bad trace data
        trace_data = trace_filter.run(trace_data)
        after_filter_num_sequences = len(trace_data)

        # Check how many sequences were filtered out
        if after_filter_num_sequences < before_filter_num_sequences:
            with skipped_sequences_due_to_filters.get_lock():
                skipped_sequences_due_to_filters.value += (
                    before_filter_num_sequences - after_filter_num_sequences
                )

        # Avoids potential partial write issues by writing to a temp file and then as a final operation, then renaming
        # to the real location
        temp_filename = os.path.join(
            global_tmp_dir, "_".join([str(c) for c in tile]) + ".pickle"
        )
        pickle.dump(trace_data, open(temp_filename, "wb"))
        os.rename(temp_filename, trace_filename)

        with finished_bbox_sections.get_lock():
            finished_bbox_sections.value += 1
    except Exception as e:
        print("ERROR: Failed to pull trace data: {}".format(repr(e)))


def make_trace_data_requests(
    session_: requests.Session, tile: tuple[int, int], conf: any
) -> list[list[dict]]:
    """
    Makes the actual calls to Mapillary API to pull trace data for a given tile at zoom 14.

    :param session_: requests.Session() to persist session across API calls
    :param tile: Tuple of [x,y] that represents a tile at z14.
    :param conf: Dict of configs. Mandatory keys are ['client_id', 'client_secret']. Optional keys are
        ['start_date', 'max_sequences_per_bbox_section', 'skip_if_fewer_imgs_than']
    :return: List of trace data sequences. Trace data is in format understood by Valhalla map matching process, i.e. it
        has 'lon', 'lat', 'time', and optionally 'radius' keys
    """
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

    # We will use this dict to group trace points by sequence ID
    sequences = []
    # We will use this set to skip sequences we've already processed
    seen_sequences = set()

    resp = session_.get(
        COVERAGE_TILES_URL.format(BBOX_SECTION_ZOOM, tile[0], tile[1], access_token)
    )

    tile_pb = vector_tile_pb2.Tile()
    tile_pb.ParseFromString(resp.content)

    for layer in tile_pb.layers:
        keys = [v for v in layer.keys]
        values = [v for v in layer.values]
        for feature in layer.features:
            captured_at = None
            sequence_id = None

            # Pull out the sequence id and when it was captured
            for i in range(0, len(feature.tags), 2):
                k = keys[feature.tags[i]]
                if k == "captured_at":
                    captured_at = values[feature.tags[i + 1]].int_value
                if k == "sequence_id":
                    sequence_id = values[feature.tags[i + 1]].string_value
            if captured_at and captured_at > start_date_epoch:
                if sequence_id and sequence_id not in seen_sequences:
                    seen_sequences.add(sequence_id)
                    with pulled_sequences.get_lock():
                        pulled_sequences.value += 1

                    sequence_resp = session_.get(
                        SEQUENCE_URL.format(sequence_id, access_token)
                    )

                    image_ids = [
                        img_id_obj["id"] for img_id_obj in sequence_resp.json()["data"]
                    ]

                    # Skip sequences that have too few images
                    if len(image_ids) < skip_if_fewer_imgs_than:
                        with skipped_sequences_due_to_filters.get_lock():
                            skipped_sequences_due_to_filters.value += 1
                        continue

                    images_resp = session_.get(
                        IMAGES_URL.format(",".join(image_ids), access_token)
                    )
                    images = [
                        {  # Convert to seconds because filtering / map matching assumes time in seconds
                            "time": img_obj["captured_at"] / 1000,
                            "lon": img_obj["geometry"]["coordinates"][0],
                            "lat": img_obj["geometry"]["coordinates"][1],
                        }
                        for img_obj in images_resp.json()["data"]
                    ]

                    # Mapillary returns their trace data in random chronological order, so we need to sort the images
                    images = sorted(images, key=lambda x: x["time"])

                    # Only process sequences that originated from this bbox. This prevents us from processing sequences
                    # twice
                    origin_lon, origin_lat = (
                        images[0]["lon"],
                        images[0]["lat"],
                    )

                    # We need to check if this specific sequence is covered by this zoom 14 tile. We do this by getting
                    # the lon, lat coordinates of this tile and checking the first coordinate of the sequence. This way
                    # we prevent the same sequence being processed by multiple different threads / zoom 14 tiles
                    upper_left_corner = get_lon_lat_from_tile(
                        BBOX_SECTION_ZOOM, tile[0], tile[1]
                    )
                    lower_right_corner = get_lon_lat_from_tile(
                        BBOX_SECTION_ZOOM, tile[0] + 1, tile[1] + 1
                    )
                    cur_tile_as_lon_lat = [
                        upper_left_corner[0],
                        lower_right_corner[1],
                        lower_right_corner[0],
                        upper_left_corner[1],
                    ]
                    if not is_within_bbox(origin_lon, origin_lat, cur_tile_as_lon_lat):
                        with skipped_sequences_due_to_origin_coordinate.get_lock():
                            skipped_sequences_due_to_origin_coordinate.value += 1
                        continue

                    sequences.append(images)

            # Already collected enough sequences. Move onto the next bbox section
            if len(sequences) > max_sequences_per_bbox_section:
                print(
                    "## Already collected {} seqs for this bbox section, greater than max_sequences_per_bbox_section={}"
                    ". Continuing...".format(len(sequences), max_sequences_per_bbox_section)
                )
                break

    return sequences


def split_bbox(
    session_: requests.Session,
    traces_dir: str,
    bbox: str,
    access_token_: str,
    start_date_epoch: float,
) -> list[tuple[int, int, str]]:
    """
    Takes the given bbox, converts it into zoom 5 tiles to check Mapillary coverage, then outputs a list of zoom 14
    tiles that we should call to find trace sequences.

    :param session_: requests session
    :param traces_dir: name of dir where traces should be stored
    :param bbox: bbox string from arg
    :param access_token_: Mapillary v4 access token (obtained through OAuth)
    :param start_date_epoch: Epoch timestamp; any traces taken at a time older than this timestamp will be rejected
    :return: list of tuples, [0:1] indices: [x,y] coordinate of the zoom 14 tile, [2] index: the filename where the
        pulled trace data should be stored
    """
    sections_filename = util.get_sections_filename(traces_dir)

    try:
        print("Reading bbox_sections from disk...")
        bbox_sections: list[tuple[int, int, str]] = pickle.load(open(sections_filename, "rb"))
    except (OSError, IOError):
        print("bbox_sections pickle not found. Creating and writing to disk...")
        min_lon, min_lat, max_lon, max_lat = [float(s) for s in bbox.split(",")]
        # Small sanity checks
        if max_lon <= min_lon or max_lat <= min_lat:
            raise ValueError(
                "Bounding box {} not well defined. Must be in the format `min_longitude,min_latitude,max_longitude,"
                "max_latitude`.".format(bbox)
            )

        bbox_sections = []

        tile1 = get_tile_from_lon_lat(min_lon, max_lat, COVERAGE_ZOOM)
        tile2 = get_tile_from_lon_lat(max_lon, min_lat, COVERAGE_ZOOM)

        start_x, end_x = min(tile1[0], tile2[0]), max(tile1[0], tile2[0])
        start_y, end_y = min(tile1[1], tile2[1]), max(tile1[1], tile2[1])

        print(
            "Searching through zoom=5 tiles from ({}, {}) to ({}, {})".format(
                start_x, start_y, end_x, end_y
            )
        )
        for x in range(start_x, end_x + 1):
            for y in range(start_y, end_y + 1):
                # Create a dir to store trace data for this zoom 5 tile
                zoom_5_dir = os.path.join(
                    traces_dir, "_".join([str(COVERAGE_ZOOM), str(x), str(y)])
                )
                if not os.path.exists(zoom_5_dir):
                    os.makedirs(zoom_5_dir)

                # At 14, the top left corner tile (i.e. pixel (0, 0) at zoom 5 tile)
                base_x_zoom_14 = x * 2 ** (BBOX_SECTION_ZOOM - COVERAGE_ZOOM)
                base_y_zoom_14 = y * 2 ** (BBOX_SECTION_ZOOM - COVERAGE_ZOOM)

                resp = session_.get(
                    COVERAGE_TILES_URL.format(COVERAGE_ZOOM, x, y, access_token_)
                )

                tile_pb = vector_tile_pb2.Tile()
                tile_pb.ParseFromString(resp.content)

                bbox_sections.extend(
                    z14_tiles_from_coverage_tile_to_bbox_sections(
                        tile_pb,
                        start_date_epoch,
                        base_x_zoom_14,
                        base_y_zoom_14,
                        x,
                        y,
                        min_lon,
                        min_lat,
                        max_lon,
                        max_lat,
                        traces_dir,
                    )
                )

        pickle.dump(bbox_sections, open(sections_filename, "wb"))

    return bbox_sections


def z14_tiles_from_coverage_tile_to_bbox_sections(
    tile_pb: vector_tile_pb2.Tile,
    start_date_epoch_: float,
    base_x_zoom_14: int,
    base_y_zoom_14: int,
    x: int,
    y: int,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    traces_dir: str,
) -> list[tuple[int, int, str]]:
    """
    Parses the given tile_pb (which is the protobuf of a Mapillary zoom 5 coverage tile), and determines which zoom 14
    tiles that we will need to traverse for trace sequences. The zoom 5 tiles have "pixels" that are quantized in a
    256x256 fashion. We decode and iterate over these pixels. Each pixel that has a sequence recent enough is
    considered. We also make sure that any zoom 14 tiles we add to bbox_sections is within the original bbox passed in
    (as defined by the min_lon, min_lat, max_lon, max_lat args)

    :param tile_pb: zoom 5 coverage tile to parse
    :param start_date_epoch_: Epoch timestamp; any traces taken at a time older than this timestamp will be rejected
    :param base_x_zoom_14: the x of the zoom 14 tile that corresponds with the (0, 0) pixel of this zoom 5 coverage tile
    :param base_y_zoom_14: the y of the zoom 14 tile that corresponds with the (0, 0) pixel of this zoom 5 coverage tile
    :param x: of the zoom 5 tile
    :param y: of the zoom 5 tile
    :param min_lon: of the original bbox of the run
    :param min_lat: of the original bbox of the run
    :param max_lon: of the original bbox of the run
    :param max_lat: of the original bbox of the run
    :param traces_dir: str where we should store the traces output
    :return: zoom 14 tiles that we will need to traverse for trace sequences
    """
    found_zoom_14_tiles = []

    for layer in tile_pb.layers:
        # This is how we can traverse data within the protobuf
        keys = [v for v in layer.keys]
        values = [v for v in layer.values]
        for feature in layer.features:
            # We want to find the captured_at key which will tell us if the sequences in this pixel is recent enough
            for i in range(0, len(feature.tags), 2):
                if keys[feature.tags[i]] != "captured_at":
                    continue

                # Only consider pixels where the latest sequence is less than one year old
                if values[feature.tags[i + 1]].int_value > start_date_epoch_:
                    pixel_x, pixel_y = feature.geometry[1], feature.geometry[2]

                    # Need to decode the pixel as per protobuf definition
                    decoded_x = (pixel_x >> 1) ^ (-(pixel_x & 1))
                    decoded_y = (pixel_y >> 1) ^ (-(pixel_y & 1))

                    # The decoded (x, y) is actually corresponding to the "center" of a 16x16 square of pixels, so we
                    # "quantize" it which gives us (quantized_x, quantized_y) in the range of (0, 0) to (256, 256)
                    quantized_x = round((decoded_x - 7) / 16)
                    quantized_y = round((decoded_y - 7) / 16)

                    # The potential zoom 14 tiles we can add. Each quantized pixel corresponds with four zoom 14 tiles
                    candidate_zoom_14_tiles = [
                        (base_x_zoom_14 + quantized_x * 2, base_y_zoom_14 + quantized_y * 2),
                        (
                            base_x_zoom_14 + quantized_x * 2 + 1,
                            base_y_zoom_14 + quantized_y * 2,
                        ),
                        (
                            base_x_zoom_14 + quantized_x * 2,
                            base_y_zoom_14 + quantized_y * 2 + 1,
                        ),
                        (
                            base_x_zoom_14 + quantized_x * 2 + 1,
                            base_y_zoom_14 + quantized_y * 2 + 1,
                        ),
                    ]

                    # Figure out which of the candidate zoom 14 tiles are actually within the originally specified bbox
                    zoom_14_tiles_in_bbox = []
                    for candidate_x, candidate_y in candidate_zoom_14_tiles:
                        candidate_min_lon, candidate_max_lat = get_lon_lat_from_tile(
                            BBOX_SECTION_ZOOM, candidate_x, candidate_y
                        )
                        candidate_max_lon, candidate_min_lat = get_lon_lat_from_tile(
                            BBOX_SECTION_ZOOM, candidate_x + 1, candidate_y + 1
                        )

                        if bboxes_overlap(
                            min_lon,
                            min_lat,
                            max_lon,
                            max_lat,
                            candidate_min_lon,
                            candidate_min_lat,
                            candidate_max_lon,
                            candidate_max_lat,
                        ):
                            # The file on disk where we will store trace data, with a dir
                            trace_filename = os.path.join(
                                traces_dir,
                                "_".join([str(COVERAGE_ZOOM), str(x), str(y)]),
                                "_".join(
                                    [
                                        str(BBOX_SECTION_ZOOM),
                                        str(candidate_x),
                                        str(candidate_y),
                                    ]
                                )
                                + ".pickle",
                            )

                            zoom_14_tiles_in_bbox.append(
                                (candidate_x, candidate_y, trace_filename)
                            )
                    found_zoom_14_tiles.extend(zoom_14_tiles_in_bbox)

    return found_zoom_14_tiles


def bboxes_overlap(
    min_lon_1, min_lat_1, max_lon_1, max_lat_1, min_lon_2, min_lat_2, max_lon_2, max_lat_2
):
    """
    Returns a boolean representing whether the given two bboxes overlap at any point.
    """
    # If one bbox is on left side of other
    if min_lon_1 >= max_lon_2 or min_lon_2 >= max_lon_1:
        return False

    # If one bbox is above other
    if min_lat_1 >= max_lat_2 or min_lat_2 >= max_lat_1:
        return False

    return True


def is_within_bbox(lon: float, lat: float, bbox: list[float]) -> bool:
    """
    Checks if lon / lat coordinate is within a bbox in the format of [min_lon, min_lat, max_lon, max_lat]
    """
    return bbox[0] <= lon < bbox[2] and bbox[1] <= lat < bbox[3]


def get_tile_from_lon_lat(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    """
    Turns a lon/lat measurement into a Slippy map tile at a given zoom.
    """
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return xtile, ytile


def get_lon_lat_from_tile(zoom: int, x: int, y: int) -> tuple[float, float]:
    """
    Turns a Slippy map tile at a given zoom into a lon/lat measurement.
    """
    n = 2.0 ** zoom
    lon_deg = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat_deg = math.degrees(lat_rad)
    return lon_deg, lat_deg
