import datetime
import logging
import math
import multiprocessing
import os
import pickle
import requests
from dateutil import parser
from ratelimit import limits, sleep_and_retry
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from conflation import util, trace_filter
from conflation.trace_fetching import vector_tile_pb2, routable_z5_tiles

MAX_SEQUENCES_PER_BBOX_SECTION_DEFAULT = (  # Max number of sequences IDs to pull for each z14 tile by default
    500
)
SEQUENCE_START_DATE_DEFAULT = (  # By default we only consider sequences up to five years old
    datetime.datetime.now() - datetime.timedelta(days=365 * 5)
)
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = (  # We will skip any sequences if they have fewer than this number of images
    30
)

# Mapillary v4 supports "coverage" search over a zoom level from 0 to 5. We perform the coverage search at zoom 5
COVERAGE_ZOOM = 5
# We then perform the actual search for trace sequences at zoom 14
BBOX_SECTION_ZOOM = 14
# The size of the sequence ID blocks that each thread will handle when pulling images
SEQUENCE_ID_BLOCK_SIZE = 10

# Name of the dir where we store sequence IDs pulled from Mapillary
SEQUENCE_IDS_DIR_NAME = "seq_ids"

# Mapillary API URLs
COVERAGE_TILES_URL = (
    "https://tiles.mapillary.com/maps/vtp/mly1_public/2/{}/{}/{}?access_token={}"
)
SEQUENCE_URL = "https://graph.mapillary.com/image_ids?fields=id&sequence_id={}&access_token={}"
IMAGES_URL = "https://graph.mapillary.com/images?fields=captured_at,geometry&image_ids={}&access_token={}"

# For all of the Mapillary calls, we need to rate limit them. The rate limit is 60k / min (last updated: 12/2021); we
# give a small leeway to make sure we don't go over
# Note(rzyc): The API calls are made from different processes and the `ratelimit` module currently rate limits within
#  each individual process but not globally. So we will later have to divide up this global rate limit by the number
#  of processes
MAPILLARY_MAX_CALLS_PER_MINUTE = 59000
MAPILLARY_PERIOD_MINUTE = 60


def check_rate_limit_undecorated() -> None:
    """
    This method does nothing, but we will add rate-limiting decorators to it and use it as a buffer before any Mapillary
    API calls.
    """
    return


def run(
    bbox: str, traces_dir: str, tmp_dir: str, config: dict, processes: int, access_token: str
) -> None:
    """
    Entrypoint for pulling trace date from Mapillary APIs. Will pull all Mapillary sequence IDs in the given bbox and
    store them temporarily so that we can find all unique sequences. Then, we pull the images (traces) for each sequence
    and store it in the traces_dir. Both these steps use the number of processes specified and any conf values from the
    `config` JSON.

    :param bbox: Bounding box we are searching over, in the format of 'min_lon,min_lat,max_lon,max_lat'
    :param traces_dir: Dir where trace data will be pickled to
    :param tmp_dir: Dir where temp output files will be stored (should be empty upon completion)
    :param config: Dict of configs. See "--trace-config" section of README for keys
    :param processes: Number of threads to use
    :param access_token: Mapillary v4 access token (obtained through OAuth)
    """

    # Requests session for persistent connections and timeout settings
    session = requests.Session()
    retry_strategy = Retry(
        total=5, status_forcelist=[429, 500, 502, 503, 504, 302], backoff_factor=3
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

    # Create a dir to store sequence IDs that we pull from Mapillary
    sequence_ids_dir = os.path.join(tmp_dir, SEQUENCE_IDS_DIR_NAME)

    # Break the bbox into sections and save it to a pickle file
    bbox_sections = split_bbox(session, sequence_ids_dir, bbox, access_token, start_date_epoch)

    # Multiprocess values to keep track of progress when we are running multi-threaded tasks
    finished_bbox_sections = multiprocessing.Value("i", 0)
    finished_sequence_id_blocks = multiprocessing.Value("i", 0)
    skipped_sequences_due_to_filters = multiprocessing.Value("i", 0)

    # Divide up the total rate limit by the number of processes
    mapillary_max_calls_per_process_per_minute = round(
        MAPILLARY_MAX_CALLS_PER_MINUTE / processes
    )

    with multiprocessing.Pool(
        initializer=initialize_multiprocess,
        initargs=(
            session,
            access_token,
            tmp_dir,
            config,
            finished_bbox_sections,
            finished_sequence_id_blocks,
            start_date_epoch,
            skipped_sequences_due_to_filters,
            mapillary_max_calls_per_process_per_minute,
        ),
        processes=processes,
    ) as pool:
        # Run the multiprocess job that takes all the bbox_sections, and pulls all the sequence IDs that are within each
        # section
        result = pool.map_async(pull_sequence_ids_for_bbox, bbox_sections)

        # This file holds the blocks of unique sequence IDs that we've pulled from Mapillary. See if it already exists;
        # if it does then we don't need to pull sequence IDs from Mapillary again
        traces_sections_filename = util.get_sections_filename(traces_dir)
        try:
            logging.info("Reading sequence ID sections from disk...")
            sequence_id_blocks: list[tuple[list[str], str]] = pickle.load(
                open(traces_sections_filename, "rb")
            )
        except (OSError, IOError):
            logging.info(
                "Sequence ID sections pickle not found. Creating and writing to disk..."
            )
            logging.info(
                "Pulling sequence IDs from the {} z14 tiles and placing them in {}...".format(
                    len(bbox_sections), sequence_ids_dir
                )
            )
            progress = 0
            increment = 5
            while not result.ready():
                result.wait(timeout=5)
                next_progress = int(finished_bbox_sections.value / len(bbox_sections) * 100)
                if int(next_progress / increment) > progress:
                    logging.info("Current progress: {}%".format(next_progress))
                    progress = int(next_progress / increment)
            if progress != 100 / increment:
                logging.info("Current progress: 100%")

            # Get all the unique sequence IDs that were pulled from the previous step
            logging.info(
                "Reading sequence IDs from all z14 tiles and generating blocks of {} unique sequence IDs...".format(
                    SEQUENCE_ID_BLOCK_SIZE
                )
            )
            sequence_id_blocks = find_unique_sequence_ids(bbox_sections, traces_dir)

            pickle.dump(sequence_id_blocks, open(traces_sections_filename, "wb"))

        # Run the multiprocess job that goes through all unique sequence IDs and actually pulls the images / coordinates
        # for each sequence
        result = pool.map_async(
            pull_filter_and_save_trace_for_sequence_ids, sequence_id_blocks
        )

        logging.info("Placing {} results in {}...".format(len(sequence_id_blocks), traces_dir))
        progress = 0
        increment = 5
        while not result.ready():
            result.wait(timeout=5)
            next_progress = int(
                finished_sequence_id_blocks.value / len(sequence_id_blocks) * 100
            )
            if int(next_progress / increment) > progress:
                logging.info("Current progress: {}%".format(next_progress))
                progress = int(next_progress / increment)
        if progress != 100 / increment:
            logging.info("Current progress: 100%")

        logging.info(
            "Note: {} sequences were skipped because of filters.".format(
                skipped_sequences_due_to_filters.value
            )
        )

        return


def initialize_multiprocess(
    session_: requests.Session,
    access_token_: str,
    global_tmp_dir_: str,
    global_config_: dict,
    finished_bbox_sections_: multiprocessing.Value,
    finished_sequence_id_blocks_: multiprocessing.Value,
    start_date_epoch_: int,
    skipped_sequences_due_to_filters_: multiprocessing.Value,
    mapillary_max_calls_per_process_per_minute_: int,
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

    # Integer counter of num of finished finished_sequence_id_blocks
    global finished_sequence_id_blocks
    finished_sequence_id_blocks = finished_sequence_id_blocks_

    global start_date_epoch
    start_date_epoch = start_date_epoch_

    global skipped_sequences_due_to_filters
    skipped_sequences_due_to_filters = skipped_sequences_due_to_filters_

    # Introduce decorators to the global rate limit check function; each thread gets their own version of this decorated
    # function with a rate limit of (GLOBAL_RATE_LIMIT / #processes) / TIME_PERIOD
    global check_rate_limit
    check_rate_limit = sleep_and_retry(
        limits(
            calls=mapillary_max_calls_per_process_per_minute_, period=MAPILLARY_PERIOD_MINUTE
        )(check_rate_limit_undecorated)
    )


def pull_sequence_ids_for_bbox(bbox_section: tuple[int, int, str]) -> None:
    """
    First, check to see if a bbox section already had sequence IDs pulled onto disk. If not, pull all sequence IDs
    within the current bbox section from Mapillary by calling make_sequence_ids_requests() and save it to disk. Meant to
    be run in a multi-threaded manner and references global vars made by initialize_multiprocess().

    :param bbox_section: tuple where [0:1] indices: [x,y] coordinate of the zoom 14 tile, [2] index: the filename where
        the pulled sequence IDs should be stored
    """
    try:
        tile, sequence_ids_filename = bbox_section[0:2], bbox_section[2]

        # If either we have already pulled sequence IDs to disk, don't pull it again
        if os.path.exists(sequence_ids_filename):
            logging.info(
                "Seq IDs already exists on disk for tile={}. Skipping...".format(tile)
            )
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        sequence_ids: set[str] = make_sequence_ids_requests(session, tile, global_config)

        # Avoids potential partial write issues by writing to a temp file and then as a final operation, then renaming
        # to the real location
        temp_filename = os.path.join(
            global_tmp_dir, "_".join([str(c) for c in tile]) + ".pickle"
        )
        pickle.dump(sequence_ids, open(temp_filename, "wb"))
        os.rename(temp_filename, sequence_ids_filename)

        with finished_bbox_sections.get_lock():
            finished_bbox_sections.value += 1
    except Exception as e:
        logging.error("Failed to pull sequence IDs: {}".format(repr(e)))


def pull_filter_and_save_trace_for_sequence_ids(
    sequence_id_blocks: tuple[list[str], str]
) -> None:
    """
    First, check to see if a sequence ID block already has trace data pulled onto disk. If not, pull it from Mapillary
    by calling make_trace_data_requests(), filter it using trace_filer.run(), and save it to disk. This is meant to
    be run in a multi-threaded manner and references global vars made by initialize_multiprocess().

    :param sequence_id_blocks: tuple where [0] index: list of sequence IDs to pull traces for, [1] index: the filename
        where the pulled trace data should be stored
    """
    try:
        sequence_id_block, trace_filename = sequence_id_blocks[0], sequence_id_blocks[1]
        processed_trace_filename = util.get_processed_trace_filename(trace_filename)

        # If either we have already pulled trace data to disk, or if it's been pulled AND processed by map_matching,
        # don't pull it again.
        if os.path.exists(trace_filename) or os.path.exists(processed_trace_filename):
            logging.info(
                "Traces already exists on disk for sequence_id_block={}. Skipping...".format(
                    sequence_id_block
                )
            )
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        # We haven't pulled API trace data for this bbox section yet
        trace_data = make_trace_data_requests(session, sequence_id_block, global_config)
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
        temp_filename = os.path.join(global_tmp_dir, os.path.basename(trace_filename))
        pickle.dump(trace_data, open(temp_filename, "wb"))
        os.rename(temp_filename, trace_filename)

        with finished_sequence_id_blocks.get_lock():
            finished_sequence_id_blocks.value += 1
    except Exception as e:
        logging.error("Failed to pull trace data: {}".format(repr(e)))


def make_sequence_ids_requests(
    session_: requests.Session, tile: tuple[int, int], conf: any
) -> set[str]:
    """
    Makes the call to the Mapillary API to pull sequence IDs for a given tile at zoom 14.

    :param session_: requests.Session() to persist session across API calls
    :param tile: Tuple of [x,y] that represents a tile at z14
    :param conf: Dict of configs. See "--trace-config" section of README for keys
    :return: List of sequence IDs within the tile
    """
    max_sequences_per_bbox_section = (
        conf["max_sequences_per_bbox_section"]
        if "max_sequences_per_bbox_section" in conf
        else MAX_SEQUENCES_PER_BBOX_SECTION_DEFAULT
    )

    # We will use this set to make sure the sequences we pull here are all unique (Mapillary does have occasional bugs
    # with duplicates)
    seen_sequences = set()

    check_rate_limit()  # Check the Mapillary rate limit
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

        # Already collected enough sequences. Move onto the next bbox section
        if len(seen_sequences) > max_sequences_per_bbox_section:
            logging.info(
                "Note: Already collected {} seqs for this bbox section, greater than max_sequences_per_bbox_section={}"
                ". Continuing...".format(len(seen_sequences), max_sequences_per_bbox_section)
            )
            break

    return seen_sequences


def make_trace_data_requests(
    session_: requests.Session, sequence_ids: list[str], conf: any
) -> list[list[dict]]:
    """
    Makes the calls to the Mapillary API to pull trace data for a given list of sequence IDs.

    :param session_: requests.Session() to persist session across API calls
    :param sequence_ids: List of strings representing Mapillary sequence IDs
    :param conf: Dict of configs. See "--trace-config" section of README for keys
    :return: List of trace data sequences. Trace data is in format understood by Valhalla map matching process, i.e. it
        has 'lon', 'lat', 'time', and optionally 'radius' keys
    """
    skip_if_fewer_imgs_than = (
        conf["skip_if_fewer_images_than"]
        if "skip_if_fewer_images_than" in conf
        else SKIP_IF_FEWER_IMAGES_THAN_DEFAULT
    )

    sequences = []
    for sequence_id in sequence_ids:
        check_rate_limit()  # Check the Mapillary rate limit
        sequence_resp = session_.get(SEQUENCE_URL.format(sequence_id, access_token))
        image_ids = [img_id_obj["id"] for img_id_obj in sequence_resp.json()["data"]]

        # Skip sequences that have too few images
        if len(image_ids) < skip_if_fewer_imgs_than:
            with skipped_sequences_due_to_filters.get_lock():
                skipped_sequences_due_to_filters.value += 1
            continue

        check_rate_limit()  # Check the Mapillary rate limit
        images_resp = session_.get(IMAGES_URL.format(",".join(image_ids), access_token))
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

        sequences.append(images)

    return sequences


def find_unique_sequence_ids(
    bbox_sections: list[tuple[int, int, str]], traces_dir: str
) -> list[tuple[list[str], str]]:
    """
    Goes through all the sequence IDs that were pulled for each bbox section (i.e. z14 tile), reads them all into
    memory, and keeps a unique master list of sequence IDs that we'll need to pull. Then, it splits this master list
    into blocks of size SEQUENCE_ID_BLOCK_SIZE. Finally, it includes the filename of where the trace data for each block
    of IDs should be stored.

    :param bbox_sections: list of tuples where the final [-1] index gives us the filename where the pulled sequence IDs
        were stored
    :param traces_dir: the dir where the pulled trace data should be stored
    :return: list of tuples where [0] index: list of sequence IDs to pull traces for, [1] index: the filename where the
        pulled trace data should be stored
    """
    # First, aggregate all the unique sequence IDs that were pulled
    unique_sequence_ids: set[str] = set()
    total_sequence_ids_count = 0

    for bbox_section in bbox_sections:
        trace_filename = bbox_section[-1]
        sequence_ids: set[str] = pickle.load(open(trace_filename, "rb"))
        unique_sequence_ids.update(sequence_ids)
        total_sequence_ids_count += len(sequence_ids)

    logging.info(
        "Note: Out of {} sequence IDs, {} were unique.".format(
            total_sequence_ids_count, len(unique_sequence_ids)
        )
    )

    # Then, build the blocks of unique sequence IDs
    sequence_id_blocks = []
    unique_sequence_ids_list: list[str] = list(unique_sequence_ids)
    block_num = 0  # Used for the filename where the traces will be stored
    for i in range(0, len(unique_sequence_ids_list), SEQUENCE_ID_BLOCK_SIZE):
        trace_filename = os.path.join(traces_dir, "block_{}.pickle".format(block_num))
        block_num += 1

        sequence_id_blocks.append(
            (unique_sequence_ids_list[i : i + SEQUENCE_ID_BLOCK_SIZE], trace_filename)
        )

    return sequence_id_blocks


def split_bbox(
    session_: requests.Session,
    storage_dir: str,
    bbox: str,
    access_token_: str,
    start_date_epoch: float,
) -> list[tuple[int, int, str]]:
    """
    Takes the given bbox, converts it into zoom 5 tiles to check Mapillary coverage, then outputs a list of zoom 14
    tiles that we should call to find trace sequences.

    :param session_: requests session
    :param storage_dir: name of dir where results from this step should be stored
    :param bbox: bbox string from arg
    :param access_token_: Mapillary v4 access token (obtained through OAuth)
    :param start_date_epoch: Epoch timestamp; any traces taken at a time older than this timestamp will be rejected
    :return: list of tuples, [0:1] indices: [x,y] coordinate of the zoom 14 tile, [2] index: the filename where the
        pulled trace data should be stored
    """
    sections_filename = util.get_sections_filename(storage_dir)

    try:
        logging.info("Reading bbox_sections from disk...")
        bbox_sections: list[tuple[int, int, str]] = pickle.load(open(sections_filename, "rb"))
    except (OSError, IOError):
        logging.info("bbox_sections pickle not found. Creating and writing to disk...")
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

        logging.info(
            "Searching through zoom=5 tiles from ({}, {}) to ({}, {})".format(
                start_x, start_y, end_x, end_y
            )
        )
        for x in range(start_x, end_x + 1):
            for y in range(start_y, end_y + 1):
                # Check to see if this z5 tile is routable in OSM
                if (x, y) not in routable_z5_tiles.ROUTABLE_Z5_TILES:
                    continue

                # The calls here don't need to be rate limited since there there are only so many z5 tiles
                resp = session_.get(
                    COVERAGE_TILES_URL.format(COVERAGE_ZOOM, x, y, access_token_)
                )
                if resp.status_code != 200:
                    raise ConnectionError(
                        "Error pulling z5 tile ({}, {}) from Mapillary: Status {}".format(
                            x, y, resp.status_code
                        )
                    )

                # Create a dir to store trace data for this zoom 5 tile
                zoom_5_dir = os.path.join(
                    storage_dir, "_".join([str(COVERAGE_ZOOM), str(x), str(y)])
                )
                if not os.path.exists(zoom_5_dir):
                    os.makedirs(zoom_5_dir)

                # At 14, the top left corner tile (i.e. pixel (0, 0) at zoom 5 tile)
                base_x_zoom_14 = x * 2 ** (BBOX_SECTION_ZOOM - COVERAGE_ZOOM)
                base_y_zoom_14 = y * 2 ** (BBOX_SECTION_ZOOM - COVERAGE_ZOOM)

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
                        storage_dir,
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
    storage_dir: str,
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
    :param storage_dir: name of dir where results from this step should be stored
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
                            storage_filename = os.path.join(
                                storage_dir,
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
                                (candidate_x, candidate_y, storage_filename)
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

    # Clamps lon, lat to proper mercator projection values
    lat = min(lat, 85.0511)
    lat = max(lat, -85.0511)
    lon = min(lon, 179.9999)
    lon = max(lon, -179.9999)

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
