import hashlib
import os
import pickle
import uuid
from typing import Callable

OUTPUT_DIR = "output"
TRACES_DIR = "traces"
TEMP_DIR = "tmp"
MAP_MATCH_DIR = "map_matches"
RESULTS_DIR = "results"
SECTIONS_PICKLE_FILENAME = "sections.pickle"
PROCESSED_TRACE_EXTENSION = ".processed"
FINAL_RESULTS_FILENAME = "config.json"
MAP_MATCH_REGION_FILENAME_DELIMITER = "-"
MAX_FILES_IN_DIR = 500  # Maximum number of files we will put in one directory


def initialize_dirs(bbox_str: str) -> tuple[str, str, str, str]:
    """
    Creates all dirs needed for run if they don't exist.

    :param bbox_str: bbox string from arg :return: tuple of (traces dir name, tmp dir name for any tmp pickle files,
        map match results dir name, final results dir name)
    """

    # Use a hash function to generate an "ID" for this bbox. Helped to detect duplicate runs.
    bbox = get_sha1_truncated_id(bbox_str)
    traces_dir = os.path.join(os.path.dirname(os.getcwd()), OUTPUT_DIR, bbox, TRACES_DIR)
    tmp_dir = os.path.join(os.path.dirname(os.getcwd()), OUTPUT_DIR, bbox, TEMP_DIR)
    map_matches_dir = os.path.join(
        os.path.dirname(os.getcwd()), OUTPUT_DIR, bbox, MAP_MATCH_DIR
    )
    results_dir = os.path.join(os.path.dirname(os.getcwd()), OUTPUT_DIR, bbox, RESULTS_DIR)
    # Make the output tmp, and result dirs if it does not exist yet
    if not os.path.exists(tmp_dir):
        os.makedirs(  # Makes all dirs recursively, so we know "output/" will also now exist
            traces_dir
        )
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    if not os.path.exists(map_matches_dir):
        os.makedirs(map_matches_dir)
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    return traces_dir, tmp_dir, map_matches_dir, results_dir


def get_sha1_truncated_id(s: str) -> str:
    """
    Return a truncated hash of any given string. Used on bbox and bbox sections to keep track of duplicate runs / work
    without having to store the entire lon,lat,lon,lat string every time.
    """
    return hashlib.sha1(s.encode("UTF-8")).hexdigest()[:10]


def get_sections_filename(traces_dir_: str) -> str:
    """
    Returns the full filename of the .pickle file that holds the bbox sections data (not guaranteed that the file
    exists).
    """
    return os.path.join(traces_dir_, SECTIONS_PICKLE_FILENAME)


def get_processed_trace_filename(trace_filename: str) -> str:
    """
    Returns the full filename of the trace pickle, if it has already been processed by a map matching script.
    """
    return trace_filename + PROCESSED_TRACE_EXTENSION


def get_final_config_filename(results_dir: str) -> str:
    """
    Returns the full filename of where the final config JSON should be stored.
    """
    return os.path.join(results_dir, FINAL_RESULTS_FILENAME)


def get_map_match_region_filename_with_identifier(country_dir: str, region: str) -> str:
    """
    To prevent file collision issues during the multiprocess map matching, add a unique identifier to the filename for
    where map match results should be written given a specific country_dir and region.
    """
    return os.path.join(
        country_dir,
        region + MAP_MATCH_REGION_FILENAME_DELIMITER + str(uuid.uuid4())[:8] + ".pickle",
    )


def split_bbox(
    traces_dir: str,
    bbox: str,
    to_bbox_str: Callable[[float, float, float, float], str],
    section_size: float = 0.25,
) -> list[tuple[str, str]]:
    """
    Takes the given bbox and splits it up into smaller sections, with the smaller bbox chunks having long/lat sizes =
    section_size. Also writes the bbox sections to disk so we can pick up instructions from previous runs (may be
    removed).

    :param traces_dir: name of dir where traces should be stored
    :param bbox: bbox string from arg
    :param to_bbox_str: function that takes (min_long, min_lat, max_long, max_lat) bbox definition coordinates, and
        returns a string that we will feed into the next function. Should be the same format as the API source expects
    :param section_size: the smaller bbox sections will have max_long-min_long = max_lat-min_lat = section_size
    :return: list of tuples, 0 index: bbox section strings, whose format will be dictated by the to_bbox_str
        function, 1 index: the filename where the pulled trace data should be stored
    """
    sections_filename = get_sections_filename(traces_dir)

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

        bbox_sections = []
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
                    traces_dir, get_sha1_truncated_id(bbox_str) + ".pickle"
                )

                bbox_sections.append((bbox_str, trace_filename))
                prev_lat += section_size
            prev_long += section_size

        pickle.dump(bbox_sections, open(sections_filename, "wb"))

    return bbox_sections
