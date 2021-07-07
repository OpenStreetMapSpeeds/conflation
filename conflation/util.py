import hashlib
import os
import uuid

OUTPUT_DIR = "output"
TRACES_DIR = "traces"
TEMP_DIR = "tmp"
MAP_MATCH_DIR = "map_matches"
RESULTS_DIR = "results"
SECTIONS_PICKLE_FILENAME = "sections.pickle"
PROCESSED_TRACE_EXTENSION = ".processed"
FINAL_RESULTS_FILENAME = "config.json"
MAP_MATCH_REGION_FILENAME_DELIMITER = "-"


def initialize_dirs(bbox_str: str) -> tuple[str, str, str, str]:
    """
    Creates all dirs needed for run if they don't exist.

    :param bbox_str: bbox string from arg :return: tuple of (traces dir name, tmp dir name for any tmp pickle files,
        map match results dir name, final results dir name)
    """

    # Use a hash function to generate an "ID" for this bbox. Helped to detect duplicate runs.
    bbox = get_sha1_truncated_id(bbox_str)
    print("This run's ID: {}. All output files will be placed in output/{}".format(bbox, bbox))
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
