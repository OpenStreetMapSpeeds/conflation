import os
import pickle
from typing import Callable

OUTPUT_DIR = "output"
TRACES_DIR = "traces"
TEMP_DIR = "tmp"
MAP_MATCH_DIR = "map_matches"
RESULTS_DIR = "results"
SECTIONS_PICKLE_FILENAME = "sections.pickle"
MAX_FILES_IN_DIR = 500  # Maximum number of files we will put in one directory


def initialize_dirs(bbox: str) -> tuple[str, str, str, str]:
    """
    Creates all dirs needed for run if they don't exist.

    :param bbox: bbox string from arg :return: tuple of (traces dir name, tmp dir name for any tmp pickle files,
        map match results dir name, final results dir name)
    """
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


def get_sections_filename(traces_dir_: str) -> str:
    """
    Returns the full filename of the .pickle file that holds the bbox sections data (not guaranteed that the file
    exists).
    """
    return os.path.join(traces_dir_, SECTIONS_PICKLE_FILENAME)


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
    :return: list of bbox section strings, whose format will be dictated by the to_bbox_str function
    """
    sections_filename = get_sections_filename(traces_dir)

    try:
        print("Reading bbox_sections from disk...")
        bbox_sections: list[tuple[str, str]] = pickle.load(open(sections_filename, "rb"))
    except (OSError, IOError):
        print("bbox_sections pickle not found. Creating and writing to disk...")
        min_long, min_lat, max_long, max_lat = [float(s) for s in bbox.split(",")]

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
                result_filename = os.path.join(traces_dir, bbox + ".pickle")

                bbox_sections.append((bbox_str, result_filename))
                prev_lat += section_size
            prev_long += section_size

        pickle.dump(bbox_sections, open(sections_filename, "wb"))

    return bbox_sections
