import os
import pickle
from typing import Callable

OUTPUT_DIR = 'output'
TEMP_DIR = 'tmp'
SECTIONS_PICKLE_FILENAME = 'sections.pickle'
MAX_FILES_IN_DIR = 500  # Maximum number of files we will put in one directory


def initialize_dirs(bbox: str) -> tuple[str, str]:
    """
    Creates all dirs needed for run if they don't exist.

    :param bbox: bbox string from arg
    :return: tuple of (output dir name, tmp dir name for any tmp pickle files)
    """
    output_dir = os.path.join(
        os.getcwd(),
        OUTPUT_DIR,
        bbox
    )
    output_tmp_dir = os.path.join(
        os.getcwd(),
        OUTPUT_DIR,
        bbox,
        TEMP_DIR
    )
    # Make the output and tmp dirs if it does not exist yet
    if not os.path.exists(output_tmp_dir):
        os.makedirs(output_tmp_dir)  # Makes all dirs recursively, so we know output_dir will also now exist

    return output_dir, output_tmp_dir


def split_bbox(output_dir_: str, bbox: str, to_bbox_str: Callable[[float, float, float, float], str],
               section_size: float = 0.25) -> list[tuple[str, str]]:
    """
    Takes the given bbox and splits it up into smaller sections, with the smaller bbox chunks having long/lat sizes =
    section_size. Also writes the bbox sections to disk so we can pick up instructions from previous runs (may be
    removed).

    :param output_dir_: output dir name
    :param bbox: bbox string from arg
    :param to_bbox_str: function that takes (min_long, min_lat, max_long, max_lat) bbox definition coordinates, and
        returns a string that we will feed into the next function. Should be the same format as the API source expects
    :param section_size: the smaller bbox sections will have max_long-min_long = max_lat-min_lat = section_size
    :return: list of bbox section strings, whose format will be dictated by the to_bbox_str function
    """
    sections_filename = os.path.join(output_dir_, SECTIONS_PICKLE_FILENAME)

    try:
        print('Reading bbox_sections from disk...')
        bbox_sections = pickle.load(open(sections_filename, 'rb'))
    except (OSError, IOError):
        print('bbox_sections pickle not found. Creating and writing to disk...')
        min_long, min_lat, max_long, max_lat = [float(s) for s in bbox.split(',')]

        # Perform a check to see how many sections would be generated
        num_files = int(((max_long - min_long) // section_size + 1) * ((max_lat - min_lat) // section_size + 1))
        if num_files > MAX_FILES_IN_DIR:
            # TODO: Check len of bbox_sections, if over some size limit, we split things up
            print('WARNING: {} bbox sections will be generated and a .pickle file will be created for all of them, '
                  'violating the MAX_FILES_IN_DIR={}'.format(num_files, MAX_FILES_IN_DIR))
        else:
            print('{} bbox sections will be generated...'.format(num_files))

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
                result_filename = os.path.join(output_dir_, bbox + '.pickle')

                bbox_sections.append((bbox_str, result_filename))
                prev_lat += section_size
            prev_long += section_size

        pickle.dump(bbox_sections, open(sections_filename, 'wb'))

    return bbox_sections
