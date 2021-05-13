import os
import pickle
import numpy as np
from math import radians, cos, sin, asin, sqrt

import util
import map_matching

# Configurable constants for filtering
MINIMUM_MEAN_SPEED = 0.001
MAXIMUM_MEAN_SPEED = 1
MINIMUM_TIME = 1
MINIMUM_DISTANCE = 0
MAXIMUM_DISTANCE_BETWEEN_ADJACENT_IMAGES = 1000


def run(output_dir: str) -> None:
    """
    FIXME: Clean this up, duplicates functionality in util.py

    :param output_dir: Where the output from the previous step is held
    """
    sections_filename = os.path.join(output_dir, util.SECTIONS_PICKLE_FILENAME)
    bbox_sections = pickle.load(open(sections_filename, 'rb'))

    for bbox in bbox_sections:
        result_filename = os.path.join(output_dir, bbox + '.pickle')
        trace_data = pickle.load(open(result_filename, 'rb'))
        for seq, traces in trace_data.items():
            print('----- SEQ: {} -----'.format(seq))
            speeds = []
            map_matching_shape = []  # The input for the next step

            # Skip if time spent on sequence isn't long enough
            if traces[0][0] - traces[-1][0] < MINIMUM_TIME:
                continue

            total_dist = 0
            # A boolean flag that allows us to signal bad sequences from within the following for loop
            should_skip_sequence = False
            for i in range(len(traces) - 1):
                from_timestamp, from_long_lat = traces[i + 1]
                to_timestamp, to_long_lat = traces[i]
                d = haversine(from_long_lat[0], from_long_lat[1], to_long_lat[0], to_long_lat[1])
                if d > MAXIMUM_DISTANCE_BETWEEN_ADJACENT_IMAGES:
                    should_skip_sequence = True
                t = to_timestamp - from_timestamp
                if t == 0:  # Skip if no distance traveled
                    continue

                total_dist += d
                v = d / t
                speeds.append(v)

                map_matching_shape.append(
                    {'lon': to_long_lat[0], 'lat': to_long_lat[1], 'type': 'via', 'time': to_timestamp})

            if should_skip_sequence:
                continue

            # Skip if distance traveled on sequence isn't long enough
            print('Dist: {}'.format(total_dist))
            if total_dist < MINIMUM_DISTANCE:
                continue

            # Skip if we feel like the speeds in this sequence don't correspond with someone driving
            speeds_arr = np.array(speeds)
            if not valid_driving_speed_mean(speeds_arr):
                continue

            # Append the final image and move onto the next step, map matching
            map_matching_shape.append(
                {'lon': traces[-1][1][0], 'lat': traces[-1][1][1], 'type': 'break', 'time': traces[-1][0]})
            # The first and last objects in the shape list should have 'type': 'break'
            map_matching_shape[0]['type'] = 'break'
            map_matching.map_match(map_matching_shape)

            break
        break


def valid_driving_speed_mean(speeds: np.ndarray) -> bool:
    """
    Determine whether the array of speeds corresponds with a valid driving sequence, currently based off of a simple
    range threshold.
    """

    td = np.array(speeds)
    m = td.mean()
    if m < MINIMUM_MEAN_SPEED or m > MAXIMUM_MEAN_SPEED:
        print("Skipping b/c not mean of {} did not meet driving threshold of ({}, {})".format(m, MINIMUM_MEAN_SPEED,
                                                                                              MAXIMUM_MEAN_SPEED))
        return False
    return True


def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """
    Calculate the great circle distance between two points on the earth (specified in decimal degrees).
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r
