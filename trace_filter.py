import numpy as np
from math import radians, cos, sin, asin, sqrt

# Configurable constants for filtering
MINIMUM_MEAN_SPEED = 10  # km / h
MINIMUM_TOTAL_TIME = 120  # seconds, can travel a couple edges
MINIMUM_TOTAL_DISTANCE = 1000  # meters, about a few city blocks
MAXIMUM_TIME_BETWEEN_ADJACENT_POINTS = 5  # seconds, we need high granularity to make accurate map matches
MAXIMUM_SPEED_BETWEEN_ADJACENT_POINTS = 160  # km / h, to weed out poor measurements and trains
MAXIMUM_POOR_MEASUREMENTS_PERCENT = 0.25  # 25%, max %age of traces that are marked as poor measurements


def run(trace_data: list[list[dict]]) -> list[list[dict]]:
    """
    Performs simple filters on trace_data. A list of trace data will only be accepted if:
    - Total time of sequence exceeds MINIMUM_TOTAL_TIME
    - There are no out-of-order timestamps, i.e. all points are sequential
    - If time between adjacent points is too long, i.e. adjacent time deltas more than
        MAXIMUM_TIME_BETWEEN_ADJACENT_POINTS, increment 'num_poor_measurements' counter
    - If speed between adjacent points is too fast, i.e. adjacent speeds more than
        MAXIMUM_SPEED_BETWEEN_ADJACENT_POINTS, increment 'num_poor_measurements' counter
    - The 'num_poor_measurements' counter is too high as per MAXIMUM_POOR_MEASUREMENTS_PERCENT
    - Total distance of sequence exceeds MINIMUM_TOTAL_DISTANCE
    - Mean speed is above the walking / driving threshold, MINIMUM_MEAN_SPEED

    :param trace_data: List of sequence of traces, where each trace should be dict objects with the same format used by
        the Valhalla map matching API, i.e. it should have 'lon', 'lat', 'time', and optionally 'radius' keys
    :return: Filtered list of trace sequences using the same dict object format
    """
    filtered_trace_data = []
    for sequence in trace_data:
        speeds = []

        # Skip if time spent on sequence isn't long enough
        if sequence[-1]['time'] - sequence[0]['time'] < MINIMUM_TOTAL_TIME:
            print('Skipping b/c min time {}'.format(sequence[-1]['time'] - sequence[0]['time']))
            continue

        total_dist = 0  # meters
        # Number of traces that we mark as being measurements as per MAXIMUM_TIME_BETWEEN_ADJACENT_POINTS and
        # MAXIMUM_SPEED_BETWEEN_ADJACENT_POINTS
        num_poor_measurements = 0

        # A boolean flag that allows us to signal bad sequences from within the following for loop
        should_skip_sequence = False
        for i in range(len(sequence) - 1):
            from_timestamp, from_lon, from_lat = sequence[i]['time'], sequence[i]['lon'], sequence[i]['lat']
            to_timestamp, to_lon, to_lat = sequence[i + 1]['time'], sequence[i + 1]['lon'], sequence[i + 1]['lat']
            d = haversine(from_lon, from_lat, to_lon, to_lat)  # Meters
            t = to_timestamp - from_timestamp

            # It's essential for us to submit traces in order for map matching, so if a trace's timestamp is less
            # than a previous trace's timestamp, something is wrong with this sequence so we will throw it away to
            # be safe
            if t < 0:
                print('Skipping b/c min time < 0')
                should_skip_sequence = True

            # Skip calculating speed for this specific trace point if no time elapsed
            if t == 0:
                continue

            # Adjacent points should not have too large of a time gap
            if t > MAXIMUM_TIME_BETWEEN_ADJACENT_POINTS:
                num_poor_measurements += 1

            total_dist += d
            v_kmph = d / 1000 / t * 3600  # km / h

            # Should not be going crazy fast between adjacent points
            if v_kmph > MAXIMUM_SPEED_BETWEEN_ADJACENT_POINTS:
                num_poor_measurements += 1

            speeds.append(v_kmph)

        if should_skip_sequence:
            print('Skipping b/c should skip seq')
            continue

        if num_poor_measurements / len(sequence) > MAXIMUM_POOR_MEASUREMENTS_PERCENT:
            print('Skipping b/c too many latent traces {}'.format(num_poor_measurements))
            continue

        # Skip if distance traveled on sequence isn't long enough
        if total_dist < MINIMUM_TOTAL_DISTANCE:
            print('Skipping b/c min total dist {}'.format(total_dist))
            continue

        # Skip if we feel like the average speed in this sequence isn't fast enough correspond with someone driving
        if np.array(speeds).mean() < MINIMUM_MEAN_SPEED:
            print('Skipping b/c mean speed {}'.format(np.array(speeds).mean()))
            continue

        filtered_trace_data.append(sequence)

    return filtered_trace_data


def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """
    Calculate the great circle distance between two points on the earth (specified in decimal degrees).

    :return: Distance in meters
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6378160  # Radius of earth in meters.
    return c * r
