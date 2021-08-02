import multiprocessing
import os
import pickle
import requests

from conflation import util

VALHALLA_MAP_MATCHING_URL_EXTENSION = "trace_attributes"

MAXIMUM_UNMATCHED_PERCENTAGE = (
    0.25  # If more than 25% of points are unmatched, skip this sequence
)
MAXIMUM_SPEED = 160  # km / h, to weed out poor measurements and trains
DENSITY_CLASSIFICATIONS = [  # The different density classifications we can give to roads
    "rural",
    "suburban",
    "urban",
]


def run(traces_dir: str, map_matches_dir: str, processes: int, config: dict) -> None:
    """
    This method is the second step in the script. It takes the traces from the first step and performs "map matching",
    which is the process of determining which real-life roads the GPS traces map to. We use the open source Valhalla
    service to do the map matching, and these results allow us to determine the speed the traces were taken at, as well
    as the specific road that the trace was taken on.

    The map matching process happens in a multi-processed format, and this method initializes the threads and tracks the
    overall progress.

    :param traces_dir: Dir where trace data from step 1 was pickled to
    :param map_matches_dir: Dir where map match results will be pickled to
    :param processes: Number of threads to use
    :param config: Dict of configs. See "--map-matching-config" section of README for keys
    """
    sections_filename = util.get_sections_filename(traces_dir)

    # Do a quick check to see if user specified the mandatory 'base_url' in config JSON
    if "base_url" not in config:
        raise KeyError(
            'Missing "base_url" (Mapillary Client ID) key in --map-matching-config JSON.'
        )

    try:
        print("Reading bbox_sections from disk...")
        bbox_sections: list[tuple[str, str]] = pickle.load(open(sections_filename, "rb"))
    except (OSError, IOError):
        raise FileNotFoundError(
            "bbox sections pickle could not be loaded from /output/traces. Cannot perform map matching."
        )

    finished_bbox_sections = multiprocessing.Value("i", 0)
    with multiprocessing.Pool(
        initializer=initialize_multiprocess,
        initargs=(map_matches_dir, config, finished_bbox_sections),
        processes=processes,
    ) as pool:
        result = pool.map_async(map_match_for_bbox, bbox_sections)

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


def initialize_multiprocess(
    global_map_matches_dir_: str,
    global_config_: dict,
    finished_bbox_sections_: multiprocessing.Value,
) -> None:
    """
    Initializes global variables referenced / updated by all threads of the multiprocess map matching requests.
    """

    global global_map_matches_dir
    global_map_matches_dir = global_map_matches_dir_

    # Integer counter of num of finished bbox_sections
    global finished_bbox_sections
    finished_bbox_sections = finished_bbox_sections_

    # So each process knows the conf provided
    global global_config
    global_config = global_config_


def map_match_for_bbox(bbox_sections: tuple) -> None:
    """
    Performs map matching for a given bbox section. It pulls the bbox section's traces from the previous steps by
    finding it on the filesystem. For each sequence in the traces, it passes it to add_map_matches_for_shape to make the
    actual Valhalla call. Map matching results are propagated to disk with write_map_matches and the original traces
    pickle is renamed as a checkpoint.

    :param bbox_sections: From util.split_bbox, used in previous step. List of tuples, where the last element is the
        filename where the trace data is stored.
    """
    try:
        bbox, trace_filename = bbox_sections[:-1], bbox_sections[-1]
        processed_trace_filename = util.get_processed_trace_filename(trace_filename)

        # Check to see if the trace has already been processed by map_matching
        if os.path.exists(processed_trace_filename):
            print("Map matching already complete for bbox={}. Skipping...".format(bbox))
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        trace_data: list[list[dict]] = pickle.load(open(trace_filename, "rb"))
        map_matches = {}
        [add_map_matches_for_shape(map_matches, shape, global_config) for shape in trace_data]
        if len(map_matches):
            write_map_matches(global_map_matches_dir, map_matches)

        # Once all results have been written, mark the file as processed by renaming
        os.rename(trace_filename, processed_trace_filename)
        with finished_bbox_sections.get_lock():
            finished_bbox_sections.value += 1
    except Exception as e:
        print("ERROR: Failed to map match using Valhalla: {}".format(repr(e)))


def add_map_matches_for_shape(
    map_matches: dict[str, dict[str, list[tuple]]], shape: any, conf: dict
) -> None:
    """
    Calls Valhalla API with the given shape dict and adds the map matching results to map_matches in place. Does some
    filtering for bad map matches if there are too many unmatched points or if the elapsed time isn't monotonically
    increasing.

    :param map_matches: Dict of already existing map matches
    :param shape: "Shape" object that is passed into Valhalla's APIs. See Valhalla's README for more specifications
    :param conf: Dict of configs. See "--map-matching-config" section of README for keys
    """
    body = {"shape": shape, "costing": "auto", "shape_match": "map_snap"}
    base_url = conf["base_url"]
    headers = conf["headers"] if "headers" in conf else None

    resp = requests.post(
        base_url + VALHALLA_MAP_MATCHING_URL_EXTENSION,
        json=body,
        headers=headers,
    )

    if resp.status_code != 200:
        # 400 Error code from Valhalla simply means that a match could not be made. This is fine, we'll just skip the
        # sequence.
        if resp.status_code == 400:
            print("Skipping b/c 400 response from Valhalla: {}".format(resp.json()))
            return

        # Any other status code and we want to report an error.
        raise ConnectionError(
            "Error connecting to Valhalla: Status {} Resp {}".format(
                resp.status_code, resp.json()
            )
        )

    resp = resp.json()

    if has_too_many_unmatched(resp["matched_points"]):
        print("Skipping b/c too many points unmatched")
        return

    prev_t = resp["edges"][0]["end_node"]["elapsed_time"]
    # TODO: Figure out the funky math for the first and last edges
    for e in resp["edges"][1:-1]:
        way_length = e["length"]  # Kilometers
        density_value = e["density"]
        admin = resp["admins"][e["end_node"]["admin_index"]]
        country, region = admin["country_code"], admin["state_code"]
        # OSM name for the service road class is "service", whereas Valhalla outputs "service_other"
        road_class = e["road_class"] if e["road_class"] != "service_other" else "service"
        t = e["end_node"]["elapsed_time"]
        t_elapsed_on_way = t - prev_t  # Seconds

        # The elapsed time should be monotonically increasing. If not, this is a bad match and we will skip it
        if t < prev_t:
            print("Skipping b/c time not monotonically increasing {} -> {}".format(prev_t, t))
            return
        # If the elapsed time doesn't increase for some reason, we can't make any measurement here, so we will ignore it
        if t == prev_t:
            # json.dumps([{'lon': b['lon'], 'lat': b['lat'], 'type': b['type'], 'time': i} for i, b in
            #             enumerate(reversed(body['shape']))])
            continue

        kph = way_length / t_elapsed_on_way * 3600

        # Skip measurements that are going too fast
        if kph > MAXIMUM_SPEED:
            print("Skipping b/c kph of {} > limit of {}".format(kph, MAXIMUM_SPEED))
            return

        # Ordered tuple that holds all the information that we need to classify this edge, as well as the speed
        # calculated. See aggregation.MAP_MATCH_COLS for the meaning of each column
        edge_data = (classify_density(density_value), road_class, get_type_for_edge(e), kph)
        add_data_to_map_matches(map_matches, country, region, edge_data)

        prev_t = t


def write_map_matches(
    map_matches_dir: str, map_matches: dict[str, dict[str, list[tuple]]]
) -> None:
    """
    Writes the results from map_matches to disk. Follows a format where each config is located in a dir corresponding to
    the the iso3166-1 spec, and has a filename corresponding to the iso3166-2 spec.
    """

    for country, regions in map_matches.items():
        country_dir = os.path.join(map_matches_dir, country)
        # Make the dir if it does not exist yet
        if not os.path.exists(country_dir):
            try:
                os.mkdir(country_dir)
            except Exception as e:
                print(
                    "WARNING: Received exception while trying to mkdir {}, assuming it already exists...: {}".format(
                        country_dir, repr(e)
                    )
                )
        for region, rows in regions.items():
            region_filename = util.get_map_match_region_filename_with_identifier(
                country_dir, region
            )
            print(
                "Creating region file of len = {} under {}...".format(
                    len(rows), region_filename
                )
            )
            pickle.dump(rows, open(region_filename, "wb"))


def get_type_for_edge(edge: any) -> str:
    """
    For a Valhalla edge object, determine the 'type' of it, which is used in the final config.
    """
    # 4 special uses
    uses = {
        "driveway": "driveway",
        "alley": "alley",
        "parking_aisle": "parking_aisle",
        "drive_through": "drive-through",
    }
    if edge["use"] in uses:
        return uses[edge["use"]]

    # Roundabout
    if "roundabout" in edge and edge["roundabout"]:
        return "roundabout"

    # Links
    if edge["use"] in ["ramp", "turn_channel"]:
        if "sign" in edge and len(edge["sign"]):
            return "link_exiting"
        else:
            return "link_turning"

    return "way"


def classify_density(density: float) -> str:
    """
    Given the density value from a Valhalla edge, determine which density it corresponds with, out of the options in
    DENSITY_CLASSIFICATIONS. Currently assumes that DENSITY_CLASSIFICATIONS has 3 elements ordered from lease dense to
    most.
    """
    if density < 5:
        return DENSITY_CLASSIFICATIONS[0]
    elif density < 11:
        return DENSITY_CLASSIFICATIONS[1]
    else:
        return DENSITY_CLASSIFICATIONS[2]


def add_data_to_map_matches(
    map_matches: dict[str, dict[str, list[tuple]]], country: str, region: str, data: tuple
) -> None:
    """
    Helper function to add a new measurement to the existing map_matches dict. Follows a country -> region -> [speeds]
    hierarchy.
    """
    if country not in map_matches:
        map_matches[country] = {}
    if region not in map_matches[country]:
        map_matches[country][region] = [data]
    else:
        map_matches[country][region].append(data)


def has_too_many_unmatched(matched_points: list[any]) -> bool:
    """
    Checks over the matched points and returns True if there are too many unmatched points, which means we should simply
    scrap this sequence.
    """
    num_unmatched = sum([1 if mp["type"] == "unmatched" else 0 for mp in matched_points])
    return num_unmatched / len(matched_points) > MAXIMUM_UNMATCHED_PERCENTAGE
