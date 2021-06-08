import multiprocessing
import os
import pickle
import requests
from conflation import util

VALHALLA_MAP_MATCHING_URL_EXTENSION = "trace_attributes"

MAXIMUM_UNMATCHED_PERCENTAGE = (
    0.25  # If more than 25% of points are unmatched, skip this sequence
)
DENSITY_CLASSIFICATIONS = [  # The different density classifications we can give to roads
    "rural",
    "suburban",
    "urban",
]


def run(
    traces_dir: str,
    map_matches_dir: str,
    processes: int,
    valhalla_url: str,
    valhalla_headers: dict,
) -> None:
    sections_filename = util.get_sections_filename(traces_dir)

    try:
        print("Reading bbox_sections from disk...")
        bbox_sections: list[tuple[str, str]] = pickle.load(open(sections_filename, "rb"))
    except (OSError, IOError):
        raise FileNotFoundError(
            "bbox sections pickle not found in output folder. Cannot perform map matching."
        )

    finished_bbox_sections = multiprocessing.Value("i", 0)
    with multiprocessing.Pool(
        initializer=initialize_multiprocess,
        initargs=(map_matches_dir, valhalla_url, valhalla_headers, finished_bbox_sections),
        processes=processes,
    ) as pool:
        result = pool.map_async(map_match_for_bbox, bbox_sections)

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


def initialize_multiprocess(
    global_map_matches_dir_: str,
    global_valhalla_url_: str,
    global_valhalla_headers_: dict,
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

    # Valhalla base url and headers
    global global_valhalla_url
    global_valhalla_url = global_valhalla_url_
    global global_valhalla_headers
    global_valhalla_headers = global_valhalla_headers_


def map_match_for_bbox(bbox_section: tuple[str, str]) -> None:
    try:
        bbox, trace_filename = bbox_section
        processed_trace_filename = util.get_processed_trace_filename(trace_filename)

        # Check to see if the trace has already been processed by map_matching.
        if os.path.exists(processed_trace_filename):
            print("Map matching already complete for bbox={}. Skipping...".format(bbox))
            with finished_bbox_sections.get_lock():
                finished_bbox_sections.value += 1
            return

        trace_data: list[list[dict]] = pickle.load(open(trace_filename, "rb"))
        map_matches = {}
        [add_map_matches_for_shape(map_matches, shape) for shape in trace_data]
        if len(map_matches):
            write_map_matches(global_map_matches_dir, map_matches)

        # Once all results have been written, mark the file as processed by renaming.
        os.rename(trace_filename, processed_trace_filename)
        with finished_bbox_sections.get_lock():
            finished_bbox_sections.value += 1
    except Exception as e:
        print("ERROR: Failed to pull trace data: {}".format(repr(e)))


def add_map_matches_for_shape(
    map_matches: dict[str, dict[str, list[tuple]]], shape: any
) -> None:
    body = {"shape": shape, "costing": "auto", "shape_match": "map_snap"}

    # print(repr(body))

    resp = requests.post(
        global_valhalla_url + VALHALLA_MAP_MATCHING_URL_EXTENSION,
        json=body,
        headers=global_valhalla_headers,
    )
    resp = resp.json()
    # print(resp)

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
        road_class = e["road_class"]
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
        # Ordered tuple that holds all the information that we need to classify this edge, as well as the speed
        # calculated.
        edge_data = (classify_density(density_value), road_class, get_type_for_edge(e), kph)
        add_data_to_map_matches(map_matches, country, region, edge_data)

        prev_t = t


def write_map_matches(map_matches_dir: str, map_matches: dict[str, dict[str, list[tuple]]]):
    for country, regions in map_matches.items():
        country_dir = os.path.join(map_matches_dir, country)
        # Make the dir if it does not exist yet
        if not os.path.exists(country_dir):
            os.mkdir(country_dir)
        for region, new_rows in regions.items():
            region_filename = os.path.join(country_dir, region + ".pickle")
            try:
                existing_rows: list[tuple] = pickle.load(open(region_filename, "rb"))
                existing_rows.extend(new_rows)
                print(
                    "Write Results: {}/{} Len: {}".format(country, region, len(existing_rows))
                )
                pickle.dump(existing_rows, open(region_filename, "wb"))
            except (OSError, IOError):
                print(
                    "Creating region .pickle file for {}/{} Len: {}...".format(
                        country, region, len(new_rows)
                    )
                )
                pickle.dump(new_rows, open(region_filename, "wb"))


def get_type_for_edge(edge: any) -> str:
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
    TODO
    :param density: Density value from Valhalla edge response
    :return: One of the values in DENSITY_CLASSIFICATIONS
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
