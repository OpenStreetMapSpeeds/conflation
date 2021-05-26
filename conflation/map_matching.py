import json
import os
import pickle
import requests
from conflation import util

TRACE_ROUTE_URL = "http://localhost:8002/trace_attributes"

MAXIMUM_UNMATCHED_PERCENTAGE = (
    0.25  # If more than 25% of points are unmatched, skip this sequence
)
DENSITY_CLASSIFICATIONS = [  # The different density classifications we can give to roads
    "rural",
    "suburban",
    "urban",
]


def run(traces_dir: str, map_matches_dir: str, processes: int) -> None:
    sections_filename = util.get_sections_filename(traces_dir)

    try:
        print("Reading bbox_sections from disk...")
        bbox_sections: list[tuple[str, str]] = pickle.load(open(sections_filename, "rb"))
    except (OSError, IOError):
        raise FileNotFoundError(
            "bbox sections pickle not found in output folder. Cannot perform map matching."
        )

    # TODO: Multiprocess this section
    for bbox_str, result_filename in bbox_sections:
        try:
            trace_data: list[list[dict]] = pickle.load(open(result_filename, "rb"))
        except (OSError, IOError):
            raise FileNotFoundError(
                "Trace data {} not found in output folder. Skipping...".format(result_filename)
            )
        for traces in trace_data:
            results = map_match(traces)
            if len(results) == 0:
                continue

            # Next step: directories grouped by country, files grouped by region, files will be .pickles of lists where
            # each row is a per-edge measurement
            write_results(map_matches_dir, results)


def write_results(map_matches_dir: str, results: dict[str, dict[str, list[tuple]]]):

    for country, regions in results.items():
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
                print("Creating region .pickle file for {}/{}...".format(country, region))
                pickle.dump(new_rows, open(region_filename, "wb"))


def map_match(shape: any) -> dict[str, dict[str, list[tuple]]]:
    body = {"shape": shape, "costing": "auto", "shape_match": "map_snap"}

    # print(repr(body))

    resp = requests.post(TRACE_ROUTE_URL, data=json.dumps(body))
    resp = resp.json()
    # print(resp)

    results = {}

    if has_too_many_unmatched(resp["matched_points"]):
        print("Skipping b/c too many points unmatched")
        return {}

    prev_t = resp["edges"][0]["end_node"]["elapsed_time"]
    # TODO: Figure out the funky math for the first and last edges
    for e in resp["edges"][1:-1]:
        way_length = e["length"]  # Kilometers
        density_value = e["density"]
        admin = resp["admins"][e["end_node"]["admin_index"]]
        country, region = admin["country_code"], admin["state_code"]
        road_class = e["road_class"]
        is_roundabout = "roundabout" in e
        t = e["end_node"]["elapsed_time"]
        t_elapsed_on_way = t - prev_t  # Seconds

        # The elapsed time should be monotonically increasing. If not, this is a bad match and we will skip it
        if t < prev_t:
            print("Skipping b/c time not monotonically increasing {} -> {}".format(prev_t, t))
            return {}
        # If the elapsed time doesn't increase for some reason, we can't make any measurement here, so we will ignore it
        if t == prev_t:
            # json.dumps([{'lon': b['lon'], 'lat': b['lat'], 'type': b['type'], 'time': i} for i, b in
            #             enumerate(reversed(body['shape']))])
            continue

        kph = way_length / t_elapsed_on_way * 3600
        # Ordered tuple that holds all the information that we need to classify this edge, as well as the speed
        # calculated TODO: Add a few more cols here depending on what we need
        edge_data = (classify_density(density_value), road_class, is_roundabout, kph)
        add_trace_to_result(results, country, region, edge_data)

        prev_t = t

    return results


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


def add_trace_to_result(results: any, country: str, region: str, data: tuple) -> any:
    if country not in results:
        results[country] = {}
    if region not in results[country]:
        results[country][region] = [data]
    else:
        results[country][region].append(data)
    return results


def has_too_many_unmatched(matched_points: list[any]) -> bool:
    """
    Checks over the matched points and returns True if there are too many unmatched points, which means we should simply
    scrap this sequence.
    """
    num_unmatched = sum([1 if mp["type"] == "unmatched" else 0 for mp in matched_points])
    return num_unmatched / len(matched_points) > MAXIMUM_UNMATCHED_PERCENTAGE
