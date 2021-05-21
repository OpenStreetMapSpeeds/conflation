import json

import requests

TRACE_ROUTE_URL = "http://localhost:8002/trace_attributes"

MAXIMUM_UNMATCHED_PERCENTAGE = (
    0.25  # If more than 25% of points are unmatched, skip this sequence
)
DENSITY_CLASSIFICATIONS = [
    "rural",
    "suburban",
    "urban",
]  # The different density classifications we can give to roads
ROAD_CLASS_MAP = {}


def run(output_dir: str, processes: int) -> None:
    pass


def map_match(shape: any) -> None:
    body = {"shape": shape, "costing": "auto", "shape_match": "map_snap"}

    print(repr(body))

    resp = requests.post(TRACE_ROUTE_URL, data=json.dumps(body))
    resp = resp.json()
    # print(resp)

    result = {}

    if has_too_many_unmatched(resp["matched_points"]):
        return

    prev_t = resp["edges"][0]["end_node"]["elapsed_time"]
    for e in resp["edges"][
        1:-1
    ]:  # TODO: Figure out the funky math for the first and last edges
        # print(e)
        way_length = e["length"]  # Kilometers
        density_value = e["density"]
        admin = resp["admins"][e["end_node"]["admin_index"]]
        country, region = admin["country_code"], admin["state_code"]
        t = e["end_node"][
            "elapsed_time"
        ]  # TODO: Do some fancy math if this is the first or last edge
        t_elapsed_on_way = t - prev_t  # Seconds

        # The elapsed time should be monotonically increasing. If not, this is a bad match and we will skip it
        if t < prev_t:
            return
        # If the elapsed time doesn't increase for some reason, we can't make any measurement here, so we will ignore it
        if t == prev_t:
            # json.dumps([{'lon': b['lon'], 'lat': b['lat'], 'type': b['type'], 'time': i} for i, b in
            #             enumerate(reversed(body['shape']))])
            continue

        kph = way_length / t_elapsed_on_way * 3600
        add_trace_to_result(result, country, region, classify_density(density_value), kph)
        # print('MAP MATCH: ###')
        # print(way_length, co, st, prev_t, t, t_elapsed_on_way)
        # print(e)
        # print(e['end_node'])

        prev_t = t
        # break
    print("MAP MATCH RESULT: {}".format(result))

    # TODO: Next step: directories grouped by country, files grouped by region, files will be .pickles of DFs where each
    #  row is a per-edge measurement

    # for l in resp['trip']['legs']:
    #     print(l['shape'])


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


def add_trace_to_result(
    result: any, country: str, region: str, density: str, speed: str
) -> any:
    if country not in result:
        result[country] = {}
    if region not in result[country]:
        result[country][region] = {}
    if density not in result[country][region]:
        result[country][region][density] = [speed]
    else:
        result[country][region][density].append(speed)
    return result


def has_too_many_unmatched(matched_points: list[any]) -> bool:
    """
    Checks over the matched points and returns True if there are too many unmatched points, which means we should simply
    scrap this sequence.
    """
    num_unmatched = sum([1 if mp["type"] == "unmatched" else 0 for mp in matched_points])
    # print(num_unmatched, len(matched_points), num_unmatched / len(matched_points))
    return num_unmatched / len(matched_points) > MAXIMUM_UNMATCHED_PERCENTAGE
