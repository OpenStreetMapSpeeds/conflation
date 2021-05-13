import json

import requests

TRACE_ROUTE_URL = 'http://localhost:8002/trace_attributes'

MAXIMUM_UNMATCHED_PERCENTAGE = 0.25  # If more than 25% of points are unmatched, skip this sequence


def map_match(shape: any) -> None:
    body = {
        'shape': shape,
        'costing': 'auto',
        'shape_match': 'map_snap'
    }

    print(repr(body))

    resp = requests.post(TRACE_ROUTE_URL, data=json.dumps(body))
    resp = resp.json()
    # print(resp)

    if is_bad_map_match(resp['matched_points']):
        return

    prev_t = 0
    for e in resp['edges']:
        way_length = e['length']
        admin = resp['admins'][e['end_node']['admin_index']]
        co, st = admin['country_code'], admin['state_code']
        t = e['end_node']['elapsed_time']  # TODO: Do some fancy math if this is the first or last edge
        t_elapsed_on_way = t - prev_t

        print(way_length, co, st, prev_t, t, t_elapsed_on_way)
        print(e)
        print(e['end_node'])

        prev_t = t
        # break

    # for l in resp['trip']['legs']:
    #     print(l['shape'])


def is_bad_map_match(matched_points: list[any]) -> bool:
    """
    Checks over the matched points and returns True if there are too many unmatched points, which means we should simply
    scrap this sequence.
    """
    num_unmatched = sum([1 if mp['type'] == 'unmatched' else 0 for mp in matched_points])
    # print(num_unmatched, len(matched_points), num_unmatched / len(matched_points))
    return num_unmatched / len(matched_points) > MAXIMUM_UNMATCHED_PERCENTAGE
