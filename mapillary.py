import time
from dateutil import parser
import requests

SEQUENCES_PER_PAGE_DEFAULT = 50  # How many sequences to receive on each page of the API call
SKIP_IF_FEWER_IMAGES_THAN_DEFAULT = 5  # We will skip any sequences if they have fewer than this number of images
MAX_FILES_IN_DIR = 500  # Maximum number of files we will put in one directory
SEQUENCE_URL = 'https://a.mapillary.com/v3/sequences_without_images?client_id={}&bbox={}&per_page={}'
IMAGES_URL = 'https://a.mapillary.com/v3/images?client_id={}&sequence_keys={}'


def to_bbox(llo, lla, mlo, mla):
    return ','.join([str(llo), str(lla), str(mlo), str(mla)])


def get_trace_data_for_bbox(session: requests.Session, bbox: str, conf: any) -> dict[
    str, list]:
    result, elapsed = {}, 0
    # try:
    start = time.time()

    map_client_id = conf['mcid']  # The Mapillary client ID, mandatory key of conf

    # Check to see if user specified any overrides in conf JSON
    seq_per_page = conf['sequences_per_page'] if 'sequences_per_page' in conf else SEQUENCES_PER_PAGE_DEFAULT
    skip_if_fewer_imgs_than = conf[
        'skip_if_fewer_images_than'] if 'skip_if_fewer_images_than' in conf else SKIP_IF_FEWER_IMAGES_THAN_DEFAULT

    print('Getting seq for bbox={}'.format(bbox))
    next_url = SEQUENCE_URL.format(map_client_id, bbox, seq_per_page)
    page = 1
    while next_url:
        print('Page {}, url={}'.format(page, next_url))
        sequence_resp = session.get(next_url)
        sequence_keys = []
        for seq_f in sequence_resp.json()['features']:
            # Skip sequences that have too few images
            if len(seq_f['geometry']['coordinates']) >= skip_if_fewer_imgs_than:
                sequence_keys.append(seq_f['properties']['key'])
        # TODO: Paginate images
        images_resp = session.get(IMAGES_URL.format(map_client_id, ','.join(sequence_keys)))
        for img_f in images_resp.json()['features']:
            if img_f['properties']['sequence_key'] not in result:
                result[img_f['properties']['sequence_key']] = []
            result[img_f['properties']['sequence_key']].append((
                parser.isoparse(img_f['properties']['captured_at']).timestamp(),  # Epoch time
                img_f['geometry']['coordinates']
            ))

        # Check if there is a next page or if we are finished with this bbox
        next_url = sequence_resp.links['next']['url'] if 'next' in sequence_resp.links else None
        page += 1

    stop = time.time()
    elapsed = stop - start
    print('\n\n##################\nFinished processing seqs for bbox={}, elapsed time: {}'.format(bbox, elapsed))
    print('{}\n##################\n\n'.format(result))
    # with response_count.get_lock():
    #     response_count.value += 1
    # except Exception as e:
    #     print(e)

    return result
