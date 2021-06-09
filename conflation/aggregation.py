import copy
import json
import os
import pandas as pd
import pickle

from mapillary import util

MAP_MATCH_COLS = ["density", "road_class", "type", "kph"]
BASE_CONFIG = {
    "rural": {
        "way": [None, None, None, None, None, None, None, None],
        "link_exiting": [None, None, None, None, None],
        "link_turning": [None, None, None, None, None],
        "roundabout": [None, None, None, None, None, None, None, None],
        "driveway": None,
        "alley": None,
        "parking_aisle": None,
        "drive-through": None,
    },
    "suburban": {
        "way": [None, None, None, None, None, None, None, None],
        "link_exiting": [None, None, None, None, None],
        "link_turning": [None, None, None, None, None],
        "roundabout": [None, None, None, None, None, None, None, None],
        "driveway": None,
        "alley": None,
        "parking_aisle": None,
        "drive-through": None,
    },
    "urban": {
        "way": [None, None, None, None, None, None, None, None],
        "link_exiting": [None, None, None, None, None],
        "link_turning": [None, None, None, None, None],
        "roundabout": [None, None, None, None, None, None, None, None],
        "driveway": None,
        "alley": None,
        "parking_aisle": None,
        "drive-through": None,
    },
}
ROAD_CLASS_INDEX_MAPPING = {
    "motorway": 0,
    "trunk": 1,
    "primary": 2,
    "secondary": 3,
    "tertiary": 4,
    "unclassified": 5,
    "residential": 6,
    "service_other": 7,
}


def run(map_matches_dir: str, results_dir: str) -> None:
    # Check to see if the final result has already been processed
    final_config_filename = util.get_final_config_filename(results_dir)
    if os.path.exists(final_config_filename):
        print("Final config already built. Skipping...")
        return

    final_config = []
    for subdir, dirs, files in os.walk(map_matches_dir):
        for file in files:
            # Pull the country and region using the name of the subdir and the pickle file
            country = os.path.basename(os.path.normpath(subdir))
            region = file.split(".")[0]

            # Pull map matches from disk
            map_matches_filename = os.path.join(subdir, file)
            try:
                print("Reading {}/{} map match results from disk...".format(country, region))
                map_matches: dict[str, dict[str, list[tuple]]] = pickle.load(
                    open(map_matches_filename, "rb")
                )
            except (OSError, IOError):
                print("ERROR: {} pickle could not be loaded. Cannot perform aggregation.")
                continue

            df = pd.DataFrame(map_matches, columns=MAP_MATCH_COLS)

            # TODO: For now we're just doing a simple median over the data. Do we need something fancier?
            final_config.append(
                measurements_to_config(
                    df.groupby(MAP_MATCH_COLS[:-1]).median(), country, region
                )
            )

    with open(final_config_filename, "w") as f:
        f.write(json.dumps(final_config, indent=4))


def measurements_to_config(df: pd.DataFrame, country: str, region: str) -> dict:
    """
    This function builds the final ETA estimates config that is defined in the OpenStreetMapSpeeds/schema repo. It takes
    the basic config from BASE_CONFIG and fills in any data that it can gather from df. The country and region can be
    optionally specified. See the .README of the OpenStreetMapSpeeds/schema repo for more details.

    :param df: DataFrame of the results, where each series has a key of (density, road_class, type) and a value of just
        the speed measurement in kph
    :param country: Optional iso3166-1, can pass in None
    :param region: Optional iso3166-2, can pass in None
    :return: The config in JSON format as defined in the OpenStreetMapSpeeds/schema repo
    """

    config: dict = copy.deepcopy(BASE_CONFIG)

    if country:
        config["iso3166-1"] = country
    if region:
        config["iso3166-2"] = region

    for idx, kph in df.iterrows():
        density, road_class, type_ = idx
        kph = kph[0]

        # Process all the different types
        if type_ in ["way", "roundabout"]:
            config[density][type_][ROAD_CLASS_INDEX_MAPPING[road_class]] = kph
        elif type_.startswith("link_") and ROAD_CLASS_INDEX_MAPPING[road_class] < 5:
            config[density][type_][ROAD_CLASS_INDEX_MAPPING[road_class]] = kph
        elif type_ in ["driveway", "alley", "parking_aisle", "drive-through"]:
            config[density][type_] = kph
        else:
            print("WARNING: type {} not supported".format(type))

    return config
