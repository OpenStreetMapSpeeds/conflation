import copy
import json
import logging
import os
import pandas as pd
import pickle
import re
from typing import Optional

from conflation import util

MAP_MATCH_COLS = ["density", "road_class", "type", "kph"]
# Basic config from OpenStreetMapSpeeds/schema repo where all values are None.
BASE_CONFIG = {
    "iso3166-1": None,
    "iso3166-2": None,
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
    "service": 7,
}


def run(map_matches_dir: str, results_dir: str) -> None:
    """
    This is the final step of the script, where the map matching results from step two are aggregated together to build
    the config.json that acts as the output of the script. The config.json follows the definition from the
    OpenStreetMapSpeeds/schema repo.

    It does a walk through the output dirs of the map matching process. Each file in these dirs holds a list of tuples
    that contain each individual measurement we made during map matching (kind of like database tables). The data is
    aggregated together with a group_by using pandas.

    :param map_matches_dir: Dir where map match results from step 2 were pickled to
    :param results_dir: Dir where the final config.json should be stored
    """
    # Check to see if the final result has already been processed
    final_config_filename = util.get_final_config_filename(results_dir)
    if os.path.exists(final_config_filename):
        logging.info("Final config already built. Skipping...")
        return

    final_config = []
    for subdir, dirs, files in os.walk(map_matches_dir):  # Iterating over countries
        # Pull the country using the name of the subdir
        country = os.path.basename(os.path.normpath(subdir))
        regions = {}
        country_level_data = []

        for file in files:  # Iterating over regions
            # Pull the region using the pickle filename
            region = file.split(".")[0].split(util.MAP_MATCH_REGION_FILENAME_DELIMITER)[0]

            # Pull map matches from disk
            map_match_data_filename = os.path.join(subdir, file)
            try:
                logging.info(
                    "Reading {}/{} map match results from file {}".format(
                        country, region, map_match_data_filename
                    )
                )
                map_match_data: list[tuple] = pickle.load(open(map_match_data_filename, "rb"))
            except (OSError, IOError):
                logging.critical("{} pickle could not be loaded. Cannot perform aggregation.")
                continue

            # Combine the data with other data from the same region. Sometimes the region isn't detected and it's just
            # an empty string. In this case, don't add it to the regions dict but add it to the overall country data
            if region:
                if region not in regions:
                    regions[region] = map_match_data
                else:
                    regions[region].extend(map_match_data)

            # Aggregate country level statistics
            country_level_data.extend(map_match_data)

        for region, data in regions.items():
            df = pd.DataFrame(data, columns=MAP_MATCH_COLS)

            # TODO: For now we're just doing a simple median over the data. Do we need something fancier?
            final_config.append(
                measurements_to_config(
                    df.groupby(MAP_MATCH_COLS[:-1]).median(), country, region
                )
            )

        if len(country_level_data):
            # Aggregate country level statistics
            df = pd.DataFrame(country_level_data, columns=MAP_MATCH_COLS)

            final_config.append(
                measurements_to_config(df.groupby(MAP_MATCH_COLS[:-1]).median(), country, None)
            )

    # TODO: Do the same aggregation at the world level. Probably need to implement another pandas DF that holds data
    #   for each country, then at the end (here) we can do a weighted group by

    if len(final_config) == 0:  # No data from map match, assume that something went wrong
        return

    # Dump the config dict to a string, then run some regex to make the format more concise.
    final_config_str = json.dumps(final_config)
    p = re.compile('("rural|"suburban|"urban|"iso3166)')
    final_config_str = p.sub(os.linesep + r"    \1", final_config_str)
    p = re.compile('("way|"link|"round|"driveway)')
    final_config_str = p.sub(os.linesep + r"      \1", final_config_str)
    p = re.compile(", {")
    final_config_str = p.sub(r"," + os.linesep + "  {", final_config_str)
    p = re.compile("\\[{")
    final_config_str = p.sub(r"[" + os.linesep + "  {", final_config_str)
    p = re.compile("}]")
    final_config_str = p.sub(r"}" + os.linesep + "]", final_config_str)

    with open(final_config_filename, "w") as f:
        f.write(final_config_str)


def measurements_to_config(
    df: pd.DataFrame, country: Optional[str], principal_subdivision: Optional[str]
) -> dict:
    """
    This function builds the final ETA estimates config that is defined in the OpenStreetMapSpeeds/schema repo. It takes
    the basic config from BASE_CONFIG and fills in any data that it can gather from df. The country and
    principal_subdivision can be optionally specified. See the .README of the OpenStreetMapSpeeds/schema repo for more
    details.

    :param df: DataFrame of the results, where each series has a key of (density, road_class, type) and a value of just
        the speed measurement in kph
    :param country: Optional iso3166-1, can pass in None
    :param principal_subdivision: Optional iso3166-2, can pass in None
    :return: The config in JSON format as defined in the OpenStreetMapSpeeds/schema repo
    """

    config: dict = copy.deepcopy(BASE_CONFIG)

    if country:
        config["iso3166-1"] = country
    else:
        del config["iso3166-1"]
    if principal_subdivision:
        config["iso3166-2"] = principal_subdivision
    else:
        del config["iso3166-2"]

    for idx, kph in df.iterrows():
        density, road_class, type_ = idx
        # Round the kph to the nearest whole number, any sig figs is just noise.
        kph = round(kph[0])

        # Process all the different types
        if type_ in ["way", "roundabout"]:
            config[density][type_][ROAD_CLASS_INDEX_MAPPING[road_class]] = kph
        elif type_.startswith("link_") and ROAD_CLASS_INDEX_MAPPING[road_class] < 5:
            config[density][type_][ROAD_CLASS_INDEX_MAPPING[road_class]] = kph
        elif type_ in ["driveway", "alley", "parking_aisle", "drive-through"]:
            config[density][type_] = kph
        else:
            logging.warning("Type {} not supported".format(type))

    return config
