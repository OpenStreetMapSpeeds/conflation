import unittest
from conflation import aggregation


class TestAggregationInterpExtrap(unittest.TestCase):
    def test_interp_extrap(self):
        test_case = {
            "iso3166-1": "FR",
            "rural": {
                "way": [None, None, 55, 45, None, 30, None, None],
                "link_exiting": [72, 67, None, 57, 53],
                "link_turning": [None, 73, 43, 41, None],
                "roundabout": [40, 31, 25, 24, None, None, None, None],
                "driveway": 16,
                "alley": 12,
                "parking_aisle": 40,
                "drive-through": 15,
            },
            "suburban": {
                "way": [90, None, None, None, None, None, None, 13],
                "link_exiting": [None, None, None, None, 45],
                "link_turning": [50, 62, None, 35, 30],
                "roundabout": [37, 32, 27, 20, 19, None, None, None],
                "driveway": 16,
                "alley": 10,
                "parking_aisle": 31,
                "drive-through": 10,
            },
            "urban": {
                "way": [None, None, None, None, None, 20, 15, 10],
                "link_exiting": [None, None, 23, 59, None],
                "link_turning": [None, 43, 32, 21, 19],
                "roundabout": [32, 27, 22, 17, 16, 16, 14, None],
                "driveway": None,
                "alley": None,
                "parking_aisle": None,
                "drive-through": None,
            },
        }
        expected = {
            "iso3166-1": "FR",
            "rural": {
                "way": [75, 65, 55, 45, 38, 30, 22, 14],
                "link_exiting": [72, 67, 62, 57, 53],
                "link_turning": [103, 73, 43, 41, 39],
                "roundabout": [40, 31, 25, 24, 23, 22, 21, 20],
                "driveway": 16,
                "alley": 12,
                "parking_aisle": 40,
                "drive-through": 15,
            },
            "suburban": {
                "way": [90, 79, 68, 57, 46, 35, 24, 13],
                "link_exiting": [
                    None,
                    None,
                    None,
                    None,
                    45,
                ],  # Should skip because not enough data points
                "link_turning": [
                    50,
                    62,
                    None,
                    35,
                    30,
                ],  # Should skip because not monotonically decreasing
                "roundabout": [37, 32, 27, 20, 19, 18, 17, 16],
                "driveway": 16,
                "alley": 10,
                "parking_aisle": 31,
                "drive-through": 10,
            },
            "urban": {
                "way": [45, 40, 35, 30, 25, 20, 15, 10],
                "link_exiting": [
                    None,
                    None,
                    23,
                    59,
                    None,
                ],  # Should skip because not monotonically decreasing
                "link_turning": [54, 43, 32, 21, 19],
                "roundabout": [32, 27, 22, 17, 16, 16, 14, 12],
                "driveway": None,
                "alley": None,
                "parking_aisle": None,
                "drive-through": None,
            },
        }
        self.assertEqual(aggregation.perform_interp_extrap(test_case), expected)


if __name__ == "__main__":
    unittest.main()
