"""
This list holds all of the z5 tiles that are routable in OSM (i.e. if a z5 tile is NOT in this list, that means there
are no routable roads in the entire tile).
"""
ROUTABLE_Z5_TILES = {
    (0, 6),
    (0, 7),
    (0, 8),
    (0, 9),
    (0, 10),
    (0, 13),
    (0, 14),
    (0, 16),
    (0, 17),
    (0, 18),
    (0, 20),
    (0, 29),
    (0, 30),
    (0, 31),
    (1, 6),
    (1, 7),
    (1, 8),
    (1, 9),
    (1, 10),
    (1, 13),
    (1, 14),
    (1, 15),
    (1, 16),
    (1, 17),
    (1, 18),
    (1, 31),
    (2, 6),
    (2, 7),
    (2, 8),
    (2, 9),
    (2, 10),
    (2, 14),
    (2, 15),
    (2, 16),
    (2, 17),
    (2, 18),
    (3, 7),
    (3, 8),
    (3, 9),
    (3, 16),
    (3, 17),
    (3, 18),
    (4, 6),
    (4, 7),
    (4, 8),
    (4, 9),
    (4, 10),
    (4, 11),
    (4, 12),
    (4, 18),
    (5, 5),
    (5, 6),
    (5, 7),
    (5, 8),
    (5, 9),
    (5, 10),
    (5, 11),
    (5, 12),
    (5, 13),
    (5, 14),
    (6, 4),
    (6, 7),
    (6, 8),
    (6, 9),
    (6, 10),
    (6, 11),
    (6, 12),
    (6, 13),
    (6, 14),
    (6, 18),
    (6, 25),
    (6, 26),
    (7, 5),
    (7, 7),
    (7, 8),
    (7, 9),
    (7, 10),
    (7, 11),
    (7, 12),
    (7, 13),
    (7, 14),
    (7, 16),
    (7, 25),
    (7, 26),
    (8, 3),
    (8, 5),
    (8, 6),
    (8, 7),
    (8, 8),
    (8, 9),
    (8, 10),
    (8, 11),
    (8, 12),
    (8, 13),
    (8, 14),
    (8, 15),
    (8, 16),
    (8, 18),
    (8, 19),
    (8, 28),
    (9, 4),
    (9, 5),
    (9, 6),
    (9, 7),
    (9, 8),
    (9, 9),
    (9, 10),
    (9, 11),
    (9, 12),
    (9, 13),
    (9, 14),
    (9, 15),
    (9, 16),
    (9, 17),
    (9, 18),
    (9, 19),
    (9, 20),
    (9, 21),
    (9, 22),
    (9, 24),
    (9, 25),
    (10, 1),
    (10, 2),
    (10, 4),
    (10, 5),
    (10, 6),
    (10, 7),
    (10, 8),
    (10, 9),
    (10, 10),
    (10, 11),
    (10, 12),
    (10, 14),
    (10, 15),
    (10, 16),
    (10, 17),
    (10, 18),
    (10, 19),
    (10, 20),
    (10, 21),
    (10, 22),
    (10, 23),
    (11, 6),
    (11, 7),
    (11, 8),
    (11, 9),
    (11, 10),
    (11, 11),
    (11, 15),
    (11, 16),
    (11, 17),
    (11, 18),
    (11, 19),
    (12, 6),
    (12, 8),
    (12, 9),
    (12, 16),
    (12, 17),
    (12, 18),
    (12, 21),
    (13, 1),
    (13, 5),
    (13, 6),
    (13, 7),
    (13, 8),
    (13, 12),
    (13, 14),
    (13, 15),
    (13, 16),
    (13, 17),
    (14, 6),
    (14, 7),
    (14, 8),
    (14, 12),
    (14, 13),
    (14, 14),
    (14, 15),
    (14, 16),
    (14, 19),
    (15, 6),
    (15, 8),
    (15, 9),
    (15, 10),
    (15, 11),
    (15, 12),
    (15, 13),
    (15, 14),
    (15, 15),
    (15, 17),
    (15, 19),
    (15, 25),
    (16, 8),
    (16, 9),
    (16, 10),
    (16, 11),
    (16, 12),
    (16, 13),
    (16, 14),
    (16, 15),
    (16, 16),
    (16, 19),
    (16, 25),
    (16, 31),
    (17, 4),
    (17, 5),
    (17, 6),
    (17, 7),
    (17, 8),
    (17, 9),
    (17, 10),
    (17, 11),
    (17, 12),
    (17, 13),
    (17, 14),
    (17, 15),
    (17, 16),
    (17, 17),
    (17, 18),
    (17, 19),
    (17, 25),
    (18, 5),
    (18, 6),
    (18, 7),
    (18, 8),
    (18, 9),
    (18, 10),
    (18, 11),
    (18, 12),
    (18, 13),
    (18, 14),
    (18, 15),
    (18, 16),
    (18, 17),
    (18, 18),
    (18, 19),
    (19, 7),
    (19, 8),
    (19, 9),
    (19, 10),
    (19, 11),
    (19, 12),
    (19, 13),
    (19, 14),
    (19, 15),
    (19, 16),
    (19, 17),
    (19, 18),
    (19, 20),
    (19, 24),
    (20, 3),
    (20, 6),
    (20, 7),
    (20, 8),
    (20, 9),
    (20, 10),
    (20, 11),
    (20, 12),
    (20, 13),
    (20, 14),
    (20, 15),
    (20, 16),
    (20, 17),
    (20, 18),
    (20, 20),
    (21, 6),
    (21, 7),
    (21, 8),
    (21, 9),
    (21, 10),
    (21, 11),
    (21, 12),
    (21, 13),
    (21, 14),
    (21, 16),
    (21, 17),
    (21, 24),
    (21, 25),
    (21, 26),
    (22, 4),
    (22, 5),
    (22, 6),
    (22, 7),
    (22, 8),
    (22, 9),
    (22, 10),
    (22, 11),
    (22, 12),
    (22, 13),
    (22, 14),
    (22, 15),
    (22, 16),
    (22, 19),
    (22, 21),
    (22, 24),
    (22, 25),
    (23, 5),
    (23, 6),
    (23, 7),
    (23, 8),
    (23, 9),
    (23, 10),
    (23, 11),
    (23, 12),
    (23, 13),
    (23, 14),
    (23, 15),
    (24, 3),
    (24, 6),
    (24, 7),
    (24, 8),
    (24, 9),
    (24, 10),
    (24, 11),
    (24, 12),
    (24, 13),
    (24, 14),
    (24, 15),
    (24, 16),
    (24, 17),
    (24, 23),
    (24, 24),
    (24, 25),
    (24, 26),
    (25, 4),
    (25, 5),
    (25, 6),
    (25, 7),
    (25, 8),
    (25, 9),
    (25, 10),
    (25, 11),
    (25, 12),
    (25, 13),
    (25, 14),
    (25, 15),
    (25, 16),
    (25, 23),
    (25, 26),
    (25, 27),
    (26, 5),
    (26, 6),
    (26, 7),
    (26, 8),
    (26, 9),
    (26, 10),
    (26, 11),
    (26, 12),
    (26, 13),
    (26, 14),
    (26, 15),
    (26, 16),
    (26, 17),
    (26, 18),
    (26, 19),
    (26, 26),
    (27, 6),
    (27, 7),
    (27, 8),
    (27, 9),
    (27, 10),
    (27, 11),
    (27, 12),
    (27, 13),
    (27, 14),
    (27, 15),
    (27, 16),
    (27, 17),
    (27, 18),
    (27, 19),
    (27, 24),
    (27, 25),
    (27, 26),
    (28, 5),
    (28, 6),
    (28, 7),
    (28, 8),
    (28, 9),
    (28, 10),
    (28, 11),
    (28, 12),
    (28, 13),
    (28, 14),
    (28, 15),
    (28, 16),
    (28, 17),
    (28, 18),
    (28, 19),
    (28, 20),
    (28, 23),
    (28, 24),
    (29, 6),
    (29, 7),
    (29, 8),
    (29, 9),
    (29, 10),
    (29, 11),
    (29, 13),
    (29, 15),
    (29, 16),
    (29, 17),
    (29, 18),
    (29, 19),
    (29, 20),
    (30, 7),
    (30, 8),
    (30, 9),
    (30, 10),
    (30, 14),
    (30, 15),
    (30, 16),
    (30, 17),
    (30, 18),
    (30, 20),
    (30, 21),
    (30, 26),
    (30, 27),
    (31, 7),
    (31, 8),
    (31, 9),
    (31, 10),
    (31, 14),
    (31, 15),
    (31, 16),
    (31, 17),
    (31, 19),
    (31, 20),
    (31, 21),
    (31, 27),
    (31, 28),
    (31, 29),
}