regex_pattern_bad_formatted_number: list[dict[str, str]] = [
    {"regex": r'^[-+]?\d+(?:\.\d{3})*,\d+$', "pattern": "d.ddd,dddd"},
    {"regex": r'^[-+]?\d*,\d+$', "pattern": "dddd,dddd"},
]

regex_pattern_valid_number: list[dict[str, str]] = [
    {"regex": r'^\d+$', "pattern": "d"},
    {"regex": r'^\d+\.\d+$', "pattern": "d.d"},
    {"regex": r'^\.\d+$', "pattern": ".d"},
    {"regex": r'^[+-]?(?:\d+|\d+\.\d+|\.\d+)[eE][+-]?\d+$', "pattern": "scientific"},
]

regex_pattern_list: list[dict[str, str]] = [
    {"regex": r'[\[\]]', "pattern": "[...]"},
]
