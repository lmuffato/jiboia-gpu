regex_pattern_boolean_raw: list[dict[str, str]] = [
    {
        "regex": r'^(?:True|False)$',
        "pattern": (True, False),
        "format": ("True", "False")
    },
    {
        "regex": r'^(?:true|false)$',
        "pattern": (True, False),
        "format": ("true", "false")
    },
    {
        "regex": r'^(?:TRUE|FALSE)$',
        "pattern": (True, False),
        "format": ("TRUE", "FALSE")
    },
    {
        "regex": r'^(?:yes|no)$',
        "pattern": (True, False),
        "format": ("yes", "no")
    },
    {
        "regex": r'^(?:Yes|No)$',
        "pattern": (True, False),
        "format": ("Yes", "No")
    },
    {
        "regex": r'^(?:YES|NO)$',
        "pattern": (True, False),
        "format": ("YES", "NO")
    },
    {
        "regex": r'^(?:y|n)$',
        "pattern": (True, False),
        "format": ("y", "n")
    },
    {
        "regex": r'^(?:Y|N)$',
        "pattern": (True, False),
        "format": ("Y", "N")
    },
    {
        "regex": r'^(?:on|off)$',
        "pattern": (True, False),
        "format": ("on", "off")
    },
    {
        "regex": r'^(?:On|Off)$',
        "pattern": (True, False),
        "format": ("On", "Off")
    },
    {
        "regex": r'^(?:ON|OFF)$',
        "pattern": (True, False),
        "format": ("ON", "OFF")
    },
    {
        "regex": r'^(?:t|f)$',
        "pattern": (True, False),
        "format": ("t", "f")
    },
    {
        "regex": r'^(?:T|F)$',
        "pattern": (True, False),
        "format": ("T", "F")
    }
]


regex_pattern_boolean_numeric_raw: list[dict[str, str]] = [
    {
        "regex": r'^1$',
        "pattern": True,
        "format": "1"
    },
    {
        "regex": r'^0$',
        "pattern": False,
        "format": "0"
    }
]


regex_pattern_boolean: list[dict[str, any]] = [
    {
        "regex": r'^(?i:true|yes|y|on|t)$',
        "pattern": True,
        "format": ("true", "yes", "y", "on", "t")
    },
    {
        "regex": r'^(?i:false|no|n|off|f)$',
        "pattern": False,
        "format": ("false", "no", "n", "off", "f")
    }
]
