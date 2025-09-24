regex_pattern_time_hh: list[dict[str, str]] = [
    {
        "regex": r'^(?:[01]\d|2[0-3])$',
        "pattern": "HH",
        "format": "%H"
    }
]

regex_pattern_time_hh_mm: list[dict[str, str]] = [
    {
        "regex": r'^(?:[01]\d|2[0-3]):[0-5]\d$',
        "pattern": "HH:MM",
        "format": "%H:%M"
    },
]

regex_pattern_time_hh_mm_ss: list[dict[str, str]] = [
    {
        "regex": r'^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?$',
        "pattern": "HH:MM:SS(.s+)?",
        "format": "%H:%M:%S.%f"
    },
]

regex_pattern_time_hh_mm_ss_n: list[dict[str, str]] = [
    {
        "regex": r'^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?$',
        "pattern": "HH:MM:SS(.s+)?",
        "format": "%H:%M:%S.%f"
    },
]


regex_pattern_time_amp_pm: list[dict[str, str]] = [
    {
        "regex": r'^(?:0?[1-9]|1[0-2]):[0-5]\d$',
        "pattern": "h:MM AM/PM",
        "format": "%I:%M %p"
    },
    {
        "regex": r'^(?:0?[1-9]|1[0-2]):[0-5]\d:[0-5]\d$',
        "pattern": "h:MM:SS AM/PM",
        "format": "%I:%M:%S %p"
    },
    {
        "regex": r'^(?:0?[1-9]|1[0-2]):[0-5]\d:[0-5]\d(?:\.\d{1,9})?\s*(?:[Aa]\.?[Mm]\.?|[Pp]\.?[Mm]\.?)$',
        "pattern": "h:MM:SS(.s+)? AM/PM",
        "format": "%I:%M:%S.%f %p"
    },
    {
        "regex": r'^(?:0?[1-9]|1[0-2])\s*(?:[Aa]\.?[Mm]\.?|[Pp]\.?[Mm]\.?)$',
        "pattern": "h AM/PM",
        "format": "%I %p"
    },
]

regex_pattern_time_utc: list[dict[str, str]] = [
    {
        "regex": r'^\d{4}\s*UTC$',
        "pattern": "HHMM UTC",
        "format": "%H%M UTC"
    }
]


regex_pattern_timedelta: list[dict[str, str]] = [
    # Timedelta com fração de segundos
    {
        "regex": r'^\d+ days \d{2}:\d{2}:\d{2}\.\d+$',
        "pattern": "D days HH:MM:SS.sss",
        "format": "%d days %H:%M:%S.%f"
    },
    # Timedelta sem fração de segundos
    {
        "regex": r'^\d+ days \d{2}:\d{2}:\d{2}$',
        "pattern": "D days HH:MM:SS",
        "format": "%d days %H:%M:%S"
    },
]
