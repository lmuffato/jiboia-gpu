regex_pattern_date: list[dict[str, str]] = [
    {
        "regex": r'^(?:\d{1,2}[^\w\d]\d{1,2}[^\w\d]\d{4})$',
        "pattern": "dd?mm?yyyy",
        "format": "%d-%m-%Y"
    },
    {
        "regex": r'^(?:\d{4}[^\w\d]\d{1,2}[^\w\d]\d{1,2})$',
        "pattern": "yyyy?mm?dd",
        "format": "%Y-%m-%d"
    },
    {
        "regex": r'^(?:\d{1,2}[^\w\d]\d{1,2}[^\w\d]\d{2})$',
        "pattern": "dd?mm?yy",
        "format": "%d-%m-%y"
    },
    {
        "regex": r'^(?:\d{4}(?:0[1-9]|1[0-2])(?:0[1-9]|[1-2][0-9]|3[0-1]))$',
        "pattern": "yyyymmdd",
        "format": "%Y%m%d"
    }
]


regex_pattern_bad_date: list[dict[str, str]] = [
    {
        "regex": r'^(?:\d{1}[^\w\d]\d{1}[^\w\d]\d{2})$',
        "pattern": "d?m?yy",
        "format": "%d-%m-%y"
    },
    {
        "regex": r'^(?:\d{1}[^\w\d]\d{2}[^\w\d]\d{2})$',
        "pattern": "d?mm?yy",
        "format": "%d-%m-%y"
    },
    {
        "regex": r'^(?:\d{2}[^\w\d]\d{1}[^\w\d]\d{2})$',
        "pattern": "dd?m?yy",
        "format": "%d-%m-%y"
    }
]

regex_pattern_datetime_fraction: list[dict[str, str]] = [
    {
        "regex": r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$',
        "pattern": "YYYY-MM-DD HH:MM:SS.sss",
        "format": "%Y-%m-%d %H:%M:%S.%f"
    }
]

regex_pattern_datetime: list[dict[str, str]] = [
    {
        "regex": r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
        "pattern": "YYYY-MM-DD HH:MM:SS",
        "format": "%Y-%m-%d %H:%M:%S"
    },
]



regex_pattern_datetime_all: list[dict[str, str]] = [
    # Espaço + fração de segundos
    {
        "regex": r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$',
        "pattern": "YYYY-MM-DD HH:MM:SS.sss",
        "format": "%Y-%m-%d %H:%M:%S.%f"
    },
    # Espaço sem fração de segundos
    {
        "regex": r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
        "pattern": "YYYY-MM-DD HH:MM:SS",
        "format": "%Y-%m-%d %H:%M:%S"
    },
    # T + fração de segundos
    {
        "regex": r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$',
        "pattern": "YYYY-MM-DDTHH:MM:SS.sss",
        "format": "%Y-%m-%dT%H:%M:%S.%f"
    },
    # T sem fração de segundos
    {
        "regex": r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',
        "pattern": "YYYY-MM-DDTHH:MM:SS",
        "format": "%Y-%m-%dT%H:%M:%S"
    },
    # Espaço + timezone
    {
        "regex": r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$',
        "pattern": "YYYY-MM-DD HH:MM:SS±HH:MM",
        "format": "%Y-%m-%d %H:%M:%S%z"
    },
    # T + timezone
    {
        "regex": r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$',
        "pattern": "YYYY-MM-DDTHH:MM:SS±HH:MM",
        "format": "%Y-%m-%dT%H:%M:%S%z"
    },
    # Espaço + fração + timezone
    {
        "regex": r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+[+-]\d{2}:\d{2}$',
        "pattern": "YYYY-MM-DD HH:MM:SS.sss±HH:MM",
        "format": "%Y-%m-%d %H:%M:%S.%f%z"
    },
    # T + fração + timezone
    {
        "regex": r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+-]\d{2}:\d{2}$',
        "pattern": "YYYY-MM-DDTHH:MM:SS.sss±HH:MM",
        "format": "%Y-%m-%dT%H:%M:%S.%f%z"
    }
]

# TODO: implement normalization with months in text
regex_pattern_month_name: list[dict[str, str]] = [
    # Mês abreviado, formato DD-MMM-YYYY
    {
        "regex": r'^\d{1,2}-([A-Za-z]{3})-\d{4}$',
        "pattern": "DD-MMM-YYYY",
        "format": "%d-%b-%Y"
    },
    # Mês abreviado, formato MMM DD, YYYY
    {
        "regex": r'^([A-Za-z]{3}) \d{1,2}, \d{4}$',
        "pattern": "MMM DD, YYYY",
        "format": "%b %d, %Y"
    },
    # Mês abreviado, formato DD MMM YYYY
    {
        "regex": r'^\d{1,2} ([A-Za-z]{3}) \d{4}$',
        "pattern": "DD MMM YYYY",
        "format": "%d %b %Y"
    },
    # Mês por extenso, formato DD-MMMM-YYYY
    {
        "regex": r'^\d{1,2}-([A-Za-z]+)-\d{4}$',
        "pattern": "DD-MMMM-YYYY",
        "format": "%d-%B-%Y"
    },
    # Mês por extenso, formato MMMM DD, YYYY
    {
        "regex": r'^([A-Za-z]+) \d{1,2}, \d{4}$',
        "pattern": "MMMM DD, YYYY",
        "format": "%B %d, %Y"
    },
    # Mês por extenso, formato DD MMMM YYYY
    {
        "regex": r'^\d{1,2} ([A-Za-z]+) \d{4}$',
        "pattern": "DD MMMM YYYY",
        "format": "%d %B %Y"
    },
    # Mês abreviado + hora, DD-MMM-YYYY HH:MM:SS
    {
        "regex": r'^\d{1,2}-([A-Za-z]{3})-\d{4} \d{2}:\d{2}:\d{2}$',
        "pattern": "DD-MMM-YYYY HH:MM:SS",
        "format": "%d-%b-%Y %H:%M:%S"
    },
    # Mês por extenso + hora, DD-MMMM-YYYY HH:MM:SS
    {
        "regex": r'^\d{1,2}-([A-Za-z]+)-\d{4} \d{2}:\d{2}:\d{2}$',
        "pattern": "DD-MMMM-YYYY HH:MM:SS",
        "format": "%d-%B-%Y %H:%M:%S"
    },
    # Mês abreviado + fração de segundos, DD-MMM-YYYY HH:MM:SS.sss
    {
        "regex": r'^\d{1,2}-([A-Za-z]{3})-\d{4} \d{2}:\d{2}:\d{2}\.\d+$',
        "pattern": "DD-MMM-YYYY HH:MM:SS.sss",
        "format": "%d-%b-%Y %H:%M:%S.%f"
    },
    # Mês por extenso + fração de segundos, DD-MMMM-YYYY HH:MM:SS.sss
    {
        "regex": r'^\d{1,2}-([A-Za-z]+)-\d{4} \d{2}:\d{2}:\d{2}\.\d+$',
        "pattern": "DD-MMMM-YYYY HH:MM:SS.sss",
        "format": "%d-%B-%Y %H:%M:%S.%f"
    }
]
