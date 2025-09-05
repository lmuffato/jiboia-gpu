DATE_REGEX_PATTERNS: list[tuple] = [
    # Formatos com barra
    (r'^\d{2}/\d{2}/\d{4}$', '%d/%m/%Y'),      # 01/02/2024
    (r'^\d{2}/\d{2}/\d{2}$', '%d/%m/%y'),      # 01/02/24
    (r'^\d{4}/\d{2}/\d{2}$', '%Y/%m/%d'),      # 2024/02/01
    (r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', '%d/%m/%Y %H:%M'),  # 01/02/2024 15:30
    (r'^\d{1,2}/\d{1,2}/\d{4}$', '%d/%m/%Y'),   # 1/1/2024
    (r'^\d{1,2}/\d{1,2}/\d{2}$', '%d/%m/%y'),   # 1/1/24

    # Formatos com hífen
    (r'^\d{2}-\d{2}-\d{4}$', '%d-%m-%Y'),      # 01-02-2024
    (r'^\d{2}-\d{2}-\d{2}$', '%d-%m-%y'),      # 01-02-24
    (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d'),      # 2024-02-01
    (r'^\d{5}-\d{2}-\d{2}$', '%Y-%m-%d'),      # 02024-02-01

    # Formatos com ponto
    (r'^\d{2}\.\d{2}\.\d{4}$', '%d.%m.%Y'),    # 01.02.2024
    (r'^\d{4}\.\d{2}\.\d{2}$', '%Y.%m.%d'),    # 2024.02.01

    # Numérico compacto
    (r'^\d{8}$', '%Y%m%d'),                    # 20240201

    # Formatos com nome de mês em inglês
    (r'^\d{2} [A-Za-z]{3} \d{4}$', '%d %b %Y'),    # 01 Feb 2024
    (r'^\d{2} [A-Za-z]+ \d{4}$', '%d %B %Y'),      # 01 February 2024
    (r'^[A-Za-z]{3} \d{2}, \d{4}$', '%b %d, %Y'),  # Feb 01, 2024
    (r'^[A-Za-z]+ \d{2}, \d{4}$', '%B %d, %Y'),    # February 01, 2024,
    (r'^\d{4} [A-Za-z]{3} \d{2}$', '%Y %b %d'),    # 2024 Feb 01
    (r'^\d{4} [A-Za-z]+ \d{2}$', '%Y %B %d'),      # 2024 February 01
]


TIME_REGEX_PATTERNS: list[tuple] = [
    (r'^\d{2}:\d{2}$', '%H:%M'),                # 23:45
    (r'^\d{2}:\d{2}:\d{2}$', '%H:%M:%S'),       # 23:45:12
    (r'^\d{2}:\d{2}:\d{2}\.\d+$', '%H:%M:%S.%f'), # 23:45:12.123456

    (r'^\d{1,2}:\d{2}\s?[AaPp][Mm]$', '%I:%M %p'),         # 3:45 PM
    (r'^\d{1,2}:\d{2}:\d{2}\s?[AaPp][Mm]$', '%I:%M:%S %p'), # 03:45:12 PM
    (r'^\d{1,2}:\d{2}:\d{2}\.\d+\s?[AaPp][Mm]$', '%I:%M:%S.%f %p'), # 3:45:12.123456 PM

    (r'^\d{4}\sUTC$', '%H%M UTC'),
]

DATE_TIME_REGEX_PATTERNS: list[tuple] = [
    # yyyy-mm-dd hh:mm:ss.sss
    (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', '%Y-%m-%d %H:%M:%S'),

    # yyyy-mm-dd hh:mm:ss.sss
    (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{1,6}$', '%Y-%m-%d %H:%M:%S.%f'),
]

RAW_INVALID_VALUES: list[str] = [
    '',
    ' ',
    'NA',
    '<NA>',
    '(NA)',
    'N/A',
    'nan',
    'NaT',
    'NULL',
    '(NULL)',
    'NONE',
    'NULO',
    'NAN',
    'UNDEFINED',
    'None',
    'NAO INFORMADO',
    'NÃO INFORMADO',
    'null',
    '(null)',
    'undefined',
    'NOT AVAILABLE',
    'NOT APPLICABLE',
    'MISSING',
    'UNKNOWN',
    '-',
    '--',
    '?',
    'N.D.',
]

RAW_BOOLEAN_VALUES: list[tuple] = [
    ("True", "False"),       # Strings capitalizadas
    ("true", "false"),       # Strings minúsculas
    ("TRUE", "FALSE"),       # Strings maiúsculas
    ("yes", "no"),           # Configs / respostas curtas
    ("Yes", "No"),           # Capitalizadas
    ("YES", "NO"),           # Maiúsculas
    ("y", "n"),              # Abreviações minúsculas
    ("Y", "N"),              # Abreviações maiúsculas
    ("on", "off"),           # Configs de sistemas minúsculas
    ("On", "Off"),           # Capitalizadas
    ("ON", "OFF"),           # Maiúsculas
    ("1", "0"),              # Strings numéricas
    ("t", "f"),              # Abreviações PostgreSQL minúsculas
    ("T", "F")               # Abreviações PostgreSQL maiúsculas
]

SIGNED_INTEGER_TYPES: tuple[str] = (
    'int8',
    'int16',
    'int32',
    'int64'
)

UNSIGNED_INTEGER_TYPES: tuple[str] = (
    'uint32',
    'uint64'
)

FLOATING_TYPES: tuple[str] = (
    'float32',
    'float64'
)

NUMERIC_TYPES: tuple[str] = (
    'int8',
    'int16',
    'int32',
    'int64',
    'uint32',
    'uint64'
    'float32',
    'float64'
)


class RegexPatternsCombine:
    @staticmethod
    def combine_regex_patterns(
        date_regex_patterns: list[tuple[str, str]]
    ) -> str:
        """
        Recebe uma lista de tuplas (regex, formato) e retorna uma string única
        que une todos os regex com o operador OR (|), encapsulando cada regex
        em parênteses para evitar problemas de precedência.

        Exemplo de saída:
        r'(^\d{2}/\d{2}/\d{4}$)|(^\d{4}-\d{2}-\d{2}$)|...'
        """
        # Extrair os padrões regex da lista
        regex_patterns: list[str] = [pattern for pattern, _ in date_regex_patterns]

        grouped_patterns: list[str] = [f"({pattern})" for pattern in regex_patterns]

        combined_regex: str = "|".join(grouped_patterns)
        
        print(type(combined_regex))
        
        return combined_regex
