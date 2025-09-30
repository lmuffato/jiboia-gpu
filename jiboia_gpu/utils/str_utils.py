def combine_regex(regex_patterns: list[dict[str, str]]) -> str:
    """
    Combina uma lista de padrões de expressão regular em uma única string.

    Percorre uma lista de dicionários, extrai o valor de cada chave "regex" e os une
    com o caractere '|' (OR) para criar uma única regex combinada.

    Args:
        regex_patterns (list[dict[str, str]]): Uma lista de dicionários, onde cada
            dicionário contém um padrão de regex sob a chave "regex".
            Exemplo: [{"regex": r'padrao1', "pattern": "desc1"}, {"regex": r'padrao2', "pattern": "desc2"}]

    Returns:
        str: Uma única string de regex combinada, com os padrões separados por '|'.
    """
    regex_pattern: str = [pattern["regex"] for pattern in regex_patterns]
    return '|'.join(regex_pattern)
