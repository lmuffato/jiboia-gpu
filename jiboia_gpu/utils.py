import cudf
import cupy as cp
import functools
import pandas as pd
from typing import Callable, ClassVar, Generator


class CudfSupportedDtypes:  
    str_types: ClassVar[list[str]] = ["object", "string"]
    int_types: ClassVar[list[str]] = ["int8", "int16", "int32", "int64"]
    uint_types: ClassVar[list[str]] = ["uint32", "uint64"]
    float_types: ClassVar[list[str]] = ["float32", "float64"]
    decimal_types: ClassVar[list[str]] = ["Decimal32Dtype", "Decimal64Dtype", "Decimal128Dtype"]
    numeric_types: ClassVar[list[str]] = int_types + uint_types + float_types + decimal_types
    bool_types: ClassVar[list[str]] = ["bool", "boolean"]
    datetime_types: ClassVar[list[str]] = ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]
    timedelta_types: ClassVar[list[str]] = ["timedelta64[s]", "timedelta64[ms]", "timedelta64[us]", "timedelta64[ns]"]
    category_types: ClassVar[list[str]] = ["CategoricalDtype"]
    struct_types: ClassVar[list[str]] = ["StructDtype"]


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


def is_valid_to_normalize(
    series: cudf.Series,
    valid_types: list[str] = [],
    invalid_types: list[str] = [],
) -> bool:

    series_type: str = str(series.dtype)

    is_empty: bool = series.isna().sum() == series.size

    if is_empty:
        return False
    
    is_valid_type: bool = True
    is_invalid_type: bool = True

    if valid_types:
        is_valid_type = series_type in valid_types

    if invalid_types:
        is_invalid_type = series_type not in invalid_types
    
    valid_series: bool = is_valid_type & is_invalid_type

    return valid_series


def get_index_samples(
    series: cudf.Series,
    n_parts: int = 10,
    n_samples: int = 10
) -> list[int]:
    """
    Retorna uma lista de indices de um datagrame para amostragem.

    Args:
        s (cudf.Series): A Series de strings que será analisada.
        n_parts (int): O número de partes em que a Series será dividida para amostragem.
        n_samples (int): O número de amostras a serem coletadas de cada parte.

    Returns:
        bool: True se um padrão de data for encontrado nas amostras, False caso contrário.
    """

    series_size = len(series)

    # print(series.name)
    # return

    if ((n_parts * n_samples) >= series_size):
        raise ValueError("The total number of samples requested exceeds or equals the series size. Please provide a smaller value for n_parts or n_samples.")
    
    if (series_size // n_parts == 0):
        raise ValueError("The number of parts is greater than the series size. Please provide a smaller value for n_parts.")

    # Gera todos os índices de amostragem DE UMA VEZ na GPU
    step_pass = series_size // n_parts
    
    # Índices iniciais de cada bloco (ex: 0, 1000, 2000, ...)
    start_indices = cp.arange(n_parts) * step_pass
    
    # Offsets dentro de cada bloco (ex: 0, 1, 2, ... n_samples-1)
    sample_offsets = cp.arange(n_samples)

    all_indices = (start_indices[:, None] + sample_offsets).flatten()
    
    # Garante que os índices não ultrapassem o tamanho da Series
    all_indices = all_indices[all_indices < series_size]

    return all_indices
    

# def check_by_sample(
#     series: cudf.Series,
#     regex_patern: str,
#     n_parts: int = 10,
#     n_samples: int = 10
# ) -> bool:
#     """
#     Verifica se algum valor nas amostras de uma Series corresponde a um padrão.

#     Args:
#         s (cudf.Series): A Series de strings.
#         n_parts (int): O número de partes em que a Series será dividida para amostragem.
#         n_samples (int): O número de amostras a serem coletadas de cada parte.

#     Returns:
#         bool: True se um padrão de data for encontrado nas amostras, False caso contrário.
#     """
#     series_size = len(series)

#     if series_size == 0:
#         return False

#     # Coluna sem dados
#     if series.notna().sum() == 0:
#         return False

#     if series.dtype not in ["object", "string"]:
#         return False

#     if ((n_parts * n_samples) >= series_size):
#         raise ValueError("The total number of samples requested exceeds or equals the series size. Please provide a smaller value for n_parts or n_samples.")
    
#     if (series_size // n_parts == 0):
#         raise ValueError("The number of parts is greater than the series size. Please provide a smaller value for n_parts.")

#     # Gera todos os índices de amostragem de uma vez na GPU
#     step_pass = series_size // n_parts
    
#     # Índices iniciais de cada bloco (ex: 0, 1000, 2000, ...)
#     start_indices = cp.arange(n_parts) * step_pass
    
#     # Offsets dentro de cada bloco (ex: 0, 1, 2, ... n_samples-1)
#     sample_offsets = cp.arange(n_samples)

#     all_indices = (start_indices[:, None] + sample_offsets).flatten()
    
#     # Garante que os índices não ultrapassem o tamanho da Series
#     all_indices = all_indices[all_indices < series_size]

#     # Seleção de todas as amostras em uma única operação
#     samples = series.iloc[all_indices]

#     if (samples.str.contains(regex_patern).sum() == samples.notna().sum()):
#         return True

#     return False


# def match(
#     series: cudf.Series,
#     regex: str,
#     match_min_rate: int = 0
# ) -> bool:
#     """
#     Retorna true no primeiro padrão encontrado.
#     Usa chunk para não estourar a memória em series grandes.
#     """
#     if match_min_rate > 0 and match_min_rate < 100:
#         total_rows: int = len(series)
#         match_min: int = total_rows // (match_min_rate*100)
#         total_match: int = 0

#         total_match += series.str.match(regex).sum()

#         if total_match >= match_min:
#             return True

#         return False

#     else:        
#         if series.str.match(regex).any():
#             return True

#         return False


# def match_count(
#     series: cudf.Series,
#     pattern: str,
#     match_min_rate: int = 0
# ) -> int:
#     """
#     Retorna o número de ocorrências para um padrão.
#     Quando preenchido, o match_limit_rate determina uma porcentagem limite
#     para parar a busca.
#     match_limit_rate: de 1 a 100 (ex: 10 para 10%)
#     """
#     total_rows: int = len(series)

#     total_match: int = 0

#     if match_min_rate > 0 and match_min_rate < 100:
#         match_min: int = total_rows // (match_min_rate*100)

#         total_match += series.str.match(pattern).sum()

#         if total_match >= match_min:
#             return total_match

#     else:        
#         total_match += series.str.match(pattern).sum()
            
#     return total_match


# def match_infer(
#     series: cudf.Series,
#     regex_patterns: list[dict[str, str]],
# ) -> list[dict[str, str]]:
#     """
#     Retorna o número de ocorrências para uma lista de padrões.
#     """
#     # Inicializa a frequência de todos os padrões como zero
#     for pattern in regex_patterns:
#         pattern["frequency"] = 0

#     for pattern in regex_patterns:
#         pattern["frequency"] += series.str.match(pattern["regex"]).sum()
            
#     return regex_patterns
