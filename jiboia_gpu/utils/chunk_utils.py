from typing import Callable, Generator
import cudf
import cupy as cp
import functools


def combine_regex(regex_patterns: list[dict[str, str]]) -> str:
    regex_pattern: str = [pattern["regex"] for pattern in regex_patterns]
    return '|'.join(regex_pattern)


def chunk_df(chunk_size: int = 500_000):
    """
    Decorador para processar um DataFrame em chunks.
    Modifica inlplace
    
    Divide o dataframe em pedaços e aplica a função decorada a cada um, otimizando o 
    processamento de grandes DataFrames, dividindo-os em
    pedaços menores e aplicando a função decorada a cada um.

    Modo de usar:
    1. Como decorador, na declaração da função:
       @chunk_df(chunk_size=100_000)
       def funcao(dataframe: cudf.DataFrame, ...):
           # Lógica da função, que será aplicada a cada chunk
           ...
           return dataframe

    2. Na chamada de uma função já declarada:
       def funcao(dataframe: cudf.DataFrame, ...):
           # Lógica da função
           ...
           return dataframe

       funcao_decorada = chunk_df(chunk_size=100_000)(funcao)
       funcao_decorada(df, ...)

    Parâmetros:
    - chunk_size (int): O tamanho de cada pedaço de DataFrame a ser processado.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(dataframe: cudf.DataFrame, column_name: str, *args, **kwargs):
            if len(dataframe) <= chunk_size:
                return func(dataframe, column_name, *args, **kwargs)

            final_dtype: str = None

            for start in range(0, len(dataframe), chunk_size):
                end = min(start + chunk_size, len(dataframe))
                chunk = dataframe.iloc[start:end]

                processed_chunk = func(chunk, column_name, *args, **kwargs)

                if final_dtype is None:
                    final_dtype = processed_chunk[column_name].dtype

                dataframe.iloc[start:end] = processed_chunk
            dataframe[column_name] = dataframe[column_name].astype(final_dtype)
            return dataframe
        return wrapper
    return decorator


def chunk_iterate(
    series: cudf.Series, 
    chunk_size: int = 500_000
) -> Generator[cudf.Series, None, None]:
    total_rows: int = len(series)
    for start_index in range(0, total_rows, chunk_size):
        end_index: int = min(start_index + chunk_size, total_rows)
        yield series.iloc[start_index:end_index]


def chunk_iterate_index(
    dataframe: cudf.DataFrame,
    column_name: str,
    chunk_size: int = 500_000
) -> Generator[cp.ndarray, None, None]:
    """
    Itera sobre uma Series retornando blocos de índices de tamanho `chunk_size`.

    Args:
        dataframe (cudf.DataFrame): O DataFrame cudf.
        column_name (str): Nome da coluna.
        chunk_size (int): O tamanho do bloco.

    Yields:
        cp.ndarray: Array de índices do bloco atual.
    """
    total_rows = len(dataframe[column_name])
    
    for start_index in range(0, total_rows, chunk_size):
        end_index = min(start_index + chunk_size, total_rows)
        yield cp.arange(start_index, end_index)


def get_index_samples(
    series: cudf.Series,
    n_parts: int = 100,
    n_samples: int = 10
) -> cp.ndarray:
    series_size = len(series)

    if ((n_parts * n_samples) >= series_size):
        raise ValueError("The total number of samples requested exceeds or equals the series size. Please provide a smaller value for n_parts or n_samples.")
    
    if (series_size // n_parts == 0):
        raise ValueError("The number of parts is greater than the series size. Please provide a smaller value for n_parts.")

    # Passo entre as partes
    step_pass = series_size // n_parts
    
    # Índices iniciais de cada bloco (ex: 0, 1000, 2000, ...)
    start_indices = cp.arange(n_parts) * step_pass
    
    # Offsets dentro de cada bloco (ex: 0, 1, 2, ... n_samples-1)
    sample_offsets = cp.arange(n_samples)

    all_indices = (start_indices[:, None] + sample_offsets).flatten()
    
    # Garante que os índices não ultrapassem o tamanho da Series
    all_indices = all_indices[all_indices < series_size]

    return all_indices


def series_samples(
    series: cudf.Series, 
    n_parts: int = 100,
    n_samples: int = 10
) -> None|cudf.Series:

    index_samples: cp.ndarray = get_index_samples(
        series=series,
        n_parts=n_parts,
        n_samples=n_samples
    )

    return series.iloc[index_samples]
