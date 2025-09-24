import cudf
import cupy as cp
import pandas as pd
from .chunk_utils import chunk_iterate
from typing import Generator
import functools
from typing import Callable


def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


def print_job_drop_column_done(
    columns_to_delete: list[str]
) -> None:
    colored_names: list[str] = [
        print_text_yellow(name) for name in columns_to_delete
    ]
    
    msg: str = ", ".join(colored_names)

    print(
        print_text_green("Done!"),
        "column",
        msg,
        "was",
        print_text_red("dropped")
    )


# def print_job_action_done(
#     columns: list[str],
#     action: list[str]
# ) -> None:
#     colored_names: list[str] = [
#         print_text_yellow(name) for name in columns_to_delete
#     ]

#     # if len(columns)
    
#     msg: str = ", ".join(colored_names)

#     print(
#         print_text_green("Done!"),
#         "column",
#         msg,
#         "was",
#         print_text_red("dropped")
#     )


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



def chunk_iterate_indexs(
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


class AnalysisUtils:
    @staticmethod
    def drop_columns(
        current_df: cudf.DataFrame,
        column_names: list[str],
    ) -> None:

        columns_to_delete: list = []

        for column_name in column_names:

            if column_name in current_df.columns:
                columns_to_delete.append(column_name)


        if len(columns_to_delete) >= 1:

            current_df.drop(
                columns=columns_to_delete,
                inplace=True
            )
            print_job_drop_column_done(columns_to_delete)


    # @staticmethod
    # def df_cudf_size_info(current_df: cudf.DataFrame, print_info: bool = False) -> None:

    #     rows: int = current_df.shape[0]
    #     columns: int =  current_df.shape[1]
    #     vram_size_mb: float = round(current_df.memory_usage(index=True, deep=True).sum() / (1024 * 1024), 2)

    #     cudf_info: dict[str, any] = {
    #         "rows": rows,
    #         "columns": columns,
    #         "VRAM size Mb": vram_size_mb
    #     }

    #     if print_info:
    #         print(
    #             print_text_green("Done!"),
    #             "rows:",
    #             print_text_yellow(rows),
    #             "columns:",
    #             print_text_yellow(columns),
    #             "VRAM size Mb:",
    #             print_text_yellow(vram_size_mb),
    #         )
                
    #     return cudf_info


    # @staticmethod
    # def memory_gpu_info(device_id: int = 0) -> dict[str, int]:
    #     free_bytes, total_bytes = cp.cuda.runtime.memGetInfo()
    #     return {
    #         "free_mb": round(free_bytes / (1024 * 1024), 2),
    #         "total": round(total_bytes / (1024 * 1024), 2),
    #         "used_mb":  round((total_bytes - free_bytes) / (1024 * 1024), 2),
    #     }
    
    # @staticmethod
    # def is_vram_use_limit(device_id: int = 0) -> dict[str, int]:
    #     free_bytes, total_bytes = cp.cuda.runtime.memGetInfo()
    #     vram_percent_in_use: float = round(((total_bytes - free_bytes) / total_bytes) * 100, 1) >= 90
        
    #     if vram_percent_in_use >= 90:
    #         return True

    #     return False
    

    # @staticmethod
    # def df_size_info(current_df: pd.DataFrame) -> None:
    #     return {
    #         "rows": current_df.shape[0],
    #         "columns": current_df.shape[1],
    #         "RAM MB": round(current_df.memory_usage(index=True, deep=True).sum() / (1024 * 1024), 2)
    #     }
    

    # def print_converted_column_type(
    #     column_name: str,
    #     column_type: str
    # ) -> None:
    #     print(
    #         "\033[1;32mDone!\033[0m",
    #         "column",
    #         # f"{column_name}",
    #         f"\033[1;33m{column_name}\033[0m",
    #         "converted to", f"\033[1;33m{column_type}\033[0m"
    #     )
       

    @staticmethod
    def get_index_samples(
        series: cudf.Series,
        n_parts: int = 10,
        n_samples: int = 10
    ) -> list[int]:
        """
        Verifica de forma performática na GPU se algum valor nas amostras de uma Series
        corresponde a um padrão de data.

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
    

    @staticmethod
    def infer_by_sample(
        series: cudf.Series,
        regex_patern: str,
        n_parts: int = 10,
        n_samples: int = 10
    ) -> bool:
        """
        Verifica de forma performática na GPU se algum valor nas amostras de uma Series
        corresponde a um padrão de data.

        Args:
            s (cudf.Series): A Series de strings.
            n_parts (int): O número de partes em que a Series será dividida para amostragem.
            n_samples (int): O número de amostras a serem coletadas de cada parte.

        Returns:
            bool: True se um padrão de data for encontrado nas amostras, False caso contrário.
        """
        series_size = len(series)

        if series_size == 0:
            return False

        # Coluna sem dados
        if series.notna().sum() == 0:
            return False

        if series.dtype not in ["object", "string"]:
            return False

        if ((n_parts * n_samples) >= series_size):
            raise ValueError("The total number of samples requested exceeds or equals the series size. Please provide a smaller value for n_parts or n_samples.")
        
        if (series_size // n_parts == 0):
            raise ValueError("The number of parts is greater than the series size. Please provide a smaller value for n_parts.")

        # Gera todos os índices de amostragem de uma vez na GPU
        step_pass = series_size // n_parts
        
        # Índices iniciais de cada bloco (ex: 0, 1000, 2000, ...)
        start_indices = cp.arange(n_parts) * step_pass
        
        # Offsets dentro de cada bloco (ex: 0, 1, 2, ... n_samples-1)
        sample_offsets = cp.arange(n_samples)

        all_indices = (start_indices[:, None] + sample_offsets).flatten()
        
        # Garante que os índices não ultrapassem o tamanho da Series
        all_indices = all_indices[all_indices < series_size]

        # Seleção de todas as amostras em uma única operação
        samples = series.iloc[all_indices]

        if (samples.str.contains(regex_patern).sum() == samples.notna().sum()):
            return True

        return False



    @staticmethod
    def combine_regex(regex_patterns: list[dict[str, str]]) -> str:
        regex_pattern: str = [pattern["regex"] for pattern in regex_patterns]
        return '|'.join(regex_pattern)


    @staticmethod
    def _chunk_iterate(
        series: cudf.Series, 
        chunk_size: int = 500_000
    ) -> Generator[cudf.Series, None, None]:
        total_rows: int = len(series)
        for start_index in range(0, total_rows, chunk_size):
            end_index: int = min(start_index + chunk_size, total_rows)
            yield series.iloc[start_index:end_index]


    @staticmethod
    def _chunk_iterate_indexs(
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


    @staticmethod
    def match(
        series: cudf.Series,
        regex: str,
        chunk_size: int = 500_000,
        match_min_rate: int = 0
    ) -> bool:
        """
        Retorna true no primeiro padrão encontrado.
        Usa chunk para não estourar a memória em series grandes.
        """
        if match_min_rate > 0 and match_min_rate < 100:
            total_rows: int = len(series)
            match_min: int = total_rows // (match_min_rate*100)
            total_match: int = 0

            for chunk in chunk_iterate(series, chunk_size):
                total_match += chunk.str.match(regex).sum()

                if total_match >= match_min:
                    return True

            return False

        else:        
            for chunk in chunk_iterate(series, chunk_size):
                if chunk.str.match(regex).any():
                    return True

            return False


    @staticmethod
    def match_count(
        series: cudf.Series,
        pattern: str,
        chunk_size: int = 500_000,
        match_min_rate: int = 0
    ) -> int:
        """
        Retorna o número de ocorrências para um padrão.
        Quando preenchido, o match_limit_rate determina uma porcentagem limite
        para parar a busca.
        match_limit_rate: de 1 a 100 (ex: 10 para 10%)
        """
        total_rows: int = len(series)

        total_match: int = 0

        if match_min_rate > 0 and match_min_rate < 100:
            match_min: int = total_rows // (match_min_rate*100)

            for chunk in chunk_iterate(series, chunk_size):
                total_match += chunk.str.match(pattern).sum()

                if total_match >= match_min:
                    return total_match

        else:        
            for chunk in chunk_iterate(series, chunk_size):
                total_match += chunk.str.match(pattern).sum()
                
        return total_match


    @staticmethod
    def match_infer(
        series: cudf.Series,
        regex_patterns: list[dict[str, str]],
        chunk_size: int = 500_000,
    ) -> list[dict[str, str]]:
        """
        Retorna o número de ocorrências para uma lista de padrões.
        """
        # Inicializa a frequência de todos os padrões como zero
        for pattern in regex_patterns:
            pattern["frequency"] = 0

        # Itera sobre os chunks
        for chunk in chunk_iterate(series, chunk_size):
            # Para cada chunk, testa todos os padrões
            for pattern in regex_patterns:
                pattern["frequency"] += chunk.str.match(pattern["regex"]).sum()
                
        return regex_patterns


    # @staticmethod
    # def is_valid_to_normalize(
    #     series: cudf.Series,
    #     valid_types: list[str] = ["object", "string"],
    #     invalid_types: list[str] = [],
    # ) -> bool:
    #     if (
    #         (str(series.dtype) in valid_types)
    #         and (str(series.dtype) not in invalid_types)
    #         and series.size != 0
    #         and series.notna().sum() == series.size
    #     ):
    #         return True
    #     return False
    


    # @staticmethod
    # def get_frequency_by_column(current_df: pd.DataFrame, column: str) -> pd.DataFrame:
    #     '''
    #     Gera um current_df com a frequencia de valores de uma coluna
    #     '''
    #     current_df_frequency: pd.DataFrame = (
    #         current_df[column]
    #         .value_counts()
    #         .reset_index()
    #     )
    
    #     current_df_frequency.columns = [column, 'frequency']
    
    #     current_df_frequency: pd.DataFrame = (
    #         current_df_frequency
    #         .sort_values(
    #             by='frequency',
    #             ascending=False
    #         )
    #     )
    
    #     return current_df_frequency


    # @staticmethod
    # def get_unique_values(current_df: pd.DataFrame, column: str) -> pd.DataFrame:
    #     '''
    #     Retorna um dataframe com os valores únicos de uma coluna
    #     '''
    #     df_unique = pd.DataFrame({
    #         column: current_df[column].dropna().unique()
    #     })

    #     df_unique = df_unique.sort_values(
    #         by=column,
    #         ascending=True
    #     ).reset_index(drop=True)

    #     return df_unique

    # @staticmethod
    # def match_any(
    #     series: cudf.Series,
    #     pattern: str,
    #     chunk_size: int=500_000
    # ) -> bool:
    #     """
    #     Retorna true no primeiro padrão encontrado.
    #     Usa chunk para não estourar a memória em series grandes.
    #     """
    #     total_rows: int = len(series)

    #     for start_index in range(0, total_rows, chunk_size):
    #         end_index: int = min(start_index + chunk_size, total_rows)
    #         chunk: cudf.Series = series.iloc[start_index:end_index]
            
    #         mask: cudf.Series = chunk.str.match(pattern)

    #         if mask.any():
    #             return True

    #     return False


    # @staticmethod
    # def match_count(
    #     series: cudf.Series,
    #     pattern: str,
    #     chunk_size: int=500_000,
    #     match_min_rate: int=0
    # ) -> int:
    #     """
    #     Retorna o número de ocorrências para um padrão.
    #     quando preenchido, o match_min_rate determinar uma pecentagem limite
    #     para parar a busca.
    #     match_min_rate: de 1 a 100
    #     """
    #     total_rows: int = len(series)

    #     total_match: int = 0

    #     min_rows: int = total_rows // match_min_rate

    #     if not match_min_rate or match_min_rate == 0:
    #         for start_index in range(0, total_rows, chunk_size):
    #             end_index: int = min(start_index + chunk_size, total_rows)
    #             chunk: cudf.Series = series.iloc[start_index:end_index]
                
    #             chunk_match: int = chunk.str.match(pattern).sum()

    #             total_match = total_match + chunk_match

    #         return total_match

    #     if match_min_rate and match_min_rate > 1:
    #         for start_index in range(0, total_rows, chunk_size):
    #             end_index: int = min(start_index + chunk_size, total_rows)
    #             chunk: cudf.Series = series.iloc[start_index:end_index]
                
    #             chunk_match: int = chunk.str.match(pattern).sum()

    #             total_match = total_match + chunk_match

    #             if min_rows >= total_match:
    #                 return total_match

    #     return total_match


    # @staticmethod
    # def match_infer(
    #     series: cudf.Series,
    #     regex_patterns: list[dict[str, str]],
    #     chunk_size: int=500_000,
        
    # ) -> list[dict[str, str]]:
    #     """
    #     Retorna o número de ocorrências para um padrão.
    #     quando preenchido, o match_min_rate determinar uma pecentagem limite
    #     para parar a busca.
    #     """
    #     total_rows: int = len(series)

    #     for pattern in regex_patterns:
    #         pattern["frequency"] = 0

    #     for start_index in range(0, total_rows, chunk_size):
    #         end_index: int = min(start_index + chunk_size, total_rows)
    #         chunk: cudf.Series = series.iloc[start_index:end_index]

    #         for pattern in regex_patterns:  
    #             chunk_match: int = chunk.str.match(pattern["regex"]).sum()
    #             print("pattern", pattern["regex"], chunk_match)
        
    #             pattern["frequency"] = pattern["frequency"] + chunk_match

    #     return regex_patterns