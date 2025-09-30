
from ..boolean.boolean_utils import BooleanUtils
from ..null.null_utils import NullUtils
from ..number.number_utils import NumberUtils
from ..string.string_utils import StringUtils
from ..time.time_utils import TimeUtils
from ..datetime.datetime_utils import DateTimeUtils
from typing import Literal
import cudf
import cupy as cp
import pandas as pd
from ..utils.log_utils import (
    print_drop_column_log,
    print_text_green,
    print_text_yellow,
    print_normalize_df_space_log,
    print_normalize_df_string_log
)


class DfUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        match_min_rate: int=50,
        null_values: list[str] = [],
        to_case: None|Literal['lower', 'upper']=None,
        to_ASCII: bool=False,
        bool_number: bool=False,
        create_category: bool=False,
        drop_columns: list[str]=[],
        inplace: None|bool=False,
        show_log: None|bool=True,
        chunk_size: int=500_000,
    ) -> bool|cudf.DataFrame:
        """
        Normaliza todas as colunas de um DataFrame cuDF aplicando múltiplas
        transformações automáticas em valores de string, nulos, numéricos, booleanos,
        temporais e categóricos.

        Operações aplicadas por coluna, nesta ordem:
        1. StringUtils.normalize -> normalização de espaços múltiplos e strings (case e ASCII).
        2. NullUtils.normalize -> padronização e substituição de valores nulos.
        3. NumberUtils.normalize -> conversão de strings numéricas para menor tipo numérico possível.
        4. BooleanUtils.normalize -> conversão de strings booleanas para tipo boolean.
        5. TimeUtils.normalize -> normalização e parsing de valores de tempo (HH:MM:SS).
        6. DateTimeUtils.normalize -> normalização e parsing de valores datetime.
        7. StringUtils.to_category (opcional) -> conversão da coluna em categoria ordenada.

        Parâmetros
        ----------
        dataframe : cudf.DataFrame
            DataFrame de entrada a ser normalizado.
        match_min_rate : int, default=50
            Percentual mínimo de correspondência de valores válidos para converter
            a coluna em um tipo específico (0–100).
        null_values : list[str], default=[]
            Lista de valores considerados nulos além dos padrões (ex.: ["NA", "null"]).
        to_case : {'lower', 'upper'} | None, default=None
            Se definido, converte todos os textos para minúsculo ou maiúsculo.
        to_ASCII : bool, default=False
            Se True, converte caracteres acentuados para ASCII puro.
        create_category : bool, default=False
            Se True, converte colunas de texto em categorias ordenadas com base nos valores únicos.
        drop_columns : list[str], default=[]
            Lista de colunas a serem removidas do DataFrame antes da normalização.
        inplace : bool, default=False
            Se True, altera o DataFrame original. Caso contrário, retorna uma cópia.
        show_log : bool, default=True
            Se True, imprime logs de normalização para cada etapa.
        chunk_size : int, default=500_000
            Tamanho dos chunks usados para processar séries grandes sem estourar memória.

        Retorna
        -------
        bool | cudf.DataFrame
            Se `inplace=True`, retorna True.  
            Se `inplace=False`, retorna um novo DataFrame normalizado.

        Notas
        -----
        - A ordem de aplicação é fixa e pode impactar o resultado (ex.: strings
          numéricas são convertidas para números antes de categoria).
        - Colunas inválidas ou incompatíveis em cada etapa são ignoradas silenciosamente.
        """

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        if drop_columns:
            DfUtils.drop_columns(
                dataframe=dataframe,
                drop_columns=drop_columns,
                inplace=True,
                show_log=show_log
            )

        column_names: list[str] = dataframe.columns

        for column_name in column_names:
            StringUtils.normalize(
                dataframe=dataframe,
                column_name=column_name,
                to_case=to_case,
                to_ASCII=to_ASCII,
                inplace=True,
                show_log=False
            )
   
            NullUtils.normalize(
                dataframe=dataframe,
                column_name=column_name,
                null_values=null_values,
                inplace=True,
                show_log=False,
                chunk_size=chunk_size
            )
        print_normalize_df_space_log(
            show_log=show_log
        )
        print_normalize_df_string_log(
            to_case=to_case,
            to_ASCII=to_ASCII,
            show_log=show_log
        )


        for column_name in column_names:

            NumberUtils.normalize(
                dataframe=dataframe,
                column_name=column_name,
                match_min_rate=match_min_rate,
                inplace=True,
                chunk_size=chunk_size,
                show_log=show_log
            )

            BooleanUtils.normalize(
                dataframe=dataframe,
                bool_number=bool_number,
                column_name=column_name,
                match_min_rate=match_min_rate,
                inplace=True,
                show_log=show_log
            )

            TimeUtils.normalize(
                dataframe=dataframe,
                column_name=column_name,
                match_min_rate=match_min_rate,
                inplace=True,
                show_log=show_log
            )

            DateTimeUtils.normalize(
                dataframe=dataframe,
                column_name=column_name,
                match_min_rate=match_min_rate,
                inplace=True,
                chunk_size=chunk_size,
                show_log=show_log
            )

            if create_category:
                if create_category:
                    StringUtils.to_category(
                        dataframe=dataframe,
                        column_name=column_name,
                        inplace=True,
                        chunk_size=chunk_size,
                        show_log=show_log
                    )

        if not inplace:
            return dataframe

        return True



    @staticmethod
    def drop_columns(
        dataframe: cudf.DataFrame,
        drop_columns: list[str],
        inplace: bool=False,
        show_log: bool=True,
    ) -> bool|cudf.DataFrame:

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        columns_to_delete: list = []

        for column_name in drop_columns:

            if column_name in dataframe.columns:
                columns_to_delete.append(column_name)

        if len(columns_to_delete) >= 1:

            dataframe.drop(
                columns=columns_to_delete,
                inplace=True
            )
            print_drop_column_log(
                show_log=show_log,
                columns_to_delete=columns_to_delete
            )

        if not inplace:
            return dataframe

        return True



    @staticmethod
    def cudf_size_info(dataframe: cudf.DataFrame, print_info: bool = False) -> None:

        rows: int = dataframe.shape[0]
        columns: int =  dataframe.shape[1]
        vram_size_mb: float = round(dataframe.memory_usage(index=True, deep=True).sum() / (1024 * 1024), 2)

        cudf_info: dict[str, any] = {
            "rows": rows,
            "columns": columns,
            "VRAM size Mb": vram_size_mb
        }

        if print_info:
            print(
                print_text_green("Done!"),
                "rows:",
                print_text_yellow(rows),
                "columns:",
                print_text_yellow(columns),
                "VRAM size Mb:",
                print_text_yellow(vram_size_mb),
            )
                
        return cudf_info


    @staticmethod
    def get_gpu_memory_info(device_id: int = 0) -> dict[str, int]:
        free_bytes, total_bytes = cp.cuda.runtime.memGetInfo()
        return {
            "free_mb": round(free_bytes / (1024 * 1024), 2),
            "total": round(total_bytes / (1024 * 1024), 2),
            "used_mb":  round((total_bytes - free_bytes) / (1024 * 1024), 2),
        }
    
    @staticmethod
    def is_vram_use_limit(device_id: int = 0) -> dict[str, int]:
        free_bytes, total_bytes = cp.cuda.runtime.memGetInfo()
        vram_percent_in_use: float = round(((total_bytes - free_bytes) / total_bytes) * 100, 1) >= 90
        
        if vram_percent_in_use >= 90:
            return True
        
        return False
    

    @staticmethod
    def df_size_info(dataframe: pd.DataFrame, print_info: bool = False) -> None:
        rows: int = dataframe.shape[0]
        columns: int =  dataframe.shape[1]
        ram_size_mb: float = round(dataframe.memory_usage(index=True, deep=True).sum() / (1024 * 1024), 2)

        df_info: dict[str, any] = {
            "rows": rows,
            "columns": columns,
            "RAM MBb": ram_size_mb
        }

        if print_info:
            print(
                print_text_green("Done!"),
                "rows:",
                print_text_yellow(rows),
                "columns:",
                print_text_yellow(columns),
                "RAM size Mb:",
                print_text_yellow(ram_size_mb),
            )
                
        return df_info


    @staticmethod
    def frequency(dataframe, column_name) -> cudf.DataFrame:
        """
        Retorna um dataframe com a frequência de cada velor único em uma coluna.

        Args:
            dataframe (pd.DataFrame): O DataFrame de entrada.
            coluna (str): O nome da coluna para calcular a frequência.

        Returns:
            pd.DataFrame: Um DataFrame com a frequência de cada valor na coluna
                        em ordem do maior para o menor.
        """
        frequencia = dataframe[column_name].value_counts()

        df_frequencia = frequencia.reset_index()

        df_frequencia.columns = [column_name, 'frequency']