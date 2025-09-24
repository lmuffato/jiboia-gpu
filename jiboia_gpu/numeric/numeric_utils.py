from ..log_utils import print_log
from ..string.string_utils import StringUtils
from ..utils import combine_regex, is_valid_to_normalize, CudfSupportedDtypes
from .regex_pattern import (
    regex_pattern_bad_formatted_number,
    regex_pattern_valid_number,
    regex_pattern_list
)
import cudf
import cupy as cp
import warnings


class NumericUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        match_min_rate: None|int=50,
        inplace: None|bool=False,
        show_log: None|bool=True
    ) -> bool|cudf.DataFrame:
        """
        Converte uma coluna de um DataFrame cuDF para o tipo numérico mais apropriado.

        - Se a coluna for string/object, tenta convertê-la para numérica.
        - Se a coluna for float, verifica se pode ser representada como inteiro sem perda de dados.
        - Realiza o downcast para o menor tipo de dado possível.

        Args:
            df: O DataFrame cuDF a ser modificado.
            column_name: O nome da coluna a ser normalizada.
            match_min_rate:
                A proporção mínima de valores não nulos que devem ser inteiro de 0 a 100
                numéricos para que uma coluna de string seja convertida (padrão: 0.7).
            inplace: Se True, modifica o DataFrame original. Se False, retorna uma cópia.
            print_info: Se True, mostra a coluna convertida e o tipo convertido.

        Returns:
            O DataFrame modificado se inplace=False, senão None.
        """
        valid_types: list[str] = CudfSupportedDtypes.str_types + CudfSupportedDtypes.numeric_types
        
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=valid_types
        )
        
        if not is_valid:
            return False
        
        is_number: bool = NumericUtils.is_number(
            series=dataframe[column_name],
            match_min_rate=match_min_rate
        )

        if not is_number:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        original_dtype: cp.dtypes = dataframe[column_name].dtype

        if original_dtype in CudfSupportedDtypes.str_types:
            NumericUtils.fix_decimal(
                dataframe=dataframe,
                column_name=column_name,
                inplace=True
            )

        col: cudf.Series = dataframe[column_name]

        if original_dtype in CudfSupportedDtypes.str_types and match_min_rate == 0:
            col: cudf.Series = cudf.to_numeric(col, errors="coerce")

        if original_dtype in CudfSupportedDtypes.str_types:
            numeric_col: cudf.Series = cudf.to_numeric(col, errors="coerce")
            non_null_before: int = col.notna().sum()      
            non_null_after: int = numeric_col.notna().sum()

            if not (round((non_null_after / non_null_before)*100) >= match_min_rate):
                return False

            col: cudf.Series = numeric_col

        if cp.issubdtype(col.dtype, cp.floating):
            is_integer_column: bool = ((col.round(0) == col) | col.isna()).all()
            
            if is_integer_column:
                warnings.filterwarnings("ignore", category=UserWarning)

                dataframe[column_name] = cudf.to_numeric(col, downcast="integer")

                warnings.resetwarnings()

                print_log(column_name=column_name, column_type=str(dataframe[column_name].dtype), show_log=show_log)
            else:
                # Usar downcast=float faz percer precisão ao converter em float32
                dataframe[column_name] = cudf.to_numeric(col, downcast=None)
                print_log(column_name=column_name, column_type=str(dataframe[column_name].dtype), show_log=show_log)

        elif cp.issubdtype(col.dtype, cp.integer):
            dataframe[column_name] = cudf.to_numeric(col, downcast="integer")
            print_log(column_name=column_name, column_type=str(dataframe[column_name].dtype), show_log=show_log)
        
        if not inplace:
            return dataframe

        return True


    @staticmethod
    def fix_decimal(
        dataframe: cudf.DataFrame,
        column_name: str,
        chunk_size: None|int = 500_000,
        inplace: None|bool=False
    ) -> bool|cudf.DataFrame:
        """
        Converte valores numéricos em formato string com separador de milhar e decimal
        para um formato numérico compatível com float, processando o DataFrame em blocos 
        para evitar estouro de memória.

        O formato esperado da string é algo como:
            '1.234,56', '12.345,67', '0,99', etc.
        Onde:
            - '.' é separador de milhar
            - ',' é separador decimal

        Parâmetros
        ----------
        current_df : cudf.DataFrame
            DataFrame do cuDF
        column_name : str
            Nome da coluna
        chunk_size : int, default=500_000
            Número máximo de linhas processadas por vez
        inplace : bool, default=False
            Quando True, altera a coluna do dataframe ideal, recomendado para dataframe grandes
        """
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )
        
        if not is_valid:
            return False

        # has_list: bool = NumericUtils.has_list(series=dataframe[column_name])

        # if has_list:
        #     return False 
        
        pattern: str = combine_regex(regex_pattern_bad_formatted_number)
        
        has_bad_formatted_number: bool = StringUtils.match(
            series=dataframe[column_name],
            regex=pattern,
            match_min_rate=0
        )

        if not has_bad_formatted_number:
            return False 

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        total_rows: int = len(dataframe)

        for start_index in range(0, total_rows, chunk_size):

            end_index: int = min(start_index + chunk_size, total_rows)

            chunk: cudf.Series = dataframe[column_name].iloc[start_index:end_index]

            mask: cudf.Series = chunk.str.match(pattern)
            
            chunk.loc[mask] = (chunk.loc[mask]
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )

            dataframe.iloc[start_index:end_index, dataframe.columns.get_loc(column_name)] = chunk

        if not inplace:
            return dataframe

        return True


    @staticmethod
    def is_number(
        series: cudf.Series,
        match_min_rate: None|int=50,
        chunk_size: int = 500_000,
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False

        all_regex_valid_number: list[dict[str, str]] = regex_pattern_valid_number + regex_pattern_bad_formatted_number

        combined_regex: str = combine_regex(all_regex_valid_number)

        has_match: bool = StringUtils.match(
            series=series,
            regex=combined_regex,
            match_min_rate=match_min_rate,
            chunk_size=chunk_size
        )

        if has_match:
            return True

        return False


    @staticmethod
    def has_list(
        series: cudf.Series,
        chunk_size: int = 500_000,
    ) -> bool:
        
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False

        combined_regex: str = combine_regex(regex_pattern_list)

        has_match: bool = StringUtils.match(
            series=series,
            regex=combined_regex,
            match_min_rate=0,
            chunk_size=chunk_size
        )

        if has_match:
            return True

        return False
