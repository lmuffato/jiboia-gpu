from ..analysis_utils import AnalysisUtils
from ..df_utils import (
    print_log
)
import cudf
import cupy as cp
import warnings


class NumericUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        numeric_threshold: None|int=50,
        inplace: None|bool=False,
        show_log: None|bool=True
    ) -> cudf.DataFrame|None:
        """
        Converte uma coluna de um DataFrame cuDF para o tipo numérico mais apropriado.

        - Se a coluna for string/object, tenta convertê-la para numérica.
        - Se a coluna for float, verifica se pode ser representada como inteiro sem perda de dados.
        - Realiza o downcast para o menor tipo de dado possível.

        Args:
            df: O DataFrame cuDF a ser modificado.
            column_name: O nome da coluna a ser normalizada.
            numeric_threshold:
                A proporção mínima de valores não nulos que devem ser inteiro de 0 a 100
                numéricos para que uma coluna de string seja convertida (padrão: 0.7).
            inplace: Se True, modifica o DataFrame original. Se False, retorna uma cópia.
            print_info: Se True, mostra a coluna convertida e o tipo convertido.

        Returns:
            O DataFrame modificado se inplace=False, senão None.
        """
        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        original_dtype: cp.dtypes = dataframe[column_name].dtype

        if original_dtype in ("object", "string"):
            NumericUtils.fix_decimal(dataframe, column_name, inplace=True)

        col: cudf.Series = dataframe[column_name]

        if original_dtype in ("object", "string"):
            # numeric_col: cudf.Series = cudf.to_numeric(col, errors="coerce")
            numeric_col: cudf.Series = cudf.to_numeric(col, errors="coerce")
            
            non_null_before: int = col.notna().sum()      
            non_null_after: int = numeric_col.notna().sum()
            
            if (round((non_null_after / non_null_before)*100) <= numeric_threshold):
                return dataframe if not inplace else None

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


    @staticmethod
    def fix_decimal(
        dataframe: cudf.DataFrame,
        column_name: str,
        chunk_size: None|int = 500_000,
        inplace: None|bool=False
    ) -> int:
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
            DataFrame do cuDF que contém a coluna a ser processada.
        column_name : str
            Nome da coluna que será convertida.
        chunk_size : int, default=500_000
            Número máximo de linhas processadas por vez. Útil para grandes DataFrames.
        inplace : bool, default=False
            Quando True, altera a coluna do dataframe ideal, recomendado para dataframe grandes.
        """
        pattern: str = (
            r'^\d{1,3}(?:\.\d{3})*,\d+$|'
            r'^\d*,\d+$'
        )

        array_pattern: str = r'[\[\]]'

        if dataframe[column_name].dtype not in ["string", "object"]:
            return dataframe if not inplace else None

        if AnalysisUtils.any_pattern(series=dataframe[column_name], pattern=array_pattern):
            return dataframe if not inplace else None 
        
        if not AnalysisUtils.any_pattern(series=dataframe[column_name], pattern=pattern):
            return dataframe if not inplace else None 

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
