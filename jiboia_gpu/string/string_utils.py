from ..boolean.regex_pattern import (
    regex_pattern_boolean_raw,
    regex_pattern_boolean_numeric_raw
)
from ..utils.chunk_utils import chunk_iterate
from ..datetime.regex_pattern import (
    regex_pattern_date,
    regex_pattern_datetime_all
)
from ..utils.log_utils import (
    print_normalize_space_log,
    print_normalize_string_log,
    print_to_category_log
)
from ..number.regex_pattern import (
    regex_pattern_bad_formatted_number,
    regex_pattern_valid_number
)
from ..time.regex_pattern import (
    regex_pattern_time_utc,
    regex_pattern_time_hh_mm,
    regex_pattern_time_hh_mm_ss,
    regex_pattern_time_hh_mm_ss_n
)
from ..utils.str_utils import combine_regex
from ..utils.validation_utils import (
    CudfSupportedDtypes,
    is_valid_to_normalize
)
from typing import Literal
import cudf


class StringUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        to_case: None|Literal['lower', 'upper']=None,
        to_ASCII: bool=False,
        inplace: bool=False,
        chunk_size: int=500_000,
        show_log: bool=True,
    ) -> bool|cudf.DataFrame:

        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )

        if not is_valid:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        StringUtils.normalize_spaces(
            dataframe=dataframe,
            column_name=column_name,
            inplace=True,
            chunk_size=chunk_size,
            show_log=show_log
        )

        StringUtils.normalize_str(
            dataframe=dataframe,
            column_name=column_name,
            to_case=to_case,
            to_ASCII=to_ASCII,
            inplace=True,
            chunk_size=chunk_size,
            show_log=show_log
        )

        if not inplace:
            return dataframe
        return True


    @staticmethod
    def normalize_spaces(
        dataframe: cudf.DataFrame,
        column_name: str,
        inplace: bool=False,
        chunk_size: int=500_000,
        show_log: bool=True,
    ) -> bool|cudf.DataFrame:

        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )

        if not is_valid:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        total_rows: int = len(dataframe)
        column_index: int = dataframe.columns.get_loc(column_name)

        for start_index in range(0, total_rows, chunk_size):
            end_index: int = min(start_index + chunk_size, total_rows)

            series_chunk = dataframe.iloc[start_index:end_index, column_index]

            # normaliza espaços e remove espaços extras no início/fim
            series_chunk = series_chunk.str.normalize_spaces().str.strip()

            dataframe.iloc[start_index:end_index, column_index] = series_chunk

        del column_index
        del total_rows

        print_normalize_space_log(
            column_name=column_name,
            show_log=show_log
        )
        
        if not inplace:
            return dataframe

        return True

    @staticmethod
    def normalize_str(
        dataframe: cudf.DataFrame,
        column_name: None|str=None,
        to_case: None|Literal['lower', 'upper']=None,
        to_ASCII: bool=False,
        inplace: bool=False,
        chunk_size: int=500_000,
        show_log: bool=True,
    ) -> bool|cudf.DataFrame:

        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )

        if not is_valid:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        if not to_ASCII:
            if to_case == "lower":
                dataframe[column_name] = dataframe[column_name].str.lower()

            if to_case == "upper":
                dataframe[column_name] = dataframe[column_name].str.upper()

            if not inplace:
                return dataframe
            return True

        if to_ASCII and to_case == "lower":

            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                series_chunk = dataframe.iloc[start_index:end_index, column_index]

                series_chunk = (
                    series_chunk.str.lower()
                    .str.replace(r"[áàâãä]", "a", regex=True)
                    .str.replace(r"[éèêë]", "e", regex=True)
                    .str.replace(r"[íìîï]", "i", regex=True)
                    .str.replace(r"[óòôõö]", "o", regex=True)
                    .str.replace(r"[úùûü]", "u", regex=True)
                    .str.replace(r"[ç]", "c", regex=True)
                )

                dataframe.iloc[start_index:end_index, column_index] = series_chunk

            del column_index
            del total_rows

            print_normalize_string_log(
                column_name=column_name,
                show_log=show_log,
                to_case=to_case,
                to_ASCII=to_ASCII
            )

            if not inplace:
                return dataframe
            return True

        if to_ASCII and to_case == "upper":
            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                series_chunk = dataframe.iloc[start_index:end_index, column_index]

                # normalização para uppercase e remoção de acentos maiúsculos
                series_chunk = (
                    series_chunk.str.upper()
                    .str.replace(r"[ÁÀÂÃÄ]", "A", regex=True)
                    .str.replace(r"[ÉÈÊË]", "E", regex=True)
                    .str.replace(r"[ÍÌÎÏ]", "I", regex=True)
                    .str.replace(r"[ÓÒÔÕÖ]", "O", regex=True)
                    .str.replace(r"[ÚÙÛÜ]", "U", regex=True)
                    .str.replace(r"[Ç]", "C", regex=True)
                )

                dataframe.iloc[start_index:end_index, column_index] = series_chunk

            del column_index
            del total_rows

            print_normalize_string_log(
                column_name=column_name,
                show_log=show_log,
                to_case=to_case,
                to_ASCII=to_ASCII
            )

            if not inplace:
                return dataframe
            return True
        

        if to_ASCII and not to_case:
            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                series_chunk = dataframe.iloc[start_index:end_index, column_index]

                # substituição de todos os acentos e cedilhas
                series_chunk = (
                    series_chunk
                    .str.replace(r"[áàâãä]", "a", regex=True)
                    .str.replace(r"[ÁÀÂÃÄ]", "A", regex=True)
                    .str.replace(r"[éèêë]", "e", regex=True)
                    .str.replace(r"[ÉÈÊË]", "E", regex=True)
                    .str.replace(r"[íìîï]", "i", regex=True)
                    .str.replace(r"[ÍÌÎÏ]", "I", regex=True)
                    .str.replace(r"[óòôõö]", "o", regex=True)
                    .str.replace(r"[ÓÒÔÕÖ]", "O", regex=True)
                    .str.replace(r"[úùûü]", "u", regex=True)
                    .str.replace(r"[ÚÙÛÜ]", "U", regex=True)
                    .str.replace(r"[ç]", "c", regex=True)
                    .str.replace(r"[Ç]", "C", regex=True)
                )

                dataframe.iloc[start_index:end_index, column_index] = series_chunk

            del column_index
            del total_rows

            print_normalize_string_log(
                column_name=column_name,
                show_log=show_log,
                to_case=to_case,
                to_ASCII=to_ASCII
            )
            
            if not inplace:
                return dataframe
            return True

        return False

    @staticmethod
    def to_category(
        dataframe: cudf.DataFrame,
        column_name: None|str=None,
        inplace: bool=False,
        chunk_size: int = 500_000,
        show_log: bool=True,
    ) -> bool|cudf.DataFrame:

        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False
        
        is_str: bool = StringUtils.is_str(
            series=dataframe[column_name],
            chunk_size=chunk_size

        )

        if not is_str:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        unique_values = dataframe[column_name].unique()

        non_null_values: int = dataframe[column_name].notna().sum()

        # Os valores únicos precisam ser até 50% dos dados válidos
        can_be_category: bool = ((non_null_values // 2) >= len(unique_values))

        if not can_be_category:
            return False

        unique_values = unique_values.sort_values()
        unique_values = unique_values.reset_index(drop=True)
        
        categorical_dtype = cudf.CategoricalDtype(
            categories=unique_values,
            ordered=True
        )
        dataframe[column_name] = dataframe[column_name].astype(categorical_dtype)

        print_to_category_log(
            column_name=column_name,
            show_log=show_log
        )

        if not inplace:
            return dataframe
        return True


    @staticmethod
    def is_str(
        series: cudf.Series,
        chunk_size: int = 500_000,
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False
        
        bool_pattern: str = combine_regex(regex_pattern_boolean_raw + regex_pattern_boolean_numeric_raw)

        is_bool: bool = StringUtils.match(
            series=series,
            regex=bool_pattern,
            chunk_size=chunk_size,
        )

        if is_bool:
            return False
        
        date_pattern: str = combine_regex(regex_pattern_date + regex_pattern_datetime_all)

        is_date: bool = StringUtils.match(
            series=series,
            regex=date_pattern,
            chunk_size=chunk_size,
        )

        if is_date:
            return False

        number_pattern: str = combine_regex(regex_pattern_bad_formatted_number + regex_pattern_valid_number)

        is_number: bool = StringUtils.match(
            series=series,
            regex=number_pattern,
            chunk_size=chunk_size,
        )

        if is_number:
            return False

        time_pattern: str = combine_regex(regex_pattern_time_utc + regex_pattern_time_hh_mm + regex_pattern_time_hh_mm_ss + regex_pattern_time_hh_mm_ss_n)

        is_time: bool = StringUtils.match(
            series=series,
            regex=time_pattern,
            chunk_size=chunk_size
        )

        if is_time:
            return False

        return True


    @staticmethod
    def normalize_unique_values(
        dataframe: cudf.DataFrame,
        column_name: str,
        mapping_dict: dict[str, list[str]],
        null_values: None|list[str] = [],
        inplace: bool=False,
    ) -> None:
        """
        Normaliza e limpa uma coluna em um cuDF DataFrame de forma inplace.

        Args:
            dataframe: O DataFrame a ser modificado.
            column_name: O nome da coluna a ser normalizada.
            mapping_dict: Dicionário no formato {'novo_valor': ['antigo1', 'antigo2']}.
            null_values: Lista opcional de valores a serem convertidos para nulo.
        """
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False

        replace_map = {
            old_value: new_value
            for new_value, old_values_list in mapping_dict.items()
            for old_value in old_values_list
        }

        # Substitiu os valores para o valor da chave do dict
        dataframe[column_name] = dataframe[column_name].replace(replace_map)

        # Substitiu os valores nulos
        if null_values:
            dataframe[column_name] = dataframe[column_name].replace(
                null_values, None
            )

        if not inplace:
            return dataframe
        return True


    # MATCHING FUNCTIONS
    @staticmethod
    def match(
        series: cudf.Series,
        regex: str,
        match_min_rate: int = 0,
        chunk_size: int = 500_000
    ) -> bool:
        """
        Verifica se uma coluna de strings (`cudf.Series`) satisfaz uma correspondência
        baseada em expressão regular, considerando limites de percentual mínimo de acerto.

        A função processa os dados em chunks para evitar sobrecarga de memória em
        séries muito grandes.

        Parâmetros
        ----------
        series : cudf.Series
            Série contendo os valores de texto a serem avaliados.
        regex : str
            Expressão regular utilizada para correspondência.
        match_min_rate : int, padrão=0
            Percentual mínimo (0–100) de linhas **não nulas** que devem corresponder ao padrão:
            
            - 0   → retorna `True` se pelo menos **uma** linha corresponder ao padrão
                     (equivalente a `any()`).
            - 1–99 → retorna `True` se pelo menos `match_min_rate%` das linhas não nulas
                      corresponderem ao padrão.
            - 100 → retorna `True` apenas se **todas** as linhas não nulas corresponderem ao padrão.
            
            O valor é automaticamente limitado ao intervalo [0, 100].
        chunk_size : int, padrão=500_000
            Quantidade de linhas processadas por vez. Usado para evitar estouro de memória em `series` muito grandes.

        Retorno
        -------
        bool
            `True` se a condição definida por `match_min_rate` for satisfeita,
            caso contrário `False`.
        """
        match_min_rate: int = max(0, min(100, int(match_min_rate)))

        total_not_null_rows: int = series.notna().sum()

        if match_min_rate == 0:
            for chunk in chunk_iterate(series, chunk_size):
                if chunk.str.match(regex).any():
                    return True

            return False

        if match_min_rate > 0 and match_min_rate < 100:
            match_min: int = ((total_not_null_rows * match_min_rate) // 100)
            total_match: int = 0

            for chunk in chunk_iterate(series, chunk_size):
                total_match = total_match + chunk.str.match(regex).sum()

                if total_match >= match_min:
                    return True

            return False    

        if match_min_rate == 100:
            total_match: int = 0
            for chunk in chunk_iterate(series, chunk_size):

                if chunk.str.match(regex).any():
                    total_match = total_match + chunk.str.match(regex).sum()

            if total_match == total_not_null_rows:
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
        for pattern in regex_patterns:
            pattern["frequency"] = 0

        # Itera sobre os chunks
        for chunk in chunk_iterate(series, chunk_size):
            # Para cada chunk, testa todos os padrões
            for pattern in regex_patterns:
                pattern["frequency"] += chunk.str.match(pattern["regex"]).sum()
                
        return regex_patterns


    @staticmethod
    def sanitize_unique(
        dataframe: cudf.DataFrame,
        column_name: str,
        sanitize_values: dict[str, list[str]] = [],
        null_values: list[str] = [],
        inplace: bool=True
    ) -> None:
        
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )

        if not is_valid:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        if len(null_values) > 0:
            dataframe[column_name] = dataframe[column_name].replace(null_values, None)
        
        if len(sanitize_values) > 0:
            for target_value, values_to_replace in sanitize_values.items():
                dataframe[column_name] = dataframe[column_name].replace(values_to_replace, target_value)

        if not inplace:
            return dataframe
        return True
