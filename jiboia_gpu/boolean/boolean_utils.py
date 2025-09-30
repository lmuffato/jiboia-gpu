import cudf
from .regex_pattern import (
    regex_pattern_boolean,
    regex_pattern_boolean_raw,
    regex_pattern_boolean_numeric_raw
)
from ..utils.chunk_utils import chunk_iterate
from ..utils.log_utils import print_log
from ..string.string_utils import StringUtils
from ..utils.str_utils import combine_regex
from ..utils.validation_utils import (
    CudfSupportedDtypes,
    is_valid_to_normalize
)


class BooleanUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        bool_number: bool=False,
        match_min_rate: int=50,
        inplace: None|bool=False,
        show_log: None|bool=True,
        chunk_size: int=500_000
    ) -> bool|cudf.DataFrame:

        valid_types: list[str] = CudfSupportedDtypes.str_types + CudfSupportedDtypes.numeric_types

        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=valid_types,
        )
        
        if not is_valid:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        if bool_number:
            has_normalized: bool = BooleanUtils.from_binary_num(
                dataframe=dataframe,
                column_name=column_name,
                inplace=True,
                show_log=show_log
            )

            if has_normalized:
                if not inplace:
                    return dataframe
                return True

        if bool_number:
            has_normalized: bool = BooleanUtils.from_binary_str(
                dataframe=dataframe,
                column_name=column_name,
                inplace=True,
                show_log=show_log
            )

            if has_normalized:
                if not inplace:
                    return dataframe
                return True
        
        has_normalized: bool = BooleanUtils.from_bool_str(
            dataframe=dataframe,
            column_name=column_name,
            match_min_rate=match_min_rate,
            inplace=True,
            show_log=show_log,
            chunk_size=chunk_size
        )

        if has_normalized:
            if not inplace:
                return dataframe
            return True

        return False


    @staticmethod
    def from_binary_str(
        dataframe: cudf.DataFrame,
        column_name: str,
        inplace: None|bool=False,
        show_log: None|bool=True,
        chunk_size: int=500_000
    ) -> bool|cudf.DataFrame:
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )

        if not is_valid:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        is_binary_str: bool = BooleanUtils.is_binary_str(
            series=dataframe[column_name],
            chunk_size=chunk_size
        )

        if not is_binary_str:
            return False

        mapping_dict = {}

        for pattern in regex_pattern_boolean_numeric_raw:
            for fmt in pattern["format"]:
                mapping_dict[fmt] = pattern["pattern"]

        dataframe[column_name] = (
            dataframe[column_name].astype("string").str.lower().map(mapping_dict)
        )
        
        dataframe[column_name] = dataframe[column_name].astype("boolean")

        print_log(column_name=column_name, column_type=str(dataframe[column_name].dtype), show_log=show_log)
        
        if not inplace:
            return dataframe

        return True


    @staticmethod
    def from_binary_num(
        dataframe: cudf.DataFrame,
        column_name: str,
        inplace: None|bool=False,
        show_log: None|bool=True
    ) -> bool|cudf.DataFrame:

        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.numeric_types,
        )
        
        if not is_valid:
            return False

        is_binary_num: bool = BooleanUtils.is_binary_num(series=dataframe[column_name])

        if not is_binary_num:
            return False
        
        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        mapping_dict = {1: True, 0: False}

        dataframe[column_name] = (
            dataframe[column_name].astype("string").str.lower().map(mapping_dict)
        )

        print_log(column_name=column_name, column_type=str(dataframe[column_name].dtype), show_log=show_log)
        
        if not inplace:
            return dataframe

        return True


    @staticmethod
    def from_bool_str(
        dataframe: cudf.DataFrame,
        column_name: str,
        match_min_rate: int=50,
        inplace: None|bool=False,
        show_log: None|bool=True,
        chunk_size: int=500_000
    ) -> bool|cudf.DataFrame:
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )

        if not is_valid:
            return False
        
        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        is_bool: bool = BooleanUtils.is_bool(
            series=dataframe[column_name],
            match_min_rate=match_min_rate,
            chunk_size=chunk_size
        )

        if not is_bool:
            return False

        mapping_dict: dict[str, str] = {}
        for pattern in regex_pattern_boolean:
            for fmt in pattern["format"]:
                mapping_dict[fmt] = pattern["pattern"]

        total_rows: int = len(dataframe)
        column_index: int = dataframe.columns.get_loc(column_name)

        for start_index in range(0, total_rows, chunk_size):
            end_index: int = min(start_index + chunk_size, total_rows)

            series_chunk = dataframe.iloc[start_index:end_index, column_index]

            series_chunk = series_chunk.astype("string").str.lower().map(mapping_dict)

            dataframe.iloc[start_index:end_index, column_index] = series_chunk

        del column_index
        del total_rows

        dataframe[column_name] = dataframe[column_name].astype("boolean")

        print_log(column_name=column_name, column_type=str(dataframe[column_name].dtype), show_log=show_log)

        if not inplace:
            return dataframe

        return True


    @staticmethod
    def is_binary_str(
        series: cudf.Series,
        chunk_size: int = 500_000
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False
        
        is_only_binary: bool = False

        for chunk in chunk_iterate(series, chunk_size):
            is_only_binary = chunk.dropna().str.match(r'^[01]$').all()

            if not is_only_binary:
                return False

        if not is_only_binary:
            return False

        return True


    @staticmethod
    def is_binary_num(
        series: cudf.Series,
        chunk_size: int = 500_000,
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.numeric_types,
        )
        
        if not is_valid:
            return False

        is_only_binary: bool = False

        for chunk in chunk_iterate(series, chunk_size):
            is_only_binary = chunk.dropna().isin([0, 1]).all()

            if not is_only_binary:
                return False

        if not is_only_binary:
            return False

        return True


    @staticmethod
    def is_bool(
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
    
        combined_regex = combine_regex(regex_pattern_boolean_raw)

        has_match: bool = StringUtils.match(
            series=series,
            regex=combined_regex,
            match_min_rate=match_min_rate,
            chunk_size=chunk_size
        )
        
        if has_match:
            return True

        return False
