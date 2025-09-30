from .regex_pattern import RAW_INVALID_LOWERCASE_VALUES
from ..utils.log_utils import print_normalize_type_log
from ..utils.validation_utils import (
    CudfSupportedDtypes,
    is_valid_to_normalize
)
import cudf


class NullUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        null_values: list[str] = [],
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

        new_lower_values: list[str] = [v.lower() for v in null_values]

        all_null_values: list[str] = set(new_lower_values + RAW_INVALID_LOWERCASE_VALUES)

        total_rows: int = len(dataframe)

        if not inplace:
            dataframe = dataframe.copy()

        total_rows: int = len(dataframe)
        column_index: int = dataframe.columns.get_loc(column_name)

        for start_index in range(0, total_rows, chunk_size):
            end_index: int = min(start_index + chunk_size, total_rows)

            series_chunk = dataframe.iloc[start_index:end_index, column_index]

            mask: cudf.Series = series_chunk.str.lower().isin(all_null_values)

            dataframe.iloc[start_index:end_index, column_index] = series_chunk.where(~mask, None)

        del column_index
        del total_rows

        print_normalize_type_log(
            column_name=column_name,
            value_original="null",
            value_final="<NA>",
            show_log=show_log
        )

        if not inplace:
            return dataframe
        
        return True

    @staticmethod
    def get_default_nulls() -> list[str]:
        return RAW_INVALID_LOWERCASE_VALUES
