import cudf
from .regex_pattern import (
    regex_pattern_time_utc,
    regex_pattern_time_amp_pm,
    regex_pattern_time_hh_mm,
    regex_pattern_time_hh_mm_ss,
    regex_pattern_time_hh_mm_ss_n,
    regex_pattern_timedelta
)
from ..utils.log_utils import print_log
from ..string.string_utils import StringUtils
from ..utils.str_utils import combine_regex
from ..utils.validation_utils import (
    CudfSupportedDtypes,
    is_valid_to_normalize
)
import cudf


class TimeUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        match_min_rate: int=50,
        inplace: bool=False,
        chunk_size: int=500_000,
        show_log: bool=True
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False
        
        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        is_time: bool = TimeUtils.is_time(
            series=dataframe[column_name],
            match_min_rate=match_min_rate,
            chunk_size=chunk_size
        )

        if not is_time:
            return False       

        pattern_time_utc: str = combine_regex(regex_pattern_time_utc)
        has_time_utc: bool = StringUtils.match(
            dataframe[column_name],
            regex=pattern_time_utc,
            match_min_rate=0,
            chunk_size=chunk_size
        )
        if has_time_utc:
            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                # pega fatia pelo índice absoluto
                mask: cudf.Series = dataframe.iloc[start_index:end_index, column_index].str.match(
                    pattern_time_utc
                )

                # aplica transformação direto no dataframe
                dataframe.iloc[start_index:end_index, column_index] = (
                    dataframe.iloc[start_index:end_index, column_index].where(~mask, 
                        dataframe.iloc[start_index:end_index, column_index].str.replace(" ", "")
                        .str.replace("UTC", "")
                        .str.slice(0, 2) + ":" + dataframe.iloc[start_index:end_index, column_index].str.slice(2, 4) + ":00"
                    )
                )
            del column_index
            del total_rows

        # hh:mm -> hh:mm:00
        pattern_time_hh_mm: str = combine_regex(regex_pattern_time_hh_mm)
        has_time_hh_mm: bool = StringUtils.match(
            dataframe[column_name],
            regex=pattern_time_hh_mm,
            match_min_rate=0,
            chunk_size=chunk_size
        )

        if has_time_hh_mm:
            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                # pega fatia pelo índice absoluto
                mask: cudf.Series = dataframe.iloc[start_index:end_index, column_index].str.match(
                    pattern_time_hh_mm
                )

                # aplica transformação direto no dataframe
                dataframe.iloc[start_index:end_index, column_index] = (
                    dataframe.iloc[start_index:end_index, column_index].where(~mask, 
                        dataframe.iloc[start_index:end_index, column_index] + ":00"
                    )
                )
            del column_index
            del total_rows


        # hh:mm:ss.s -> hh:mm:00
        pattern_time_hh_mm_ss_n: str = combine_regex(regex_pattern_time_hh_mm_ss_n)
        has_time_hh_mm: bool = StringUtils.match(
            dataframe[column_name],
            regex=pattern_time_hh_mm_ss_n,
            match_min_rate=0,
            chunk_size=chunk_size
        )

        if has_time_hh_mm:
            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                # pega fatia pelo índice absoluto
                mask: cudf.Series = dataframe.iloc[start_index:end_index, column_index].str.match(
                    pattern_time_hh_mm_ss_n
                )

                # aplica transformação direto no dataframe
                dataframe.iloc[start_index:end_index, column_index] = (
                    dataframe.iloc[start_index:end_index, column_index].where(~mask, 
                        dataframe.iloc[start_index:end_index, column_index].str.slice(0, 8)
                    )
                )
            del column_index
            del total_rows


        # Valores inválidos são convertidos em nulos
        pattern_time_hh_mm_ss: str = combine_regex(regex_pattern_time_hh_mm_ss)
        has_time_hh_mm_ss: bool = StringUtils.match(
            dataframe[column_name],
            regex=pattern_time_hh_mm_ss,
            match_min_rate=0,
            chunk_size=chunk_size
        )

        if has_time_hh_mm_ss:
            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                # pega fatia pelo índice absoluto
                mask: cudf.Series = dataframe.iloc[start_index:end_index, column_index].str.match(
                    pattern_time_hh_mm_ss
                )

                # aplica transformação direto no dataframe
                dataframe.iloc[start_index:end_index, column_index] = (
                    dataframe.iloc[start_index:end_index, column_index].where(mask, None)
                )
            del column_index
            del total_rows

        dataframe[column_name] = cudf.to_datetime(dataframe[column_name], format="%H:%M:%S")

        ref_dt = cudf.to_datetime('1970-01-01')
        dataframe[column_name] = dataframe[column_name] - ref_dt

        print_log(
            column_name=column_name,
            column_type=str(dataframe[column_name].dtype),
            show_log=show_log
        )

        if not inplace:
            return dataframe

        return True


    @staticmethod
    def is_time(
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

        combined_regex: str = combine_regex(regex_pattern_time_utc + regex_pattern_time_hh_mm + regex_pattern_time_hh_mm_ss + regex_pattern_time_hh_mm_ss_n)

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
    def is_time_am_pm(
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

        combined_regex: str = combine_regex(regex_pattern_time_amp_pm)

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
    def is_unique_timedelta_format(
        series: cudf.Series,
        chunk_size: int = 500_000,
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False

        timedelta_types_found: int = 0

        for pattern in regex_pattern_timedelta:
            has_timedelta: bool = StringUtils.match(
                series=series,
                regex=pattern["regex"],
                match_min_rate=0,
                chunk_size=chunk_size
            )
            if has_timedelta:
                timedelta_types_found = timedelta_types_found + 1
        
        if timedelta_types_found == 1:
            return True

        return False
