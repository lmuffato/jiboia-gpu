import cudf
from .regex_pattern import (
    regex_pattern_time_utc,
    regex_pattern_time_amp_pm,
    regex_pattern_time_hh,
    regex_pattern_time_hh_mm,
    regex_pattern_time_hh_mm_ss,
    regex_pattern_time_hh_mm_ss_n,
    regex_pattern_timedelta
)
from ..log_utils import print_log
from ..string.string_utils import StringUtils
from ..utils import is_valid_to_normalize, combine_regex, CudfSupportedDtypes
import cudf


class TimeUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        match_min_rate: int=50,
        inplace: bool=False,
        show_log: bool=True,
        chunk_size: int=500_000
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False
        
        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        TimeUtils.from_time(
            dataframe=dataframe,
            column_name=column_name,
            match_min_rate=match_min_rate,
            inplace=True,
            chunk_size=chunk_size,
            show_log=show_log
        )
        print_log(
            column_name=column_name,
            column_type=str(dataframe[column_name].dtype),
            show_log=show_log
        )

        if not inplace:
            return dataframe

        return True


    @staticmethod
    def from_utc(
        dataframe: cudf.DataFrame,
        column_name: str,
        inplace: bool=False,
        show_log: bool=True,
        chunk_size: int=500_000
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False
        
        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        is_utc_time: bool = TimeUtils.is_time_utc(
            series=dataframe[column_name],
            chunk_size=chunk_size
        )

        if not is_utc_time:
            return False

        utc_pattern: str = combine_regex(regex_pattern_time_utc)

        mask: cudf.Series = dataframe[column_name].str.match(utc_pattern)

        dataframe.loc[mask, column_name] = (
            dataframe.loc[mask, column_name]
            .str.replace(" ", "")
            .str.replace("UTC", "")
        )
        
        dataframe.loc[mask, column_name] = (
            cudf.to_datetime(dataframe.loc[mask, column_name] + ":00", format="%H%M:%S")
            .dt.strftime("%H:%M:%S")
        )

        if not inplace:
            return dataframe

        return True


    @staticmethod
    def from_time(
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

        if not is_time:
            return False

        # HHMMUTC|HHMM UTC -> HH:MM:00 
        pattern_utc: str = combine_regex(regex_pattern_time_utc)
        mask: cudf.Series = dataframe[column_name].str.match(pattern_utc)

        dataframe.loc[mask, column_name] = (
            dataframe.loc[mask, column_name]
            .str.replace(" ", "")
            .str.replace("UTC", "")
        )
        dataframe.loc[mask, column_name] = dataframe.loc[mask, column_name] + ":00"
        
        dataframe.loc[mask, column_name] = (
            cudf.to_datetime(dataframe.loc[mask, column_name] + ":00", format="%H%M:%S")
            .dt.strftime("%H:%M:%S")
        )

        # hh -> hh:00:00
        pattern_hh: str = combine_regex(regex_pattern_time_hh)
        mask: cudf.Series = dataframe[column_name].str.match(pattern_hh)
        dataframe.loc[mask, column_name] = dataframe.loc[mask, column_name] + ":00:00"

        # hh:mm -> hh:mm:00
        pattern_hh_mm: str = combine_regex(regex_pattern_time_hh_mm)
        mask: cudf.Series = dataframe[column_name].str.match(pattern_hh_mm)
        dataframe.loc[mask, column_name] = dataframe.loc[mask, column_name] + ":00"

        # # hh:mm:ss.s -> hh:mm:00
        pattern_hh_mm_ss_n: str = combine_regex(regex_pattern_time_hh_mm_ss_n)
        mask: cudf.Series = dataframe[column_name].str.match(pattern_hh_mm_ss_n)
        dataframe.loc[mask, column_name] = dataframe.loc[mask, column_name].str.slice(0, 8)

        # Valores inválidos são convertidos em nulos
        all_time_patern: str = combine_regex(regex_pattern_time_utc + regex_pattern_time_hh_mm + regex_pattern_time_hh_mm_ss + regex_pattern_time_hh_mm_ss_n)
        mask: cudf.Series = dataframe[column_name].str.match(all_time_patern)
        dataframe.loc[~mask, column_name] = None

        dataframe[column_name] = cudf.to_datetime(dataframe[column_name], format="%H:%M:%S")

        ref_dt = cudf.to_datetime('1970-01-01')
        dataframe[column_name] = dataframe[column_name] - ref_dt

        if not inplace:
            return dataframe

        return True


    @staticmethod
    def is_time_hh(
        series: cudf.Series,
        chunk_size: int = 500_000,
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False

        combined_regex: str = combine_regex(regex_pattern_time_hh)

        has_match: bool = StringUtils.match(
            series=series,
            regex=combined_regex,
            match_min_rate=100,
            chunk_size=chunk_size
        )

        if has_match:
            return True

        return False


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
    def is_time_utc(
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

        combined_regex: str = combine_regex(regex_pattern_time_utc)

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
