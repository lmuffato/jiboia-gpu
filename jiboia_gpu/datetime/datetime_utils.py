from .regex_pattern import (
    regex_pattern_date,
    regex_pattern_bad_date,
    regex_pattern_datetime_all
)
from ..log_utils import print_log
from ..string.string_utils import StringUtils
from ..utils import (is_valid_to_normalize, combine_regex, CudfSupportedDtypes)
import cudf


class DateTimeUtils:
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: str,
        match_min_rate: int=50,
        inplace: bool=False,
        chunk_size: int=500_000,
        show_log: bool=True,
    ) -> bool|cudf.DataFrame:

        return DateTimeUtils.to_datetime(
            dataframe=dataframe,
            column_name=column_name,
            match_min_rate=match_min_rate,
            inplace=inplace,
            show_log=show_log
        )


    @staticmethod
    def fix_digit_shape(
        dataframe: cudf.DataFrame,
        column_name: str,
        inplace: bool=False,
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

        # Fix d/mm/yy and d/m/yy to 0d/mm/yy and 0d/m/yy
        dataframe[column_name] = dataframe[column_name].str.replace_with_backrefs(
            r'^(?:\d{1}[^\w\d]\d{1,2}[^\w\d]\d{2,4})$',
            '0\\0',
        )

        # Fix dd/m/yy to 0d/0m/yy
        dataframe[column_name] = dataframe[column_name].str.replace_with_backrefs(
            r'^(\d{2}[^\w\d])(\d{1}[^\w\d]\d{2,4})$',
            "\\1|\\2",
        )
        
        # unfortunately it is necessary to use the "|" marker, because luffy understands "\\10" or "\\1'+0+\\2" as 10 or group 0
        dataframe[column_name] = dataframe[column_name].str.replace("|", "0", regex=False)
        
        if not inplace:
            return dataframe

        return True


    @staticmethod
    def to_datetime(
        dataframe: cudf.DataFrame,
        column_name: str,
        match_min_rate: int=50,
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

        is_unique_datetime_format: bool = DateTimeUtils.is_unique_datetime_format(
            series=dataframe[column_name],
        )

        if is_unique_datetime_format:
            if not inplace:
                dataframe: cudf.DataFrame = dataframe.copy()

            combined_regex: str = combine_regex(regex_pattern_datetime_all)

            mask = dataframe[column_name].str.match(combined_regex)
            
            dataframe.loc[(~mask), column_name] = None

            dataframe[column_name] = cudf.to_datetime(dataframe[column_name])

            if not inplace:
                return dataframe
            return True

        is_date: bool = DateTimeUtils.is_date(
            series=dataframe[column_name],
            match_min_rate=match_min_rate
        )

        if not is_date:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        dataframe[column_name] = (
            dataframe[column_name]
            .str.replace("/", "-", regex=False)
            .str.replace(" ", "-", regex=False)
            .str.replace("_", "-", regex=False)
            .str.replace(".", "-", regex=False)
        )

        has_bad_date_format: bool = StringUtils.match(
            series=dataframe[column_name],
            regex=combine_regex(regex_pattern_bad_date)
        )

        if has_bad_date_format:
            DateTimeUtils.fix_digit_shape(dataframe, column_name, inplace=True)

        combined_regex: str = combine_regex(regex_pattern_date)

        mask = dataframe[column_name].str.match(combined_regex)
        
        dataframe.loc[(~mask), column_name] = None
        
        for pattern in regex_pattern_date:
            mask = dataframe[column_name].str.match(pattern["regex"])
            
            dataframe.loc[mask, column_name] = (
                cudf.to_datetime(
                    dataframe.loc[mask, column_name], format=pattern["format"]
                )
            )

        dataframe[column_name] = dataframe[column_name].astype("datetime64[s]")

        print_log(
            column_name=column_name,
            column_type=str(dataframe[column_name].dtype),
            show_log=show_log
        )
        if not inplace:
            return dataframe

        return True


    @staticmethod
    def is_date(
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

        all_regex_valid_date: list[dict[str, str]] = regex_pattern_date + regex_pattern_bad_date

        combined_regex: str = combine_regex(all_regex_valid_date)

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
    def is_unique_datetime_format(
        series: cudf.Series,
        chunk_size: int = 500_000,
    ) -> bool:
        is_valid: bool = is_valid_to_normalize(
            series=series,
            valid_types=CudfSupportedDtypes.str_types,
        )
        if not is_valid:
            return False

        datetime_types_found: int = 0

        for pattern in regex_pattern_datetime_all:
            has_datetime: bool = StringUtils.match(
                series=series,
                regex=pattern["regex"],
                match_min_rate=0,
                chunk_size=chunk_size
            )
            if has_datetime:
                datetime_types_found = datetime_types_found + 1
        
        if datetime_types_found == 1:
            return True

        return False


    @staticmethod
    def to_date(
        dataframe: cudf.DataFrame,
        column_name: str,
        match_min_rate: int=50,
        coerce_errors: bool=False,
        inplace: bool=False,
        date_format: str="%Y-%m-%d",
        chunk_size: int=500_000,
        show_log: bool=True,
    ) -> bool|cudf.DataFrame:

        is_valid: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.str_types,
        )

        if not is_valid:
            return False
        
        if coerce_errors:
            match_min_rate=0

        is_date: bool = DateTimeUtils.is_date(
            series=dataframe[column_name],
            match_min_rate=match_min_rate
        )

        if not is_date:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        dataframe[column_name] = (
            dataframe[column_name]
            .str.replace("/", "-", regex=False)
            .str.replace(" ", "-", regex=False)
            .str.replace("_", "-", regex=False)
            .str.replace(".", "-", regex=False)
        )

        has_bad_date_format: bool = StringUtils.match(
            series=dataframe[column_name],
            regex=combine_regex(regex_pattern_bad_date)
        )

        if has_bad_date_format:
            DateTimeUtils.fix_digit_shape(dataframe, column_name, inplace=True)

        if coerce_errors:
            combined_regex: str = combine_regex(regex_pattern_date)

            mask = dataframe[column_name].str.match(combined_regex)
            
            dataframe.loc[(~mask), column_name] = None

        for pattern in regex_pattern_date:
            mask = dataframe[column_name].str.match(pattern["regex"])
            
            dataframe.loc[mask, column_name] = (
                cudf.to_datetime(
                    dataframe.loc[mask, column_name], format=pattern["format"]
                )
                .dt.strftime(date_format)
            )

        print_log(
            column_name=column_name,
            column_type=date_format,
            show_log=show_log
        )
        if not inplace:
            return dataframe

        return True


    @staticmethod
    def combine_date_time(
        dataframe: cudf.DataFrame,
        column_name: str,
        time_column_name: str,
        inplace: bool=False,
        chunk_size: int=500_000
    ) -> bool|cudf.DataFrame:
        """
        Adiciona o time de uma coluna datetime a uma coluna datetime.
        """
        is_valid_datetime: bool = is_valid_to_normalize(
            series=dataframe[column_name],
            valid_types=CudfSupportedDtypes.datetime_types,
        )

        is_valid_timedelta: bool = is_valid_to_normalize(
            series=dataframe[time_column_name],
            valid_types=CudfSupportedDtypes.timedelta_types,
        )

        if not is_valid_datetime or not is_valid_timedelta:
            return False

        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        dataframe[column_name] = dataframe[column_name] + dataframe[time_column_name]

        if not inplace:
            return dataframe

        return True
