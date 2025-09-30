from .regex_pattern import (
    regex_pattern_date,
    regex_pattern_bad_date,
    regex_pattern_datetime_all
)
from ..utils.log_utils import print_log
from ..string.string_utils import StringUtils
from ..utils.str_utils import combine_regex
from ..utils.validation_utils import (
    CudfSupportedDtypes,
    is_valid_to_normalize
)
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
            chunk_size=chunk_size,
            show_log=show_log
        )  


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
        
        is_date: bool = DateTimeUtils.is_date(
            series=dataframe[column_name],
            match_min_rate=match_min_rate
        )

        if not is_date:
            return False

        is_unique_datetime_pattern: bool = DateTimeUtils.is_unique_datetime_pattern(
            series=dataframe[column_name],
        )

        if is_unique_datetime_pattern:
            
            is_normalized: bool = DateTimeUtils.normalize_unique_pattern(
                dataframe=dataframe,
                column_name=column_name,
                inplace=True,
                chunk_size=chunk_size,
            )

            if not is_normalized:
                return False

            print_log(
                column_name=column_name,
                column_type=str(dataframe[column_name].dtype),
                show_log=show_log
            )

            if not inplace:
                return dataframe
            return True
        
        DateTimeUtils.normalize_date_delimiters(
            dataframe=dataframe,
            column_name=column_name,
            inplace=True,
            chunk_size=chunk_size,
        )

        # Datas com um único dígito: 1/1/2010
        has_bad_date_format: bool = StringUtils.match(
            series=dataframe[column_name],
            regex=combine_regex(regex_pattern_bad_date)
        )

        if has_bad_date_format:
            DateTimeUtils.fix_digit_shape(dataframe, column_name, inplace=True)

        is_normalized: bool = DateTimeUtils.normalize_mixed_dates(
                dataframe=dataframe,
                column_name=column_name,
                inplace=True,
                chunk_size=chunk_size,
            )

        if not is_normalized:
            return False

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

        all_regex_valid_date: list[dict[str, str]] = regex_pattern_date + regex_pattern_bad_date + regex_pattern_datetime_all

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
    def is_unique_datetime_pattern(
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


    def normalize_unique_pattern(
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

        is_unique_datetime_pattern: bool = DateTimeUtils.is_unique_datetime_pattern(
            series=dataframe[column_name],
        )
        if is_unique_datetime_pattern:
            if not inplace:
                dataframe: cudf.DataFrame = dataframe.copy()

            total_rows: int = len(dataframe)
            column_index: int = dataframe.columns.get_loc(column_name)

            combined_regex: str = combine_regex(regex_pattern_datetime_all)

            for start_index in range(0, total_rows, chunk_size):
                end_index: int = min(start_index + chunk_size, total_rows)

                series_chunk = dataframe.iloc[start_index:end_index, column_index]

                mask = series_chunk.str.match(combined_regex)

                series_chunk = series_chunk.where(mask, None)

                dataframe.iloc[start_index:end_index, column_index] = series_chunk

            dataframe[column_name] = cudf.to_datetime(dataframe[column_name])
            dataframe[column_name] = dataframe[column_name].astype("datetime64[ns]")

            del total_rows
            del column_index

            if not inplace:
                return dataframe
            return True

        return False


    def normalize_date_delimiters(
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

        total_rows: int = len(dataframe)
        column_index: int = dataframe.columns.get_loc(column_name)

        for start_index in range(0, total_rows, chunk_size):
            end_index: int = min(start_index + chunk_size, total_rows)

            series_chunk = dataframe.iloc[start_index:end_index, column_index]

            series_chunk = (
                series_chunk
                .str.replace("/", "-", regex=False)
                .str.replace(" ", "-", regex=False)
                .str.replace("_", "-", regex=False)
                .str.replace(".", "-", regex=False)
            )

            dataframe.iloc[start_index:end_index, column_index] = series_chunk

        del total_rows
        del column_index

        if not inplace:
            return dataframe
        return True


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

        total_rows: int = len(dataframe)
        column_index: int = dataframe.columns.get_loc(column_name)

        for start_index in range(0, total_rows, chunk_size):
            end_index: int = min(start_index + chunk_size, total_rows)

            series_chunk = dataframe.iloc[start_index:end_index, column_index]

            # Fix d/mm/yy and d/m/yy to 0d/mm/yy and 0d/m/yy
            series_chunk = series_chunk.str.replace_with_backrefs(
                r'^(?:\d{1}[^\w\d]\d{1,2}[^\w\d]\d{2,4})$',
                '0\\0',
            )

            # Fix dd/m/yy to 0d/0m/yy
            series_chunk = series_chunk.str.replace_with_backrefs(
                r'^(\d{2}[^\w\d])(\d{1}[^\w\d]\d{2,4})$',
                "\\1|\\2",
            )
            
            # unfortunately it is necessary to use the "|" marker, because luffy understands "\\10" or "\\1'+0+\\2" as 10 or group 0
            series_chunk = series_chunk.str.replace("|", "0", regex=False)

            dataframe.iloc[start_index:end_index, column_index] = series_chunk

        del column_index
        del total_rows
          
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
        Adiciona o time de uma coluna timedelta a uma coluna datetime.
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


    def normalize_mixed_dates(
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

        # aplicação de regex de data em chunks
        combined_regex: str = combine_regex(regex_pattern_date)

        total_rows: int = len(dataframe)
        column_index: int = dataframe.columns.get_loc(column_name)

        for start_index in range(0, total_rows, chunk_size):
            end_index: int = min(start_index + chunk_size, total_rows)

            series_chunk = dataframe.iloc[start_index:end_index, column_index]

            mask = series_chunk.str.match(combined_regex)

            # Converte valores inválidos para None
            series_chunk = series_chunk.where(mask, None)

            # Converte para cada padrão de data válido
            for pattern in regex_pattern_date:
                mask_pattern = series_chunk.str.match(pattern["regex"])
                series_chunk.loc[mask_pattern] = cudf.to_datetime(series_chunk.loc[mask_pattern], format=pattern["format"])

            dataframe.iloc[start_index:end_index, column_index] = series_chunk

        del total_rows
        del column_index

        dataframe[column_name] = dataframe[column_name].astype("datetime64[s]")

        if not inplace:
            return dataframe
        return True
