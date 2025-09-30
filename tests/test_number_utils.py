import cudf
import cupy as cp
import cupy.typing as npt
import string
from jiboia_gpu.number.number_utils import NumberUtils


DF_SIZE: int = 10
COLUMN_NAME: str = "col_name"


def generate_df_int_numbers(
    df_size: None|int=10,
    column_name: None|str="col_name",
    dtype: None|npt.DTypeLike = cp.int64,
) -> cudf.DataFrame:
    test_values: cp.ndarray = cp.linspace(
        cp.iinfo(cp.int8).min+1,
        cp.iinfo(cp.int8).max-1,
        df_size
    ).round().astype(dtype)

    return cudf.DataFrame({
        column_name: test_values
    })


def generate_df_scientific_numbers(
    df_size: None|int=10,
    column_name: None|str="col_name"
) -> cudf.DataFrame:
    test_values: list[str] = []

    for i in range(df_size):
        value: int = cp.iinfo(cp.int16).max + i
        value_sci: str = f"{value:.4e}"
        test_values.append(value_sci)

    df = cudf.DataFrame({
        column_name: test_values
    })

    df[column_name] = df[column_name].astype(str)

    return df


def generate_df_float_numbers(
    df_size: None|int = 10,
    column_name: None | str = "col_name",
    dtype: None|npt.DTypeLike = cp.float64,
) -> cudf.DataFrame:
    base_value: float = float(cp.iinfo(cp.int16).max)

    increments: cp.ndarray = cp.arange(1, df_size + 1, dtype=cp.float64) * 0.1

    test_values: cp.ndarray = (base_value + increments).astype(dtype)

    return cudf.DataFrame({column_name: test_values})


def generate_df_random_letters(
    df_size: None|int = 10,
    column_name: None|str = "col_name",
    string_length: None|int = 5,
) -> cudf.DataFrame:

    letters: str = string.ascii_letters
    result_list: list[str] = []

    for _ in range(df_size):
        random_letters: list[str] = [letters[int(cp.random.randint(0, len(letters)))] for _ in range(string_length)]
        result_list.append("".join(random_letters))

    return cudf.DataFrame({column_name: result_list})


def generate_df_all_na(
    df_size: None|int = 10,
    column_name: None|str = "col_name",
) -> cudf.DataFrame:
    return cudf.DataFrame({column_name: [cudf.NA] * df_size})


def generate_df_float_formatted(
    df_size: None|int = 10,
    column_name: None|str = "col_name",
    base_value: None|float = float(cp.iinfo(cp.int16).max),
    increment: None|float = 0.1,
    decimal_sep: None|str = ",",
    thousand_sep: None|str = ".",
) -> cudf.DataFrame:
    result_list: list[str] = []

    for i in range(df_size):
        value: float = base_value + (i + 1) * increment
        value_str: str = f"{value:,.1f}"

        value_str = value_str.replace(",", "TEMP")
        value_str = value_str.replace(".", decimal_sep)
        value_str = value_str.replace("TEMP", thousand_sep)

        result_list.append(value_str)

    return cudf.DataFrame({column_name: result_list})


# ---- TESTS ---- #
def test_normalize_downcast_int64_to_int8() -> None:
    df: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME)

    df[COLUMN_NAME] = df[COLUMN_NAME].astype(str)

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    assert (df[COLUMN_NAME].dtype == cp.int8)


def test_normalize_downcast_int64_to_int16() -> None:
    COLUMN_NAME: str = "col_name"
    df: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME)

    df[COLUMN_NAME].iloc[0] = cp.iinfo(cp.int8).min - 1
    df[COLUMN_NAME].iloc[-1] = cp.iinfo(cp.int8).max + 1

    df[COLUMN_NAME] = df[COLUMN_NAME].astype(str)

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    assert (df[COLUMN_NAME].dtype == cp.int16)


def test_normalize_downcast_int64_to_int32() -> None:
    column_name: str = "col_name"
    df: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME)

    df[column_name].iloc[0] = cp.iinfo(cp.int16).min - 1
    df[column_name].iloc[-1] = cp.iinfo(cp.int16).max + 1

    df[column_name] = df[column_name].astype(str)

    NumberUtils.normalize(
        dataframe=df,
        column_name=column_name,
        inplace=True,
        show_log=False
    )

    assert (df[column_name].dtype == cp.int32)


def test_normalize_downcast_false_float64_to_int8() -> None:
    df: cudf.DataFrame = generate_df_int_numbers(dtype=cp.float64, df_size=DF_SIZE, column_name=COLUMN_NAME)
 
    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    assert (df[COLUMN_NAME].dtype == cp.int8)

    df[COLUMN_NAME] = df[COLUMN_NAME].astype(str)

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    assert (df[COLUMN_NAME].dtype == cp.int8)


def test_normalize_downcast_float64_to_float32() -> None:
    df: cudf.DataFrame = generate_df_int_numbers(dtype=cp.float64, df_size=DF_SIZE, column_name=COLUMN_NAME)

    decimal_values = cp.arange(1, (DF_SIZE+1)) / DF_SIZE

    df[COLUMN_NAME] = df[COLUMN_NAME] + decimal_values

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    assert (df[COLUMN_NAME].dtype == cp.float64)

    df[COLUMN_NAME] = df[COLUMN_NAME].astype(str)

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    assert (df[COLUMN_NAME].dtype == cp.float64)


def test_normalize_convert_scientific_float_to_int32() -> None:
    df: cudf.DataFrame = generate_df_scientific_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME)

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    assert (df[COLUMN_NAME].dtype == cp.int32)


def test_normalize_convert_mixed_numeric_to_int32() -> None:
    df_int8: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_false_float: cudf.DataFrame = generate_df_int_numbers(dtype=cp.float64, df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_scitific_num_str: cudf.DataFrame = generate_df_scientific_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)

    df: cudf.DataFrame = cudf.concat([df_int8, df_false_float, df_scitific_num_str], ignore_index=True)

    not_null_before: int = df[COLUMN_NAME].notna().sum()

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    not_null_after: int = df[COLUMN_NAME].notna().sum()

    assert (not_null_before == not_null_after)
    assert (df[COLUMN_NAME].dtype == cp.int32)


def test_normalize_convert_mixed_numeric_to_float32() -> None:
    df_int8: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_float32: cudf.DataFrame = generate_df_float_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_scitific_num_str: cudf.DataFrame = generate_df_scientific_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)

    df: cudf.DataFrame = cudf.concat([df_int8, df_float32, df_scitific_num_str], ignore_index=True)

    not_null_before: int = df[COLUMN_NAME].notna().sum()

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    not_null_after: int = df[COLUMN_NAME].notna().sum()

    assert (not_null_before == not_null_after)
    assert (df[COLUMN_NAME].dtype == cp.float64)


def test_normalize_convert_bad_formatted_number_to_float32() -> None:
    df_bad_formatted_number_with_dot_and_comma: cudf.DataFrame = generate_df_float_formatted(
        df_size=DF_SIZE,
        column_name=COLUMN_NAME,
        decimal_sep=",",
        thousand_sep="."
    ).astype(str)

    df_bad_formatted_number_with_comma: cudf.DataFrame = generate_df_float_formatted(
        df_size=DF_SIZE,
        column_name=COLUMN_NAME,
        decimal_sep=",",
        thousand_sep=""
    ).astype(str)
    
    df: cudf.DataFrame = cudf.concat([df_bad_formatted_number_with_dot_and_comma, df_bad_formatted_number_with_comma], ignore_index=True)

    not_null_before: int = df[COLUMN_NAME].notna().sum()

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    not_null_after: int = df[COLUMN_NAME].notna().sum()

    assert (not_null_before == not_null_after)
    assert (df[COLUMN_NAME].dtype == cp.float64)


def test_normalize_convert_mixed_and_null_numeric_to_float64() -> None:
    df_int8: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_false_float: cudf.DataFrame = generate_df_int_numbers(dtype=cp.float64, df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_scitific_num_str: cudf.DataFrame = generate_df_scientific_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_null: cudf.DataFrame = generate_df_all_na(df_size=DF_SIZE*2, column_name=COLUMN_NAME)

    df_bad_formatted_number_with_dot_and_comma: cudf.DataFrame = generate_df_float_formatted(
        df_size=DF_SIZE,
        column_name=COLUMN_NAME,
        decimal_sep=",",
        thousand_sep="."
    ).astype(str)

    df_bad_formatted_number_with_comma: cudf.DataFrame = generate_df_float_formatted(
        df_size=DF_SIZE,
        column_name=COLUMN_NAME,
        decimal_sep=",",
        thousand_sep=""
    ).astype(str)

    df: cudf.DataFrame = cudf.concat(
        [
            df_int8,
            df_false_float,
            df_scitific_num_str,
            df_null,
            df_bad_formatted_number_with_dot_and_comma,
            df_bad_formatted_number_with_comma
        ]
        , ignore_index=True
    )

    not_null_before: int = df[COLUMN_NAME].notna().sum()

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        inplace=True,
        show_log=False
    )

    not_null_after: int = df[COLUMN_NAME].notna().sum()

    assert (not_null_before == not_null_after)
    assert (df[COLUMN_NAME].dtype == cp.float64)


def test_normalize_convert_mixed_with_letters_to_float32() -> None:
    """
    Se um df conter mais de 50% das linhas contendo apenas números válidos, 
    as linhas com strings serão convertidas em NA.
    """
    df_int8: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_float32: cudf.DataFrame = generate_df_float_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_scitific_num_str: cudf.DataFrame = generate_df_scientific_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_letters: cudf.DataFrame = generate_df_random_letters(df_size=DF_SIZE, column_name=COLUMN_NAME)
  
    df: cudf.DataFrame = cudf.concat([df_int8, df_float32, df_scitific_num_str, df_letters], ignore_index=True)

    not_null_before: int = df[COLUMN_NAME].notna().sum()

    not_numbers: int = df_letters.size

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        match_min_rate=50,       # Pelo menos, 50% das linhas precisa ser um número válido
        inplace=True,
        show_log=False
    )

    not_null_after: int = df[COLUMN_NAME].notna().sum()

    assert (not_null_before == (not_null_after + not_numbers))
    assert (df[COLUMN_NAME].dtype == cp.float64)


def test_normalize_do_not_convert_when_string_mix_is_greater_than_numeric_threshold() -> None:
    df_int8: cudf.DataFrame = generate_df_int_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_float32: cudf.DataFrame = generate_df_float_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_scitific_num_str: cudf.DataFrame = generate_df_scientific_numbers(df_size=DF_SIZE, column_name=COLUMN_NAME).astype(str)
    df_letters: cudf.DataFrame = generate_df_random_letters(df_size=DF_SIZE, column_name=COLUMN_NAME)
  
    df: cudf.DataFrame = cudf.concat([df_int8, df_float32, df_scitific_num_str, df_letters], ignore_index=True)

    not_null_before: int = df[COLUMN_NAME].notna().sum()

    NumberUtils.normalize(
        dataframe=df,
        column_name=COLUMN_NAME,
        match_min_rate=90,       # Pelo menos, 90% das linhas precisa ser um número válido
        inplace=True,
        show_log=False
    )

    not_null_after: int = df[COLUMN_NAME].notna().sum()

    assert (not_null_before == not_null_after)
    assert (df[COLUMN_NAME].dtype == "object")
