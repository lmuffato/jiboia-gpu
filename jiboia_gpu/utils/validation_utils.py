from typing import ClassVar
import cudf


class CudfSupportedDtypes:  
    str_types: ClassVar[list[str]] = ["object", "string"]
    int_types: ClassVar[list[str]] = ["int8", "int16", "int32", "int64"]
    uint_types: ClassVar[list[str]] = ["uint32", "uint64"]
    float_types: ClassVar[list[str]] = ["float32", "float64"]
    decimal_types: ClassVar[list[str]] = ["Decimal32Dtype", "Decimal64Dtype", "Decimal128Dtype"]
    numeric_types: ClassVar[list[str]] = int_types + uint_types + float_types + decimal_types
    bool_types: ClassVar[list[str]] = ["bool", "boolean"]
    datetime_types: ClassVar[list[str]] = ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]
    timedelta_types: ClassVar[list[str]] = ["timedelta64[s]", "timedelta64[ms]", "timedelta64[us]", "timedelta64[ns]"]
    category_types: ClassVar[list[str]] = ["CategoricalDtype"]
    struct_types: ClassVar[list[str]] = ["StructDtype"]


def is_valid_to_normalize(
    series: cudf.Series,
    valid_types: list[str] = [],
    invalid_types: list[str] = [],
) -> bool:

    series_type: str = str(series.dtype)

    is_empty: bool = series.isna().sum() == series.size

    if is_empty:
        return False
    
    is_valid_type: bool = True
    is_invalid_type: bool = True

    if valid_types:
        is_valid_type = series_type in valid_types

    if invalid_types:
        is_invalid_type = series_type not in invalid_types
    
    valid_series: bool = is_valid_type & is_invalid_type

    return valid_series
