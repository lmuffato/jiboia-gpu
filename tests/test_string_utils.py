import cudf
from jiboia_gpu.string.string_utils import StringUtils


str_normal: list = [
    "King Cobra",
    "Jiboia",
    "Coral Verdadeira",
    "Jararaca",
    "Surucucu",
    "Naja",
    "Black Mamba",
    "Taipan",
    "sea snake",
    "solid snake",
]


# Irregular string = 7
str_witch_spaces: list = [
    "My PyTest test 00",
    " My PyTest test 01",
    "My PyTest test 02 ",
    " My PyTest test 03 ",
    " ",
    "  ",
    "    ",
    "My PyTest   test  07",
    "My PyTest test 08",
    "My PyTest test 09",
]

str_witch_category: list = [
    "car",
    "car",
    "bus",
    "bus",
    "bike",
    "bike",
    "bike",
    "bike",
    "bike",
    "truck",
]

test_df: cudf.DataFrame = cudf.DataFrame({
    "str_normal": str_normal,
    "str_witch_spaces": str_witch_spaces,
    "str_witch_category": str_witch_category
})


def test_normalize() -> None:
    column_name="str_witch_spaces"

    multiple_spaces_before: int = (
        test_df[column_name]
        .str.contains(r'(^\s+|\s+$|\s{2,})', regex=True)
    ).sum()
    
    StringUtils.normalize(
        dataframe=test_df,
        column_name=column_name,
        to_case=None,
        to_ASCII=False,
        inplace=True
    ) == None

    multiple_spaces_after: int = (
        test_df[column_name]
        .str.contains(r'(^\s+|\s+$|\s{2,})', regex=True)
    ).sum()

    expected = [
        "My PyTest test 00",
        "My PyTest test 01",
        "My PyTest test 02",
        "My PyTest test 03",
        "",
        "",
        "",
        "My PyTest test 07",
        "My PyTest test 08",
        "My PyTest test 09",
    ]

    assert (multiple_spaces_before > 0)
    assert (multiple_spaces_after == 0)
    assert (test_df[column_name].to_arrow().to_pylist() == expected)
