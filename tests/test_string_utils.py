# import os
# import sys
 

# testar:
# pytest -v

# cobertura de testes:
# pytest --cov=jiboia_gpu --cov-report=term-missing

# # Diretório raiz
# workdir: str = "../../app/"
# os.chdir(workdir)

# current_workdir = os.getcwd()
# print(current_workdir)
# # %%
# # print(os.listdir())
# # para o jiboi-gpu
# sys.path.append('../../app/jiboia-gpu/')
# # %%
# import pytest
# import cudf
# import pandas as pd
# from jiboia_gpu.string_utils import StringUtils

# string_rows: list = [
#     "My PyTest test 00",
#     " My PyTest test 01",
#     "My PyTest test 02 ",
#     " My PyTest test 03 ",
#     " ",
#     "  ",
#     "    ",
#     "My PyTest   test  07"
    
# ]

# test_df: pd.DataFrame = pd.DataFrame({
#     "string_with_spaces": string_rows
# })

# StringUtils.normalize(test_df)

# print(test_df)
# %%
import pytest
import cudf
import pandas as pd
# from jiboia_gpu.string_utils import StringUtils
from jiboia_gpu.string_utils import StringUtils

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

# sss = test_df["str_witch_spaces"].to_arrow().to_pylist()
# print(sss)

def test_normalize() -> None:
    multiple_spaces_before: int = (
        test_df["str_witch_spaces"]
        .str.contains(r'(^\s+|\s+$|\s{2,})', regex=True)
    ).sum()
    
    StringUtils.normalize(
        dataframe=test_df,
        to_case=None,
        to_ASCII=False
    ) == None

    multiple_spaces_after: int = (
        test_df["str_witch_spaces"]
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
    assert (test_df["str_witch_spaces"].to_arrow().to_pylist() == expected)

# del test_df
 
# %%


# StringUtils.normalize(test_df)

# cpu_df = test_df.to_pandas()

# A expressão regular `(^\s+|\s+$|\s{2,})` busca por:
# - ^\s+  (espaço no início)
# - |     (OU)
# - \s+$  (espaço no fim)
# - |     (OU)
# - \s{2,} (dois ou mais espaços no meio)

# multiple_spaces_mask = (
#     test_df["str_witch_spaces"]
#     .str.contains(r'(^\s+|\s+$|\s{2,})', regex=True)
# )
# print(multiple_spaces_mask)
