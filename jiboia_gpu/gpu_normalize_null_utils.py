import cudf
from typing import Literal


def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"

def print_job_normalize_null_done() -> None:
    print(
        print_text_green("Done!"),
        "all",
        print_text_yellow("null"),
        "values converted to",
        print_text_yellow("<NA>")
    )

# RAW_INVALID_VALUES: list[str] = [
#     '',
#     ' ',
#     'NA',
#     '<NA>',
#     '(NA)',
#     'N/A',
#     'nan',
#     'NaT',
#     'NULL',
#     '(NULL)',
#     'NONE',
#     'NULO',
#     'NAN',
#     'UNDEFINED',
#     'None',
#     'NAO INFORMADO',
#     'NÃO INFORMADO',
#     'null',
#     '(null)',
#     'undefined',
#     'NOT AVAILABLE',
#     'NOT APPLICABLE',
#     'MISSING',
#     'UNKNOWN',
#     '-',
#     '--',
#     '?',
#     'N.D.',
# ]


RAW_INVALID_VALUES: list[str] = [
    '',
    ' ',
    '-',
    '--',
    '?',
]

RAW_INVALID_UPPERCASE_VALUES: list[str] = ['',
 'NA',
 '<NA>',
 '(NA)',
 'N/A',
 'NAN',
 'NAT',
 'NULL',
 '(NULL)',
 'NONE',
 'NULO',
 'NAN',
 'UNDEFINED',
 'NONE',
 'NAO INFORMADO',
 'NÃO INFORMADO',
 'NOT AVAILABLE',
 'NOT APPLICABLE',
 'MISSING',
 'UNKNOWN',
 'N.D.'
]


RAW_INVALID_LOWERRCASE_VALUES: list[str] = [
    'n.d.'
    'na',
    '<na>',
    '(na)',
    'n/a',
    'nan',
    'nat',
    'null',
    '(null)',
    'none',
    'nulo',
    'nan',
    'undefined',
     'none',
     'nao informado',
     'não informado',
     'undefined',
     'not available',
     'not applicable',
     'missing',
     'unknown'
]




# --- Constantes com os valores inválidos ---
# Mantidos fora da classe como constantes globais

RAW_INVALID_VALUES: list[str] = [
    '', ' ', '-', '--', '?',
]

RAW_INVALID_UPPERCASE_VALUES: list[str] = [
    'NA', '<NA>', '(NA)', 'N/A', 'NAN', 'NAT', 'NULL', '(NULL)', 'NONE',
    'NA/NA', 'NULO', 'UNDEFINED', 'NAO INFORMADO', 'NÃO INFORMADO',
    'NOT AVAILABLE', 'NOT APPLICABLE', 'MISSING', 'UNKNOWN', 'N.D.'
]

RAW_INVALID_LOWERRCASE_VALUES: list[str] = [
    'na', '<na>', '(na)', 'n/a', 'nan', 'nat', 'null', '(null)', 'none',
    'na/na', 'nulo', 'undefined', 'nao informado', 'não informado',
    'not available', 'not applicable', 'missing', 'unknown', 'n.d.'
]


# # class GPUNormalizeNullUtils:
#     """
#     Classe otimizada para normalizar valores nulos em colunas de um cudf.DataFrame
#     utilizando operações 100% na GPU.
#     """

class GPUNormalizeNullUtils:
    @staticmethod
    def normalize_na(
        current_df: cudf.DataFrame,
        additional_null_values: list[str] = [],
        # case_type: Literal['low', 'upper'] | None = None,
    ):
        # current_df = current_df.fillna(cudf.NA)

        for column_name in current_df.columns:
            GPUNormalizeNullUtils.normalize_na_column(
                current_df=current_df,
                column_name=column_name,
                additional_null_values=additional_null_values,
                # case_type=case_type
            )
        print_job_normalize_null_done()


    # 1. Pré-computa os valores e os armazena na GPU como atributos de classe
    # O uso de 'set' remove duplicatas antes de criar a Series
    _GPU_RAW = cudf.Series(list(set(RAW_INVALID_VALUES)))
    _GPU_UPPER = cudf.Series(list(set(RAW_INVALID_UPPERCASE_VALUES)))
    _GPU_LOWER = cudf.Series(list(set(RAW_INVALID_LOWERRCASE_VALUES)))
    _GPU_ALL = cudf.concat([_GPU_RAW, _GPU_UPPER, _GPU_LOWER]).unique()

    @staticmethod
    def normalize_na_column(
        current_df: cudf.DataFrame,
        column_name: str,
        additional_null_values: list[str] = [],
        case_type: Literal['low', 'upper'] | None = None,
    ) -> None:

        if column_name not in current_df.columns or current_df[column_name].empty:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        if current_df[column_name].isnull().all():
            return

        # --- Seleção da lista de valores a serem substituídos (já na GPU) ---
        if case_type == 'low':
            values_to_replace = cudf.concat(
                [
                    GPUNormalizeNullUtils._GPU_RAW,
                    GPUNormalizeNullUtils._GPU_LOWER]
                )
        elif case_type == 'upper':
            values_to_replace = cudf.concat(
                [
                    GPUNormalizeNullUtils._GPU_RAW,
                    GPUNormalizeNullUtils._GPU_UPPER]
            )
        else:
            values_to_replace = GPUNormalizeNullUtils._GPU_ALL

        if additional_null_values:
            gpu_additional = cudf.Series(additional_null_values)
            values_to_replace = cudf.concat([values_to_replace, gpu_additional])

        values_to_replace = values_to_replace.unique()
        
        mask = current_df[column_name].isin(values_to_replace)

        current_df.loc[mask, column_name] = cudf.NA


    # @staticmethod
    # def normalize_na_column(
    #     current_df: cudf.DataFrame,
    #     column_name: str,
    #     additional_null_values: list[str] = []
    # ) -> None:        
    #     size: int = current_df[column_name].size

    #     # Coluna vazia
    #     if size == 0:
    #         return False
        
    #     # Coluna sem dados
    #     if current_df[column_name].notna().sum() == 0:
    #         return False

    #     # A coluna deve ser do tipo str
    #     if current_df[column_name].dtype not in ["object", "string"]:
    #         return False

    #     raw_invalid_values: list = RAW_INVALID_VALUES.copy()

    #     if additional_null_values:
    #         raw_invalid_values.extend(additional_null_values)
        
    #     current_df[column_name].replace(RAW_INVALID_VALUES, cudf.NA, inplace=True)

