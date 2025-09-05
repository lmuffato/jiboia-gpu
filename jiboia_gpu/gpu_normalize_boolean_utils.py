import cudf


RAW_BOOLEAN_VALUES: list[tuple] = [
    ("True", "False"),       # Strings capitalizadas
    ("true", "false"),       # Strings minúsculas
    ("TRUE", "FALSE"),       # Strings maiúsculas
    ("yes", "no"),           # Configs / respostas curtas
    ("Yes", "No"),           # Capitalizadas
    ("YES", "NO"),           # Maiúsculas
    ("y", "n"),              # Abreviações minúsculas
    ("Y", "N"),              # Abreviações maiúsculas
    ("on", "off"),           # Configs de sistemas minúsculas
    ("On", "Off"),           # Capitalizadas
    ("ON", "OFF"),           # Maiúsculas
    ("1", "0"),              # Strings numéricas
    ("t", "f"),              # Abreviações PostgreSQL minúsculas
    ("T", "F")               # Abreviações PostgreSQL maiúsculas
]


BOOL_PATTERN = (
    r'^(?:'
    r'True|False|'
    r'true|false|'
    r'TRUE|FALSE|'
    r'yes|no|'
    r'Yes|No|'
    r'YES|NO|'
    r'y|n|'
    r'Y|N|'
    r'on|off|'
    r'On|Off|'
    r'ON|OFF|'
    r'1|0|'
    r't|f|'
    r'T|F'
    r')$'
)


def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


def print_job_normalize_bool_done(
        column_name: str,
        column_type: str
    ) -> None:
        print(
            print_text_green("Done!"),
            "column",
            print_text_yellow(column_name),
            "converted to",
            print_text_yellow(column_type)
        )


class GPUNormalizeBooleanUtils:
    @staticmethod
    def normalize_bool(current_df: cudf.DataFrame) -> None:
        for column_name in current_df.columns:
            GPUNormalizeBooleanUtils.convert_column_to_bool(
                current_df=current_df,
                column_name=column_name
            )

    @staticmethod
    def convert_column_to_bool(
        current_df: cudf.DataFrame,
        column_name: str
    ) -> bool:
        size: int = current_df[column_name].size

        # Coluna vazia
        if size == 0:
            return False

        # Coluna sem dados
        if current_df[column_name].notna().sum() == 0:
            return False

        if current_df[column_name].dtype not in ["object", "string"]:
            return False
        
        # A coluna não deve ser um tipo de booleano
        if current_df[column_name].dtype == 'bool':
            return False

        # Verifica as primeiras 10 primeiras linhas ou todo a coluna no caso de ser menor que 10
        rows_size_to_check: int = size if size <= 10 else 10

        preliminary_nor_null_rows_size: int = (
            current_df[column_name]
            .head(rows_size_to_check)
            .notna()
            .sum()
        )

        # Análise prévia da coluna considerando um número limitado de linhas
        for raw_boolean_value in RAW_BOOLEAN_VALUES:    
            preliminary_compatible_rows: int = (
                current_df[column_name]
                .dropna()
                .head(rows_size_to_check)
                .isin(raw_boolean_value)
                .sum()
            )

            # Se há indício de que pode ser compatível considerando as linhas analisadas
            if preliminary_compatible_rows == preliminary_nor_null_rows_size:

                # Análise profunda considerando todas as linhas
                notna_row_values: cudf.Series = (
                    current_df[column_name]
                    .dropna()
                    .isin(raw_boolean_value)
                )
                
                # Se todas as linhas não nulas são compatíveis então pode-se converter em bool
                if notna_row_values.sum() == current_df[column_name].notna().sum():
                    current_df[column_name] = (
                        current_df[column_name]
                        .map(
                            {
                                raw_boolean_value[0]: True,
                                raw_boolean_value[1]: False
                            }
                        )
                        .astype("boolean")
                    )
                    print_job_normalize_bool_done(column_name=column_name, column_type="boolean")
