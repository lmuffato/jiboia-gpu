import cudf
from typing import Literal
from .df_utils import DfUtils


def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


MIXED_PATTERN = (
    r'^(?:'
    # === NÚMEROS ===
    r'-?\d+'                         # inteiro simples (ex.: 2003, -42)
    r'|'
    r'-?\d+(?:[.,]\d+)'              # decimal com ponto ou vírgula
    r'|'
    r'-?\d+(?:\.\d+)?[eE][+-]?\d+'   # notação científica
    r'|'
    r'-?\d{1,3}(?:\.\d{3})*,\d+'     # dd.ddd,dd
    r'|'
    # === DATAS ===
    r'\d{4}[/\-._ ](0[1-9]|1[0-2])[/\-._ ](0[1-9]|[12]\d|3[01])'  # YYYY/MM/DD
    r'|'
    r'(0[1-9]|[12]\d|3[01])[/\-._ ](0[1-9]|1[0-2])[/\-._ ]\d{4}'  # DD/MM/YYYY
    r'|'
    r'(0[1-9]|1[0-2])[/\-._ ](0[1-9]|[12]\d|3[01])[/\-._ ]\d{4}'  # MM/DD/YYYY
    r'|'
    r'(0[1-9]|[12]\d|3[01])[/\-._ ]\d{2}[/\-._ ]\d{2}'            # DD/MM/YY
    r'|'
    r'\d{2}[/\-._ ](0[1-9]|[12]\d|3[01])[/\-._ ]\d{2}'            # YY/MM/DD
    r'|'
    r'\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])'                  # YYYYMMDD
    r'|'
    # === HORAS ===
    r'([01]\d|2[0-3]):[0-5]\d'            # hh:mm
    r'|'
    r'([01]\d|2[0-3]):[0-5]\d:[0-5]\d'    # hh:mm:ss
    r'|'
    r'([01]\d|2[0-3])[0-5]\d ?UTC'        # hhmm UTC
    r'|'
    r'([01]\d|2[0-3])[0-5]\dUTC'          # hhmmUTC
    r'|'
    # === BOOLEANOS ===
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
    r't|f|'
    r'T|F'
    r')$'
)



def print_job_normalize_string_done(
    to_case: None|str,
    to_ASCII: bool = False
) -> None:

    to_case_msg: str = ''

    if to_case == "upper":
        to_case_msg = "uppercase"
    if to_case == "lower":
        to_case_msg = "lowercase"

    to_ASCII_msg: str = "ASCII" if to_ASCII else ""

    msg: str = " ".join(filter(None, [to_case_msg, to_ASCII_msg]))

    # msg: str = "to_case_msg"

    if len(msg) > 0:
        print(
            print_text_green("Done!"),
            "all",
            print_text_yellow("string"),
            "were converted to",
            print_text_yellow(msg)
        )


def print_job_normalize_string_to_ascii_done(
    case_type: None|str = None,
) -> None:
    
    case_type_msg: str = ''

    if case_type == "upper":
        case_type_msg = "uppercase"
    if case_type == "low":
        case_type_msg = "lowercase"

    to_ASCII_msg: str = "ASCII"

    msg: str = " ".join(filter(None, [to_ASCII_msg, case_type_msg]))

    # msg: str = "to_ASCII_msg"

    if len(msg) > 0:
        print(
            print_text_green("Done!"),
            "all",
            print_text_yellow("string"),
            "were converted to",
            print_text_yellow(msg)
        )


def print_job_normalize_space_done() -> None:
    print(
        print_text_green("Done!"),
        "all duplicate and edge",
        print_text_yellow("spaces"),
        "have been",
        print_text_yellow("removed")
    )


def print_job_create_category_done(
    column_name: str,
) -> None:

    print(
        print_text_green("Done!"),
        "the column",
        print_text_yellow(column_name),
        "was converted to a",
        print_text_yellow("category")
    )



class StringUtils:
    # == CONVERTE TODAS AS COLUNAS EM STRING == #
    @staticmethod
    def convert_df_to_string(dataframe: cudf.DataFrame):
        for column in dataframe.columns:
            dataframe[column] = (
                dataframe[column]
                    .astype(str)
            )
        print('columns with boolean types have been normalized')


    # == CRIA CATEGORIAS DE COLUNAS DE STRING, SE FOR POSSÍVEL == #
    @staticmethod
    def normalize_category(dataframe: cudf.DataFrame):
        for column_name in dataframe.columns:
            # Verifica se a coluna é do tipo string
            if not StringUtils.is_string(dataframe, column_name):
                continue
            # Verifica se mesmo sendo string, é uma coluna de outro tipo de dados como int, bool, float
            if not StringUtils.is_probably_text(dataframe, column_name):
                continue
            StringUtils.to_category(
                dataframe,
                column_name
            )


    # == REMOVE TODOS OS ESPAÇOS DUPLOS, NO COMEÇO E FINAL DE STRING == #
    @staticmethod
    def normalize_spaces(dataframe: cudf.DataFrame) -> None:
        print(111)
        for column_name in dataframe.columns:
            size: int = dataframe[column_name].size

            # Coluna vazia
            if size == 0:
                continue

            # Coluna sem dados
            if dataframe[column_name].notna().sum() == 0:
                continue

            # A coluna deve ser do tipo str
            if dataframe[column_name].dtype not in ["object", "string"]:
                continue

            dataframe[column_name] = (
                dataframe[column_name]
                .str.normalize_spaces()
                .str.strip()
            )
        print_job_normalize_space_done()


    # == NORMALIZA TODAS AS STRINGS DO DF == #
    @staticmethod
    def normalize(
        dataframe: cudf.DataFrame,
        column_name: None|str=None,
        to_case: None|Literal['lower', 'upper']=None,
        to_ASCII: bool=False,
        inplace: None|bool=False,
        log: None|bool=True,
    ) -> None|cudf.DataFrame:
        if not inplace:
            dataframe: cudf.DataFrame = dataframe.copy()

        # if column_name:
    
        for column_name in dataframe.columns:
            if column_name not in dataframe.columns or dataframe[column_name].empty:
                continue

            if dataframe[column_name].dtype not in ["object", "string"]:
                continue
            
            if dataframe[column_name].isnull().all():
                continue
            
            processed_column = (
                dataframe[column_name]
                .str.normalize_spaces()
                .str.strip()
            )               

            if to_case == "low":
                processed_column = (
                    processed_column
                    .str.lower()
                )

                if to_ASCII:
                    processed_column = (
                    processed_column
                        .str.replace(r"[áàâãä]", "a", regex=True)
                        .str.replace(r"[éèêë]", "e", regex=True)
                        .str.replace(r"[íìîï]", "i", regex=True)
                        .str.replace(r"[óòôõö]", "o", regex=True)
                        .str.replace(r"[úùûü]", "u", regex=True)
                        .str.replace(r"[ç]", "c", regex=True)
                )

            if to_case == "upper":
                processed_column = (
                    processed_column
                    .str.upper()
                )
                if to_ASCII:
                    processed_column = (
                        processed_column
                        .str.replace(r"[ÁÀÂÃÄ]", "A", regex=True)
                        .str.replace(r"[ÉÈÊË]", "E", regex=True)
                        .str.replace(r"[ÍÌÎÏ]", "I", regex=True)
                        .str.replace(r"[ÓÒÔÕÖ]", "O", regex=True)
                        .str.replace(r"[ÚÙÛÜ]", "U", regex=True)
                        .str.replace(r"[Ç]", "C", regex=True)
                    )

            if to_ASCII and not to_case:
                processed_column = (
                    processed_column
                        .str.replace(r"[áàâãä]", "a", regex=True)
                        .str.replace(r"[ÁÀÂÃÄ]", "A", regex=True)
                        .str.replace(r"[éèêë]", "e", regex=True)
                        .str.replace(r"[ÉÈÊË]", "E", regex=True)
                        .str.replace(r"[íìîï]", "i", regex=True)
                        .str.replace(r"[ÍÌÎÏ]", "I", regex=True)
                        .str.replace(r"[óòôõö]", "o", regex=True)
                        .str.replace(r"[ÓÒÔÕÖ]", "O", regex=True)
                        .str.replace(r"[úùûü]", "u", regex=True)
                        .str.replace(r"[ÚÙÛÜ]", "U", regex=True)
                        .str.replace(r"[ç]", "c", regex=True)
                        .str.replace(r"[Ç]", "C", regex=True)
                    )

            dataframe[column_name] = processed_column

        if log:
            print_job_normalize_space_done()
        if log and (to_case or to_ASCII):
            print_job_normalize_string_done(to_case=to_case, to_ASCII=to_ASCII)

        if inplace:
            return None

        return dataframe


    @staticmethod
    def to_ascii(
        dataframe: cudf.DataFrame,
        case_type: None|str = None
    ) -> None:
        for column_name in dataframe.columns:
            size: int = dataframe[column_name].size

            if size == 0:
                print("empty column")
                continue

            if dataframe[column_name].dtype not in ["object", "string"]:
                continue
            
            if not case_type:
                dataframe[column_name] = (
                    dataframe[column_name]
                    .str.replace(r"[áàâãä]", "a", regex=True)
                    .str.replace(r"[ÁÀÂÃÄ]", "A", regex=True)
                    .str.replace(r"[éèêë]", "e", regex=True)
                    .str.replace(r"[ÉÈÊË]", "E", regex=True)
                    .str.replace(r"[íìîï]", "i", regex=True)
                    .str.replace(r"[ÍÌÎÏ]", "I", regex=True)
                    .str.replace(r"[óòôõö]", "o", regex=True)
                    .str.replace(r"[ÓÒÔÕÖ]", "O", regex=True)
                    .str.replace(r"[úùûü]", "u", regex=True)
                    .str.replace(r"[ÚÙÛÜ]", "U", regex=True)
                    .str.replace(r"[ç]", "c", regex=True)
                    .str.replace(r"[Ç]", "C", regex=True)
                )
                continue

            if case_type == 'lower':
                dataframe[column_name] = (
                    dataframe[column_name]
                    .str.replace(r"[áàâãä]", "a", regex=True)
                    .str.replace(r"[éèêë]", "e", regex=True)
                    .str.replace(r"[íìîï]", "i", regex=True)
                    .str.replace(r"[óòôõö]", "o", regex=True)
                    .str.replace(r"[úùûü]", "u", regex=True)
                    .str.replace(r"[ç]", "c", regex=True)
                )
                continue

            if case_type == 'upper':
                dataframe[column_name] = (
                    dataframe[column_name]
                    .str.replace(r"[ÁÀÂÃÄ]", "A", regex=True)
                    .str.replace(r"[ÉÈÊË]", "E", regex=True)
                    .str.replace(r"[ÍÌÎÏ]", "I", regex=True)
                    .str.replace(r"[ÓÒÔÕÖ]", "O", regex=True)
                    .str.replace(r"[ÚÙÛÜ]", "U", regex=True)
                    .str.replace(r"[Ç]", "C", regex=True)
                )
                continue

        print_job_normalize_string_to_ascii_done(case_type=case_type)
        return

    @staticmethod
    def to_upercase(
        dataframe: cudf.DataFrame,
    ) -> None:
        for column_name in dataframe.columns:
            size: int = dataframe[column_name].size

            if size == 0:
                continue

            if dataframe[column_name].dtype in ["object", "string"]:

                dataframe[column_name] = (
                    dataframe[column_name]
                    .str.upper()
                )
                continue
        
        print_job_normalize_string_done(to_case='upper')


    @staticmethod
    def normalize_unique_values_in_column(
        dataframe: cudf.DataFrame, 
        column_name: str, 
        target_value: str, 
        values_to_replace: list[str]
    ) -> None:
        '''
        Substitui valores específicos de uma coluna por um valor alvo.
        
        Parâmetros:
        - dataframe: DataFrame contendo os dados.
        - column: Nome da coluna onde a substituição ocorrerá.
        - target_value: O valor que substituirá os valores encontrados.
        - values_to_replace: Lista de valores que serão substituídos pelo valor alvo.
        '''
        size: int = dataframe[column_name].size
        if size == 0:
            print("empty column")
            return

        if dataframe[column_name].dtype not in ["object", "string"]:
            return

        # Substitui os valores encontrados pela coluna `column` que estão na lista `values_to_replace` com `target_value`
        dataframe[column_name].replace(
            values_to_replace,
            target_value, 
            inplace=True
        )


    @staticmethod
    def is_category(
        dataframe: cudf.DataFrame,
        column_name: str
    ) -> None:
        # Coluna sem dados
        if dataframe[column_name].notna().sum() == 0:
            return False
        
        # A coluna deve ser do tipo str
        if dataframe[column_name].dtype not in ["object", "string"]:
            return False
        
        unique_values = dataframe[column_name].unique()
        not_na_size = dataframe[column_name].notna().size

        # Se os valores únicos são mais de 60% das linhas, não vale a pena
        if (round((unique_values.size / not_na_size) * 100) > 60):
            return False
        
        return True


    @staticmethod
    def to_category(
        dataframe: cudf.DataFrame,
        column_name: str
    ) -> None:
        # Coluna sem dados
        if dataframe[column_name].notna().sum() == 0:
            return
        
        # A coluna deve ser do tipo str
        if dataframe[column_name].dtype not in ["object", "string"]:
            return
        
        unique_values = dataframe[column_name].unique()

        unique_values = unique_values.sort_values()
        unique_values = unique_values.reset_index(drop=True)
        
        categorical_dtype = cudf.CategoricalDtype(categories=unique_values, ordered=True)
    
        dataframe[column_name] = dataframe[column_name].astype(categorical_dtype)
        print_job_create_category_done(column_name)


    @staticmethod
    def is_string(
        dataframe: cudf.DataFrame,
        column_name: str
    ) -> None:
        if dataframe[column_name].dtype in ["object", "string"]:
            return True
        return False


    @staticmethod
    def to_categories(
        dataframe: cudf.DataFrame,
        column_names: list[str]
    ) -> None:
        for column_name in column_names:

            if dataframe[column_name].notna().sum() == 0:
                continue
            
            if dataframe[column_name].dtype not in ["object", "string"]:
                continue

            unique_values = dataframe[column_name].unique()
            not_na_size = dataframe[column_name].notna().size

            # Os dados únicos devem ser pelo menos 60% oi mais do total
            if (round((unique_values.size / not_na_size) * 100) > 60):
                continue

            unique_values = unique_values.sort_values()
            unique_values = unique_values.reset_index(drop=True)
            
            categorical_dtype = cudf.CategoricalDtype(categories=unique_values, ordered=True)
        
            dataframe[column_name] = dataframe[column_name].astype(categorical_dtype)
            print_job_create_category_done(column_name)


    @staticmethod
    def is_probably_text(
        dataframe: cudf.DataFrame,
        column_name: str
    ) -> bool:
        """
        Verifica se a coluna mesmo sendo string, tem dados tipicos de outros tipos como
        int, float, bool, datetime, timedelta
        """
        has_a_not_string_data = (
            DfUtils.infer_by_sample(
                series=dataframe[column_name],
                regex_patern=MIXED_PATTERN,
                n_parts=100
                )
            )

        # Se contem apenas dados de string, então é uma coluna string real
        return (not has_a_not_string_data)


    @staticmethod
    def normalize_unique_values(
        dataframe: cudf.DataFrame,
        column_name: str,
        mapping_dict: dict[str, list[str]],
        null_values: None|list[str] = []
    ) -> None:
        """
        Normaliza e limpa uma coluna em um cuDF DataFrame de forma inplace.

        Args:
            dataframe: O DataFrame a ser modificado.
            column_name: O nome da coluna a ser normalizada.
            mapping_dict: Dicionário no formato {'novo_valor': ['antigo1', 'antigo2']}.
            null_values: Lista opcional de valores a serem convertidos para nulo.
        """
        replace_map = {
            old_val: new_val
            for new_val, old_vals_list in mapping_dict.items()
            for old_val in old_vals_list
        }

        # Substitiu os valores para o valor da chave do dict
        dataframe[column_name] = dataframe[column_name].replace(replace_map)

        # Substitiu os valores nulos
        if null_values:
            dataframe[column_name] = dataframe[column_name].replace(
                null_values, cudf.NA
            )
