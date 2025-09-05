import cudf
import cupy as cp
from .gpu_analysis_utils import GPUAnalysisUtils


SIGNED_INTEGER_TYPES: tuple[str] = (
    'int8',
    'int16',
    'int32',
    'int64'
)

UNSIGNED_INTEGER_TYPES: tuple[str] = (
    'uint32',
    'uint64'
)

FLOATING_TYPES: tuple[str] = (
    'float32',
    'float64'
)

NUMERIC_TYPES: tuple[str] = (
    'int8',
    'int16',
    'int32',
    'int64',
    'uint32',
    'uint64'
    'float32',
    'float64'
)


NUMERIC_PATTERN = (
    r'^(?:'
    # === NÚMEROS ===
    r'-?\d+'                         # inteiro simples (ex.: 2003, -42)
    r'|'
    r'-?\d+(?:[.,]\d+)'              # decimal com ponto ou vírgula
    r'|'
    r'-?\d+(?:\.\d+)?[eE][+-]?\d+'   # notação científica
    r'|'
    r'-?\d{1,3}(?:\.\d{3})*,\d+'     # dd.ddd,dd
    r')$'
)


# mixed_pattern = (
#     r'^(?:'
#         # === 1. Inteiro simples ===
#     r'-?\d+'
#     r'|'
#         # === 2. Números com milhar e/ou decimal ===
#         # -?: opcional negativo
#         # (?:\d{1,3}(?:\.\d{3})+) : captura grupos de milhar (ex: 1.000, 5.500.000)
#         # |\d+ : ou inteiro simples sem milhar
#         # (?:[.,]\d+)? : decimal opcional (vírgula ou ponto)
#     r'-?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:[.,]\d+)?'
#     r'|'
#         # === 3. Notação científica ===
#         # -?\d+ : parte inteira
#         # (?:\.\d+)? : decimal opcional
#         # [eE][+-]?\d+ : expoente obrigatório com E ou e, opcional + ou -
#     r'-?\d+(?:\.\d+)?[eE][+-]?\d+'
#     r')$'
# )

def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


def print_job_normalize_numeric_done(
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


class GPUNormalizeNumericUtils:
    @staticmethod
    def normalize_number(
        current_df: cudf.DataFrame,
    ) -> bool:
        for column_name in current_df.columns:
            # Verifica se é um número por amostragem
            if GPUAnalysisUtils.infer_by_sample(
                series=current_df[column_name],
                regex_patern=NUMERIC_PATTERN,
                n_parts=100
            ):
                GPUNormalizeNumericUtils.normalize_number_format(
                    current_df=current_df,
                    column_name=column_name,
                )
                GPUNormalizeNumericUtils.convert_column_to_mumber(
                    current_df=current_df,
                    column_name=column_name,
                )

    @staticmethod
    def convert_column_to_mumber(
        current_df: cudf.DataFrame,
        column_name: str
    ) -> None:
        size: int = current_df[column_name].size

        # Coluna vazia
        if size == 0:
            return
        
        # Coluna sem dados
        if current_df[column_name].notna().sum() == 0:
            return
        
        # A coluna deve ser do tipo str
        if current_df[column_name].dtype not in ["object", "string"]:
            return

        is_number = current_df[column_name].str.isfloat()
        not_null = current_df[column_name].notna().sum()

        if (round((is_number.sum() / not_null) * 100) < 90):
            return

        if not is_number.any():
            return

        current_df.loc[~is_number, column_name] = cudf.NA
        
        del is_number

        current_df[column_name] = current_df[column_name].astype("float64")
        
        if ((current_df[column_name] % 1 == 0).all()):

            max_value = current_df[column_name].max()
            min_value = current_df[column_name].min()

            if max_value < 127 and min_value > -127:
                current_df[column_name] = current_df[column_name].astype('int8')
                print_job_normalize_numeric_done(column_name=column_name, column_type="int8")
                return

            if max_value < 32767 and min_value > -32768:
                current_df[column_name] = current_df[column_name].astype('int16')
                print_job_normalize_numeric_done(column_name=column_name, column_type="int16")
                return

            if max_value <= 2147483647 and min_value >= -2147483648:
                current_df[column_name] = current_df[column_name].astype('int32')
                print_job_normalize_numeric_done(column_name=column_name, column_type="int32")
                return

            current_df[column_name] = current_df[column_name].astype('int64')
            print_job_normalize_numeric_done(column_name=column_name, column_type="int64")
            return

        print_job_normalize_numeric_done(column_name=column_name, column_type="float64")
        return



# novos:
    @staticmethod
    def is_integer(
        current_series: cudf.Series,
        n_parts: int = 10,
        n_samples: int = 10
    ) -> bool:
        """
        Verifica de forma performática na GPU se algum valor nas amostras de uma Series
        corresponde a um padrão de data.

        Args:
            s (cudf.Series): A Series de strings que será analisada.
            n_parts (int): O número de partes em que a Series será dividida para amostragem.
            n_samples (int): O número de amostras a serem coletadas de cada parte.

        Returns:
            bool: True se um padrão de data for encontrado nas amostras, False caso contrário.
        """
        series_size = len(current_series)

        
        if (series_size <= 1000):
            int_pattern = (r'^-?\d+$')
            
            return bool(
                current_series
                .str.contains(int_pattern)
                .any()
            )

        if (n_parts * n_samples >= series_size):
            raise ValueError(
                "The total number of samples requested exceeds or equals the series size. Please provide a smaller value for n_parts or n_samples."
            )
        
        if (series_size // n_parts == 0):
            raise ValueError(
                "The number of parts is greater than the series size. Please provide a smaller value for n_parts."
            )
        
        # Índices iniciais de cada bloco (ex: 0, 1000, 2000, ...)
        start_indices = cp.arange(n_parts) * (cp.floor_divide(series_size, n_parts))
        
        # Offsets dentro de cada bloco (ex: 0, 1, 2, ... n_samples-1)
        sample_offsets = cp.arange(n_samples)

        all_indices = (start_indices[:, None] + sample_offsets).flatten()
        
        # Garante que os índices não ultrapassem o tamanho da Series
        all_indices = all_indices[all_indices < series_size]

        # 3. Seleção de todas as amostras em uma única operação
        current_series = current_series.iloc[all_indices]

        int_pattern = (r'^-?\d+$')

        # found_date_pattern = current_series.str.isinteger()
        found_date_pattern = current_series.str.contains(int_pattern)

        # O resultado de .any() é um único valor booleano que é transferido para a CPU
        return bool(found_date_pattern.notna().sum() == found_date_pattern.sum())


    @staticmethod
    def is_float(
        current_series: cudf.Series,
        n_parts: int = 10,
        n_samples: int = 10
    ) -> bool:
        series_size = len(current_series)

        if (series_size <= 1000):
            numeric_pattern = (r'^-?(\d+|\d+\.\d+)$')
            
            return bool(
                current_series
                .str.contains(numeric_pattern)
                .any()
            )

        if (n_parts * n_samples >= series_size):
            raise ValueError(
                "The total number of samples requested exceeds or equals the series size. Please provide a smaller value for n_parts or n_samples."
            )
        
        if (series_size // n_parts == 0):
            raise ValueError(
                "The number of parts is greater than the series size. Please provide a smaller value for n_parts."
            )
        
        # Índices iniciais de cada bloco (ex: 0, 1000, 2000, ...)
        start_indices = cp.arange(n_parts) * (cp.floor_divide(series_size, n_parts))
        
        # Offsets dentro de cada bloco (ex: 0, 1, 2, ... n_samples-1)
        sample_offsets = cp.arange(n_samples)

        all_indices = (start_indices[:, None] + sample_offsets).flatten()
        
        # Garante que os índices não ultrapassem o tamanho da Series
        all_indices = all_indices[all_indices < series_size]

        # 3. Seleção de todas as amostras em uma única operação
        current_series = current_series.iloc[all_indices]

        numeric_pattern = (r'^-?(\d+|\d+\.\d+)$')

        found_date_pattern = current_series.str.contains(numeric_pattern)

        return bool(
            found_date_pattern.notna().sum() == found_date_pattern.sum()
        )


    # def normalize_number(
    #         current_df: cudf.DataFrame,
    #         column_name:str
    # ) -> None:

    #     # ddd.ddd,dd
    #     mask = (
    #         (current_df[column_name].notna()) &
    #         (current_df[column_name].str.contains(".", regex=False)) &
    #         (current_df[column_name].str.contains(",", regex=False))
    #     )

    #     # print(current_df.loc[mask, column_name])

    #     current_df.loc[mask, column_name] = (
    #         current_df.loc[mask, column_name]
    #         .str.replace(".", "", regex=False)
    #         .str.replace(",", ".", regex=False)
    #     )
        
    #     # dd,dddd
    #     mask = (
    #         (current_df[column_name].notna()) &
    #         (current_df[column_name].str.contains(",")) &
    #         (~current_df[column_name].str.contains("."))
    #     )
        
    #     current_df.loc[mask, column_name] = (
    #         current_df.loc[mask, column_name].str.replace(",", ".", regex=False)
    #     )
    #     del mask


    @staticmethod
    def is_probably_number(
        current_df: cudf.DataFrame,
        column_name: str
    ) -> None:
        size: int = current_df[column_name].size

        # Coluna vazia
        if size == 0:
            return
        
        # Coluna sem dados
        if current_df[column_name].notna().sum() == 0:
            return
        
        # A coluna deve ser do tipo str
        if current_df[column_name].dtype not in ["object", "string"]:
            return

        # is_number = current_df[column_name].str.isfloat()
        # not_null = current_df[column_name].notna().sum()

        # print(column_name, "not_null :", not_null, "is_number", is_number.sum())

        # # Se os valores únicos são mais de 60% das linhas, não vale a pena
        # if (round((is_number.sum() / not_null) * 100) < 90):
            # print("não é numero: ", column_name)
            # return False
        # print("é numero: ", column_name)
        # print(column_name, is_number.sum())
        # print(is_number.sum())
        # return
        
        # if not is_number.any():
        #     return

        # current_df.loc[~is_number, column_name] = cudf.NA
        
        # del is_number

        # current_df[column_name] = current_df[column_name].astype("float64")
        
        if ((current_df[column_name] % 1 == 0).all()):

            max_value = current_df[column_name].max()
            min_value = current_df[column_name].min()

            if max_value < 127 and min_value > -127:
                current_df[column_name] = current_df[column_name].astype('int8')
                print_job_normalize_numeric_done(column_name=column_name, column_type="int8")
                return

            if max_value < 32767 and min_value > -32768:
                current_df[column_name] = current_df[column_name].astype('int16')
                print_job_normalize_numeric_done(column_name=column_name, column_type="int16")
                return

            if max_value <= 2147483647 and min_value >= -2147483648:
                current_df[column_name] = current_df[column_name].astype('int32')
                print_job_normalize_numeric_done(column_name=column_name, column_type="int32")
                return

            current_df[column_name] = current_df[column_name].astype('int64')
            print_job_normalize_numeric_done(column_name=column_name, column_type="int64")
            return

        print_job_normalize_numeric_done(column_name=column_name, column_type="float64")
        return


    @staticmethod
    def normalize_number_format(
        current_df: cudf.DataFrame,
        column_name:str
    ) -> None:
        
        # Coluna vazia
        if current_df[column_name].size == 0:
            return
        
        # Coluna sem dados
        if current_df[column_name].notna().sum() == 0:
            return
        
        # A coluna deve ser do tipo str
        if current_df[column_name].dtype not in ["object", "string"]:
            return

        # ddd.ddd,dd
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.contains(".", regex=False)) &
            (current_df[column_name].str.contains(",", regex=False))
        )

        current_df.loc[mask, column_name] = (
            current_df.loc[mask, column_name]
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        
        # dd,dddd
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.contains(","))
        )
        
        current_df.loc[mask, column_name] = (
            current_df.loc[mask, column_name].str.replace(",", ".", regex=False)
        )
        del mask