import cudf
import cupy as cp
from .gpu_analysis_utils import GPUAnalysisUtils

def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


def print_job_normalize_date_done() -> None:
    print(
        print_text_green("Done!"),
        "all columns containing",
        print_text_yellow("dates"),
        "values converted to",
        "have been normalized to ",
        print_text_yellow("YYYY-MM-DD")
    )


def print_job_normalize_datetime_column_done(
        column_name: str
    ) -> None:
        print(
            print_text_green("Done!"),
            "column",
            print_text_yellow(column_name),
            "containing",
            print_text_yellow("date"),
            "value",
            "have been normalized to",
            print_text_yellow("datedatetime64[ns]")
        )


def print_job_normalize_date_column_done(
        column_name: str
    ) -> None:
        print(
            print_text_green("Done!"),
            "column",
            print_text_yellow(column_name),
            "containing",
            print_text_yellow("date"),
            "value",
            "have been normalized to",
            print_text_yellow("yyyy-mm-dd")
        )


DATE_PATTERN = (
    r'^(?:'
    # Padrões com separadores: YYYY/MM/DD, DD/MM/YYYY, MM/DD/YYYY, etc.
    r'\d{4}[/\-._ ](0[1-9]|1[0-2])[/\-._ ](0[1-9]|[12]\d|3[01])|'
    r'(0[1-9]|[12]\d|3[01])[/\-._ ](0[1-9]|1[0-2])[/\-._ ]\d{4}|'
    r'(0[1-9]|1[0-2])[/\-._ ](0[1-9]|[12]\d|3[01])[/\-._ ]\d{4}|'
    r'(0[1-9]|[12]\d|3[01])[/\-._ ]\d{2}[/\-._ ]\d{2}|' # Formatos com ano de 2 dígitos
    r'\d{2}[/\-._ ](0[1-9]|[12]\d|3[01])[/\-._ ]\d{2}|'
    # Padrões sem separadores (8 dígitos)
    r'\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])|' # YYYYMMDD
    # r'(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{4}'   # MMDDYYYY
    r')$'
)


class GPUNormalizeDateUtils:
    @staticmethod
    def normalize_date(current_df: cudf.DataFrame) -> None:
        for column_name in current_df.columns:
            # if GPUNormalizeDateUtils.is_date(
            #     current_df,
            #     column_name
            # ):
            if GPUAnalysisUtils.infer_by_sample(
                series=current_df[column_name],
                regex_patern=DATE_PATTERN
            ):
                GPUNormalizeDateUtils.to_date_iso(
                    current_df=current_df,
                    column_name=column_name
                )
                print_job_normalize_date_column_done(column_name)

    @staticmethod
    def normalize_datetime(current_df: cudf.DataFrame) -> None:
        for column_name in current_df.columns:
            # if GPUNormalizeDateUtils.is_date(
            #     current_df,
            #     column_name
            # ):
            if GPUAnalysisUtils.infer_by_sample(
                series=current_df[column_name],
                regex_patern=DATE_PATTERN
            ):
                GPUNormalizeDateUtils.to_datetime(
                    current_df=current_df,
                    column_name=column_name
                )
                print_job_normalize_datetime_column_done(column_name)


    @staticmethod
    def is_date_column_by_samples(
        series: cudf.Series,
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

        series_size = len(series)

        # print(series.name)
        # return

        if ((n_parts * n_samples) >= series_size):
            raise ValueError("The total number of samples requested exceeds or equals the series size. Please provide a smaller value for n_parts or n_samples.")
        
        if (series_size // n_parts == 0):
            raise ValueError("The number of parts is greater than the series size. Please provide a smaller value for n_parts.")

        # Gera todos os índices de amostragem DE UMA VEZ na GPU
        step_pass = series_size // n_parts
        
        # Índices iniciais de cada bloco (ex: 0, 1000, 2000, ...)
        start_indices = cp.arange(n_parts) * step_pass
        
        # Offsets dentro de cada bloco (ex: 0, 1, 2, ... n_samples-1)
        sample_offsets = cp.arange(n_samples)

        all_indices = (start_indices[:, None] + sample_offsets).flatten()
        
        # Garante que os índices não ultrapassem o tamanho da Series
        all_indices = all_indices[all_indices < series_size]

        # 3. Seleção de todas as amostras em uma única operação
        samples = series.iloc[all_indices]

        date_pattern = (
            r'^(?:'
            # Padrões com separadores: YYYY/MM/DD, DD/MM/YYYY, MM/DD/YYYY, etc.
            r'\d{4}[/\-._ ](0[1-9]|1[0-2])[/\-._ ](0[1-9]|[12]\d|3[01])|'
            r'(0[1-9]|[12]\d|3[01])[/\-._ ](0[1-9]|1[0-2])[/\-._ ]\d{4}|'
            r'(0[1-9]|1[0-2])[/\-._ ](0[1-9]|[12]\d|3[01])[/\-._ ]\d{4}|'
            r'(0[1-9]|[12]\d|3[01])[/\-._ ]\d{2}[/\-._ ]\d{2}|' # Formatos com ano de 2 dígitos
            r'\d{2}[/\-._ ](0[1-9]|[12]\d|3[01])[/\-._ ]\d{2}|'
            # Padrões sem separadores (8 dígitos)
            r'\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])|' # YYYYMMDD
            # r'(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{4}'   # MMDDYYYY
            r')$'
        )

        if (samples.str.contains(date_pattern).sum()) == samples.size:
            return True
        return False


    @staticmethod
    def is_date(current_df: cudf.DataFrame, column_name:str) -> bool:
        if current_df.size == 0:
            return False

        # Coluna sem dados
        if current_df[column_name].notna().sum() == 0:
            return False

        if current_df[column_name].dtype not in ["object", "string"]:
            return False
        
        is_time = current_df[column_name].str.contains(":").any()

        if is_time:
            return False
    
        # yyyy?mm?dd
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 10) &
            (current_df[column_name].str.slice(0,4).str.isdigit()) &   # quatros digitos do ano
            (~current_df[column_name].str.slice(4,5).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(4,5).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(5,7).str.isdigit()) &   # dois digitos dos meses
            (~current_df[column_name].str.slice(7,8).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(7,8).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(8,10).str.isdigit())    # dois digitos dos dias
        )
        if mask.any():
            return True

        # dd?mm?yyyy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 10) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &   # dois digitos dos dias
            (~current_df[column_name].str.slice(2,3).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(2,3).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(3,5).str.isdigit()) &   # dois digitos dos meses
            (~current_df[column_name].str.slice(5,6).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(5,6).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,10).str.isdigit())    # quatros digitos do ano
        )

        if mask.any():
            return True

        # dd?mm?yy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 8) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &   # dois digitos dos dias
            (~current_df[column_name].str.slice(2,3).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(2,3).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(3,5).str.isdigit()) &   # dois digitos dos meses    
            (~current_df[column_name].str.slice(5,6).str.isdigit()) &  # separador: há algo que não é letra
            (~current_df[column_name].str.slice(5,6).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,8).str.isdigit())     # dois digitos dos anos
        )
        if mask.any():
            return True
        
        days: list[str] = [str(number).zfill(2) for number in range(1, 32)]
        months: list[str] = [str(number).zfill(2) for number in range(1, 13)]

        mask = (
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 8) &
                (current_df[column_name].str.isdigit()) &
                (current_df[column_name].str.slice(4, 6).isin(months)) &
                (current_df[column_name].str.slice(6, 8).isin(days))  
            )
            &
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.isdigit())
            )
        )

        if mask.any():
            return True

        return False


    @staticmethod
    def infer_date_formats(current_df: cudf.DataFrame, column_name:str) -> list[tuple[str, int]]:

        if current_df.size == 0:
            return []

        # Coluna sem dados
        if current_df[column_name].notna().sum() == 0:
            return []

        if current_df[column_name].dtype not in ["object", "string"]:
            return []
        
        is_time = current_df[column_name].str.contains(":").any()

        if is_time:
            return []

        date_paterns: list[tuple[str, int]] = []
        
        valid_date_rows: int = 0

        # yyyy?mm?dd
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 10) &
            (current_df[column_name].str.slice(0,4).str.isdigit()) &   # quatros digitos do ano
            (~current_df[column_name].str.slice(4,5).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(4,5).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(5,7).str.isdigit()) &   # dois digitos dos meses
            (~current_df[column_name].str.slice(7,8).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(7,8).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(8,10).str.isdigit())    # dois digitos dos dias
        )
        
        if mask.any():
            valid_date_rows = valid_date_rows + mask.sum()
            date_paterns.append(
                ("%Y?%m?%d", mask.sum())
            )

        # dd?mm?yyyy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 10) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &   # dois digitos dos dias
            (~current_df[column_name].str.slice(2,3).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(2,3).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(3,5).str.isdigit()) &   # dois digitos dos meses
            (~current_df[column_name].str.slice(5,6).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(5,6).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,10).str.isdigit())    # quatros digitos do ano
        )

        if mask.any():
            valid_date_rows = valid_date_rows + mask.sum()
            date_paterns.append(
                ("%m?%d?%Y", mask.sum())
            )       

        # dd?mm?yy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 8) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &   # dois digitos dos dias
            (~current_df[column_name].str.slice(2,3).str.isdigit()) &  # separador: há algo que não é numero
            (~current_df[column_name].str.slice(2,3).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(3,5).str.isdigit()) &   # dois digitos dos meses    
            (~current_df[column_name].str.slice(5,6).str.isdigit()) &  # separador: há algo que não é letra
            (~current_df[column_name].str.slice(5,6).str.isalpha()) &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,8).str.isdigit())     # dois digitos dos anos
        )
        if mask.any():
            valid_date_rows = valid_date_rows + mask.sum()
            date_paterns.append(
                ("%m?%d?%y", mask.sum())
            )

        days: list[str] = [str(number).zfill(2) for number in range(1, 32)]
        months: list[str] = [str(number).zfill(2) for number in range(1, 13)]

        mask = (
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 8) &
                (current_df[column_name].str.isdigit()) &
                (current_df[column_name].str.slice(4, 6).isin(months)) &
                (current_df[column_name].str.slice(6, 8).isin(days))  
            )
            &
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.isdigit())
            )
        )

        if mask.any():
            valid_date_rows = valid_date_rows + mask.sum()
            date_paterns.append(
                ("Y%m%d", mask.sum())
            )
            
        na_rows = (current_df[column_name].isna().sum())
        total_rows = current_df[column_name].size
        invalid_rows = total_rows - na_rows - valid_date_rows
        
        date_paterns.append(
            [
                ("<NA>", na_rows),
                ("invalid", invalid_rows)
            ]
        )

        return date_paterns
    

    @staticmethod
    def to_date_iso(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        is_time = current_df[column_name].str.contains(":").any()
        
        if is_time:
            return

        current_df[column_name] = (
            current_df[column_name]
            .str.replace("/", "-", regex=False)
            .str.replace(" ", "-", regex=False)
            .str.replace("_", "-", regex=False)
            .str.replace(".", "-", regex=False)
        )

        # dd?mm?yyyy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 10) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &        # dois digitos dos dias
            (current_df[column_name].str.slice(2,3) == "-") &               # separador: há algo que não é numero
            (current_df[column_name].str.slice(3,5).str.isdigit()) &        # dois digitos dos meses
            (current_df[column_name].str.slice(5,6) == "-") &               # separador: há algo que não é numero
            (current_df[column_name].str.slice(6,10).str.isdigit())         # quatros digitos do ano
        )

        if mask.any():
            current_df.loc[mask, column_name] = cudf.to_datetime(
                current_df.loc[mask, column_name], format="%d-%m-%Y"
            ).dt.strftime("%Y-%m-%d")
        del mask

        # dd?mm?yy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 8) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &        # dois digitos dos dias
            (current_df[column_name].str.slice(2,3) == "-") &               # separador: há algo que não é numero
            (current_df[column_name].str.slice(3,5).str.isdigit()) &        # dois digitos dos meses    
            (current_df[column_name].str.slice(5,6) == "-") &               # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,8).str.isdigit())          # dois digitos dos anos
        )

        if mask.any():
            current_df.loc[mask, column_name] = cudf.to_datetime(
                current_df.loc[mask, column_name], format="%d-%m-%y"
            ).dt.strftime("%Y-%m-%d")
        del mask

        days: list[str] = [str(number).zfill(2) for number in range(1, 32)]
        months: list[str] = [str(number).zfill(2) for number in range(1, 13)]
        
        # yyyymmdd
        mask = (
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 8) &
                (current_df[column_name].str.isdigit()) &
                (current_df[column_name].str.slice(4, 6).isin(months)) &
                (current_df[column_name].str.slice(6, 8).isin(days))  
            )
            &
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.isdigit())
            )
        )

        if mask.any():     
            current_df.loc[mask, column_name] = cudf.to_datetime(
                current_df.loc[mask, column_name], format='%Y%m%d'
            ).dt.strftime('%Y-%m-%d')
        del mask

        mask = (
            (   # yyyy?mm?ddd
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 10) &
                (current_df[column_name].str.slice(0,4).str.isdigit()) &   # quatros digitos do ano
                (~current_df[column_name].str.slice(4,5).str.isdigit()) &  # separador: há algo que não é numero
                (~current_df[column_name].str.slice(4,5).str.isalpha()) &  # separador: há algo que não é letra
                (current_df[column_name].str.slice(5,7).str.isdigit()) &   # dois digitos dos meses
                (~current_df[column_name].str.slice(7,8).str.isdigit()) &  # separador: há algo que não é numero
                (~current_df[column_name].str.slice(7,8).str.isalpha()) &  # separador: há algo que não é letra
                (current_df[column_name].str.slice(8,10).str.isdigit())    # dois digitos dos dias
            )
        )

        current_df.loc[~mask, column_name] = cudf.NA
        del mask

        return


    @staticmethod
    def to_datetime(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        is_time = current_df[column_name].str.contains(":").any()
        
        if is_time:
            return

        current_df[column_name] = (
            current_df[column_name]
            .str.replace("/", "-", regex=False)
            .str.replace(" ", "-", regex=False)
            .str.replace("_", "-", regex=False)
            .str.replace(".", "-", regex=False)
        )

        # yyyy?mm?dd
        mask = (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 10) &
                (current_df[column_name].str.slice(0,4).str.isdigit()) &    # quatros digitos do ano
                (current_df[column_name].str.slice(4,5) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(5,7).str.isdigit()) &    # dois digitos dos meses
                (current_df[column_name].str.slice(7,8) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(8,10).str.isdigit())     # dois digitos dos dias
        )

        if mask.any():
            current_df.loc[mask, column_name] = cudf.to_datetime(
                current_df.loc[mask, column_name], format="%Y-%m-%d"
            ).dt.strftime("%Y%m%d")
        del mask


        # dd?mm?yyyy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 10) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &        # dois digitos dos dias
            (current_df[column_name].str.slice(2,3) == "-") &               # separador: há algo que não é numero
            (current_df[column_name].str.slice(3,5).str.isdigit()) &        # dois digitos dos meses
            (current_df[column_name].str.slice(5,6) == "-") &               # separador: há algo que não é numero
            (current_df[column_name].str.slice(6,10).str.isdigit())         # quatros digitos do ano
        )

        if mask.any():
            current_df.loc[mask, column_name] = cudf.to_datetime(
                current_df.loc[mask, column_name], format="%d-%m-%Y"
            ).dt.strftime("%Y%m%d")
        del mask


        # dd?mm?yy
        mask = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 8) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &        # dois digitos dos dias
            (current_df[column_name].str.slice(2,3) == "-") &               # separador: há algo que não é numero
            (current_df[column_name].str.slice(3,5).str.isdigit()) &        # dois digitos dos meses    
            (current_df[column_name].str.slice(5,6) == "-") &               # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,8).str.isdigit())          # dois digitos dos anos
        )

        if mask.any():
            current_df.loc[mask, column_name] = cudf.to_datetime(
                current_df.loc[mask, column_name], format="%d-%m-%y"
            ).dt.strftime("%Y%m%d")
        del mask

        days: list[str] = [str(number).zfill(2) for number in range(1, 32)]
        months: list[str] = [str(number).zfill(2) for number in range(1, 13)]

        # yyyymmdd
        mask = (
            (
                (   
                    (current_df[column_name].notna()) &
                    (current_df[column_name].str.len() == 8) &
                    (current_df[column_name].str.isdigit()) &
                    (current_df[column_name].str.slice(4, 6).isin(months)) &
                    (current_df[column_name].str.slice(6, 8).isin(days))
                )
                &
                (
                    (current_df[column_name].notna()) &
                    (current_df[column_name].str.isdigit())
                )
                
            )
        )

        current_df.loc[~mask, column_name] = cudf.NA
        del mask

        current_df[column_name] = cudf.to_datetime(current_df[column_name], format='%Y%m%d')

        return


    @staticmethod
    def normalize_delimiter(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return

        current_df[column_name] = (
            current_df[column_name]
            .str.replace("/", "-", regex=False)
            .str.replace(" ", "-", regex=False)
            .str.replace("_", "-", regex=False)
            .str.replace(".", "-", regex=False)
        )


    @staticmethod
    def convert_dd_mm_yyyy_to_yyyy_mm_dd(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        mask_dd_mm_yyyy = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 10) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &   # dois digitos dos dias
            (current_df[column_name].str.slice(2,3) == "-") &  # separador: há algo que não é numero
            (current_df[column_name].str.slice(3,5).str.isdigit()) &    # dois digitos dos meses
            (current_df[column_name].str.slice(5,6) == "-") &  # separador: há algo que não é numero
            (current_df[column_name].str.slice(6,10).str.isdigit())   # quatros digitos do ano
        )

        if mask_dd_mm_yyyy.any():
            current_df.loc[mask_dd_mm_yyyy, column_name] = cudf.to_datetime(
                current_df.loc[mask_dd_mm_yyyy, column_name], format="%d-%m-%Y"
            ).dt.strftime("%Y-%m-%d")
        
        del mask_dd_mm_yyyy


    @staticmethod
    def convert_dd_mm_yy_to_yyyy_mm_dd(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        mask_dd_mm_yy = (
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 8) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &   # dois digitos dos dias
            (current_df[column_name].str.slice(2,3) == "-") &  # separador: há algo que não é numero
            (current_df[column_name].str.slice(3,5).str.isdigit()) &    # dois digitos dos meses    
            (current_df[column_name].str.slice(5,6) == "-") &  # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,8).str.isdigit())    # dois digitos dos anos
        )

        if mask_dd_mm_yy.any():
            current_df.loc[mask_dd_mm_yy, column_name] = cudf.to_datetime(
                current_df.loc[mask_dd_mm_yy, column_name], format="%d-%m-%y"
            ).dt.strftime("%Y-%m-%d")
            
        del mask_dd_mm_yy


    @staticmethod
    def convert_yyyyddmm_to_yyyy_mm_dd(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return

        days: list[str] = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
            "31"
        ]
        
        months: list[str] = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12"
        ]
        
        # yyyymmdd
        mask_yyyymmdd = (
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 8) &
                (current_df[column_name].str.isdigit()) &
                (current_df[column_name].str.slice(4, 6).isin(months)) &
                (current_df[column_name].str.slice(6, 8).isin(days))
            )
            &
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.isdigit())
            )
        )
        
        if mask_yyyymmdd.any():     
            current_df.loc[mask_yyyymmdd, column_name] = cudf.to_datetime(
                current_df.loc[mask_yyyymmdd, column_name], format='%Y%m%d'
            ).dt.strftime('%Y-%m-%d')
            
        del mask_yyyymmdd


    @staticmethod
    def convert_invalid_date(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        days: list[str] = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
            "31"
        ]
        
        months: list[str] = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12"
        ]
        
        all_dates_mask = (
            (   # yyyy?mm?dd
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 10) &
                (current_df[column_name].str.slice(0,4).str.isdigit()) &    # quatros digitos do ano
                (current_df[column_name].str.slice(4,5) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(5,7).str.isdigit()) &    # dois digitos dos meses
                (current_df[column_name].str.slice(7,8) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(8,10).str.isdigit())     # dois digitos dos dias
            )
            |
            (   # dd?mm?yyyy
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 10) &
                (current_df[column_name].str.slice(0,2).str.isdigit()) &    # dois digitos dos dias
                (current_df[column_name].str.slice(2,3) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(3,5).str.isdigit()) &    # dois digitos dos meses
                (current_df[column_name].str.slice(5,6) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(6,10).str.isdigit())     # quatros digitos do ano
            )
            |
            (
            # dd?mm?yy
            (current_df[column_name].notna()) &
            (current_df[column_name].str.len() == 8) &
            (current_df[column_name].str.slice(0,2).str.isdigit()) &        # dois digitos dos dias
            (current_df[column_name].str.slice(2,3) == "-") &               # separador: há algo que não é numero
            (current_df[column_name].str.slice(3,5).str.isdigit()) &        # dois digitos dos meses    
            (current_df[column_name].str.slice(5,6) == "-") &               # separador: há algo que não é letra
            (current_df[column_name].str.slice(6,8).str.isdigit())          # dois digitos dos anos
            )
            |
            (   # yyyymmdd
                (
                    (current_df[column_name].notna()) &
                    (current_df[column_name].str.len() == 8) &
                    (current_df[column_name].str.isdigit()) &
                    (current_df[column_name].str.slice(4, 6).isin(months)) &
                    (current_df[column_name].str.slice(6, 8).isin(days))
                )
                &
                (
                    (current_df[column_name].notna()) &
                    (current_df[column_name].str.isdigit())
                )
                
            )
        )

        current_df.loc[~all_dates_mask, column_name] = cudf.NA
            
        del all_dates_mask


    @staticmethod
    def convert_invalid_date_yyyy_mm_dd_to_na(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        mask_yyyy_mm_dd = (
            (
                (current_df[column_name].notna()) &
                (current_df[column_name].str.len() == 10) &
                (current_df[column_name].str.slice(0,4).str.isdigit()) &    # quatros digitos do ano
                (current_df[column_name].str.slice(4,5) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(5,7).str.isdigit()) &    # dois digitos dos meses
                (current_df[column_name].str.slice(7,8) == "-") &           # separador: há algo que não é numero
                (current_df[column_name].str.slice(8,10).str.isdigit())     # dois digitos dos dias
            )
        )

        current_df.loc[~mask_yyyy_mm_dd, column_name] = cudf.NA
            
        del all_dates_mask

    
    @staticmethod
    def convert_invalid_date_yyyymmdd_to_na(current_df: cudf.DataFrame, column_name:str) -> None:
        if current_df.size == 0:
            return

        if current_df[column_name].notna().sum() == 0:
            return

        if current_df[column_name].dtype not in ["object", "string"]:
            return
        
        days: list[str] = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
            "31"
        ]
        
        months: list[str] = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12"
        ]
        
        mask_yyyymmdd = (
            (
                (
                    (current_df[column_name].notna()) &
                    (current_df[column_name].str.len() == 8) &
                    (current_df[column_name].str.isdigit()) &
                    (current_df[column_name].str.slice(4, 6).isin(months)) &
                    (current_df[column_name].str.slice(6, 8).isin(days))
                )
                &
                (
                    (current_df[column_name].notna()) &
                    (current_df[column_name].str.isdigit())
                )
                
            )
        )

        current_df.loc[~mask_yyyymmdd, column_name] = cudf.NA
