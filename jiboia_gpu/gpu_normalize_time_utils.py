import cudf
from .gpu_analysis_utils import GPUAnalysisUtils

def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


CUDF_TIME_REGEX_PATTERN: list[str] = [
    # 24h: HH:MM, HH:MM:SS, HH:MM:SS.sss... (fração opcional, 1–9 dígitos)
    r'^(?:[01]\d|2[0-3]):[0-5]\d(?:[:][0-5]\d(?:\.\d{1,9})?)?$',

    # 12h com AM/PM: H:MM, HH:MM, opcional :SS e .sss, AM/PM com ou sem pontos, qualquer caixa
    r'^(?:0?[1-9]|1[0-2]):[0-5]\d(?:[:][0-5]\d(?:\.\d{1,9})?)?\s*(?:[Aa]\.?[Mm]\.?|[Pp]\.?[Mm]\.?)$',

    # 12h somente hora inteira + AM/PM (ex.: "5 PM", "12am")
    r'^(?:0?[1-9]|1[0-2])\s*(?:[Aa]\.?[Mm]\.?|[Pp]\.?[Mm]\.?)$',

    # UTC: HHMM UTC
    r'^\d{4}\s*UTC$',
]


TIME_PATTERN = (
    r'^(?:'
    # hh:mm
    r'([01]\d|2[0-3]):[0-5]\d|'
    # hh:mm:ss
    r'([01]\d|2[0-3]):[0-5]\d:[0-5]\d|'
    # hhmm UTC
    r'([01]\d|2[0-3])[0-5]\d ?UTC|'
    # hhmmUTC
    r'([01]\d|2[0-3])[0-5]\dUTC'
    r')$'
)


def print_job_normalize_time_done() -> None:
    print(
        print_text_green("Done!"),
        "all columns containing",
        print_text_yellow("times"),
        "have been normalized to ",
        print_text_yellow("HH:MM:SS")
    )


def print_job_normalize_time_column_done(
        column_name: str
    ) -> None:
        print(
            print_text_green("Done!"),
            "column",
            print_text_yellow(column_name),
            "with",
            print_text_yellow("time"),
            "value",
            "have been normalized to",
            print_text_yellow("hh:mm:ss")
        )


def print_job_normalize_timedelta_column_done(
        column_name: str
    ) -> None:
        print(
            print_text_green("Done!"),
            "column",
            print_text_yellow(column_name),
            "with",
            print_text_yellow("time"),
            "value",
            "have been normalized to",
            print_text_yellow("timedelta64[s]")
        )


def print_job_create_category_column_done(
    column_name: str,
    column_from: str,
) -> None:
    print(
        print_text_green("Done!"),
        print_text_yellow("category"),
        "column",
        print_text_yellow(column_name),
        "created from",
        print_text_yellow(column_from),
        "type",
        print_text_yellow("category")
    )


class GPUNormalizeTimeUtils:
    @staticmethod
    def combine_regex_patterns(
        pattern_list: list[tuple[str, str, str]]
    ) -> str:
        """
        Combina múltiplos regex em um único padrão com OR (|),
        garantindo match da string inteira.
        """
        # extrai só o padrão da tupla
        patterns_only = [p[0] for p in pattern_list]
        # remove grupos de captura, senão vira confusão
        # transforma `(\d{4})-(\d{2})-(\d{2})` em `\d{4}-\d{2}-\d{2}`
        patterns_cleaned = [p.replace("(", "").replace(")", "") for p in patterns_only]

        # junta todos com OR
        combined_regex = r"^(?:{}$)".format("|".join(patterns_cleaned))

        return combined_regex


    @staticmethod
    def normalize_time(current_df: cudf.DataFrame) -> None:
        for column_name in current_df.columns:
            if GPUNormalizeTimeUtils.is_time(current_df, column_name):
                GPUNormalizeTimeUtils.convert_hhmm_utc_to_hh_mm_ss(
                    current_df=current_df,
                    column_name=column_name
                )
                GPUNormalizeTimeUtils.convert_hh_mm_to_hh_mm_ss(
                    current_df=current_df,
                    column_name=column_name
                )
                print_job_normalize_time_column_done(column_name)


    @staticmethod
    def normalize_timedelta(current_df: cudf.DataFrame) -> None:
        for column_name in current_df.columns:
            # if GPUNormalizeTimeUtils.is_time(current_df, column_name):
            if GPUAnalysisUtils.infer_by_sample(current_df[column_name], TIME_PATTERN):
                GPUNormalizeTimeUtils.convert_hhmm_utc_to_hh_mm_ss(
                    current_df=current_df,
                    column_name=column_name
                )
                GPUNormalizeTimeUtils.convert_hh_mm_to_hh_mm_ss(
                    current_df=current_df,
                    column_name=column_name
                )
                GPUNormalizeTimeUtils.to_timedelta_s(
                    current_df=current_df,
                    column_name=column_name
                )
                print_job_normalize_timedelta_column_done(column_name)


    @staticmethod
    def to_timedelta_s(
        current_df: cudf.DataFrame,
        column_name: str,
    ) -> None:

        current_df[column_name] = cudf.to_datetime(
            current_df[column_name], format="%H:%M:%S"
        )

        current_df[column_name] = current_df[column_name] - cudf.to_datetime(
            "00:00:00", format="%H:%M:%S"
        )

        current_df[column_name] = current_df[column_name].astype("timedelta64[s]")


    @staticmethod
    def normalize_time_to_timedelta_ns(
        current_df: cudf.DataFrame,
        column_name: str,
    ) -> None:
        
        size: int = current_df[column_name].size
        
        # A coluna deve ter pelo menos 1 linha
        if size == 0:
            return
        
        # A coluna não deve ser vazia
        if current_df[column_name].notna().sum() == 0:
            return

        # A coluna deve ser string
        if current_df[column_name].dtype not in ["object", "string"]:
            return

        # Quebra em horas, minutos e segundos
        split_values: cudf.DataFrame = (
            current_df[column_name]
            .str.split(":", expand=True)
        )
        split_values.columns = ["hours", "minutes", "seconds"]

        hours: cudf.Series = split_values["hours"].astype("int32")
        minutes: cudf.Series = split_values["minutes"].astype("int32")

        # Divide segundos em parte inteira e parte fracionária (se existir)
        sec_str: cudf.Series = split_values["seconds"]

        sec_int: cudf.Series = sec_str.str.extract(r"^(\d+)", expand=False).astype("int64")
        sec_frac: cudf.Series = (
            sec_str.str.extract(r"\.(\d+)", expand=False)
            .fillna("0")
        )

        # Normaliza fração para nanossegundos (preenche à direita até 9 dígitos)
        sec_frac_ns: cudf.Series = (
            sec_frac.str.pad(width=9, side="right", fillchar="0").astype("int64")
        )

        # Total em nanossegundos
        total_ns: cudf.Series = (
            (hours * 3600 + minutes * 60 + sec_int) * 1_000_000_000
            + sec_frac_ns
        )

        current_df[column_name] = total_ns.astype("timedelta64[ns]")


    # Normaliza UTC para HH:MM:SS
    @staticmethod
    def convert_hhmm_utc_to_hh_mm_ss(
            current_df: cudf.DataFrame,
            column_name: str
    ) -> None:
        mask_hh_mm_utc = (
            (current_df[column_name].notna())
            &
            (
                (current_df[column_name].str.len() == 8)
                |
                (current_df[column_name].str.len() == 7)
            )
            
            &
            (current_df[column_name].str.slice(0,4).str.isdigit())    # quatros digitos da hora
            &
            (current_df[column_name].str.contains("UTC"))   # separador: há algo que não é numero
        )

        current_df.loc[mask_hh_mm_utc, column_name] = (
            current_df.loc[mask_hh_mm_utc, column_name]
            .str.replace(" ", "")
            .str.replace("UTC", "")
        )
        
        current_df.loc[mask_hh_mm_utc, column_name] = (
            cudf.to_datetime(current_df.loc[mask_hh_mm_utc, column_name] + ":00", format="%H%M:%S")
            .dt.strftime("%H:%M:%S")
        )


    # Normaliza HH:MM:SS AM/PM para HH:MM:SS
    @staticmethod
    def normalize_column_time_am_pm(
        current_df: cudf.DataFrame,
        column_name: str,
    ) -> None:
        # Máscara para as linhas que contêm AM/PM
        mask = current_df[column_name].str.contains(r'(?:[Aa]\.?[Mm]\.?|[Pp]\.?[Mm]\.?)', regex=True)

        # Extrai grupos
        extracted = current_df.loc[mask, column_name].str.extract(
            r'^(\d{1,2}):(\d{2}):?(\d{2})?(?:\.\d+)?\s*([AaPp])\.?[Mm]\.?$',
            expand=True
        )
        
        extracted[2].replace('', '00', inplace=True)

        # Separa as colunas extraídas
        hours_am_pm = extracted[0].astype("int32")
        minutes = extracted[1]
        seconds = extracted[2]
        am_pm = extracted[3]

        
        # Lógica de conversão de AM/PM para 24h
        is_pm_mask = (am_pm.str.upper() == 'P') & (hours_am_pm != 12)
        is_12am_mask = (am_pm.str.upper() == 'A') & (hours_am_pm == 12)

        # Cria uma cópia das horas para evitar alterar a série original
        hours_24h = hours_am_pm.copy()
        hours_24h.loc[is_pm_mask] = hours_24h + 12
        hours_24h.loc[is_12am_mask] = 0

        # Concatena as partes para o formato 'HH:MM:SS'
        normalized = (
            hours_24h.astype("str").str.zfill(2)
            + ":" + minutes + ":" + seconds
        )

        # Altera a coluna original apenas onde a máscara é True
        current_df.loc[mask, column_name] = normalized


    # Normaliza HH:MM para HH:MM:SS
    @staticmethod
    def convert_hh_mm_to_hh_mm_ss(
            current_df: cudf.DataFrame,
            column_name: str
    ) -> None:

        mask_hh_mm = (
            (current_df[column_name].notna())
            &
            (current_df[column_name].str.len() == 5)
            &
            (current_df[column_name].str.slice(0,2).str.isdigit())    # quatros digitos da hora
            &
            (current_df[column_name].str.slice(2,3) == ":")   # separador: há algo que não é numero
            &
            (current_df[column_name].str.slice(3,5).str.isdigit())    # dois digitos dos minutos
        )
        
        current_df.loc[mask_hh_mm, column_name] = current_df.loc[mask_hh_mm, column_name] + ":00"


    # Novo:
    @staticmethod
    def is_time(current_df: cudf.DataFrame, column_name:str) -> bool:
        if current_df.size == 0:
            return False

        # Coluna sem dados
        if current_df[column_name].notna().sum() == 0:
            return False

        if current_df[column_name].dtype not in ["object", "string"]:
            return False
        
        not_na_rows = current_df[column_name].notna().sum()

        mask_not_time = (
            (current_df[column_name].notna())
            &
            (~current_df[column_name].str.slice(0, 1).str.isdigit())
            &
            (
                (~current_df[column_name].str.contains("UTC"))
                |
                (~current_df[column_name].str.contains(":"))
            )
        )

        if mask_not_time.sum() == not_na_rows:
            return False
        del mask_not_time

        mask_hh_mm_ss_decimal = (
            (current_df[column_name].notna())
            &
            (current_df[column_name].str.len() > 8)
            &
            (current_df[column_name].str.slice(0, 2).str.isdigit())  # HH
            &
            (current_df[column_name].str.slice(2, 3) == ":")   # :
            &
            (current_df[column_name].str.slice(3, 5).str.isdigit())  # MM
            &
            (current_df[column_name].str.slice(5, 6) == ":")   # :
            &
            (current_df[column_name].str.slice(6, 8).str.isdigit())  # SS
            &
            (current_df[column_name].str.slice(8, 9) == ".")   # .
        )

        mask_hh_mm_ss = (
            (current_df[column_name].notna())
            &
            (current_df[column_name].str.len() == 8)
            &
            (current_df[column_name].str.slice(0,2).str.isdigit())    # quatros digitos da hora
            &
            (current_df[column_name].str.slice(2,3) == ":")   # separador: há algo que não é numero
            &
            (current_df[column_name].str.slice(3,5).str.isdigit())    # dois digitos dos minutos
            &
            (current_df[column_name].str.slice(5,6) == ":")   # separador: há algo que não é numero
            &
            (current_df[column_name].str.slice(6,8).str.isdigit())   # dois digitos dos segundos
        )
        
        mask_hh_mm = (
            (current_df[column_name].notna())
            &
            (current_df[column_name].str.len() == 5)
            &
            (current_df[column_name].str.slice(0,2).str.isdigit())    # quatros digitos da hora
            &
            (current_df[column_name].str.slice(2,3) == ":")   # separador: há algo que não é numero
            &
            (current_df[column_name].str.slice(3,5).str.isdigit())    # dois digitos dos minutos
        )
        
        mask_hh_mm_utc = (
            (current_df[column_name].notna())
            &
            (
                (current_df[column_name].str.len() == 8)
                |
                (current_df[column_name].str.len() == 7)
            )
            
            &
            (current_df[column_name].str.slice(0,4).str.isdigit())    # quatros digitos da hora
            &
            (current_df[column_name].str.contains("UTC"))   # separador: há algo que não é numero
        )
        
        is_time = mask_hh_mm_ss_decimal | mask_hh_mm_ss | mask_hh_mm | mask_hh_mm_utc

        if round((is_time.sum() / not_na_rows) * 100) >= 50:
            return True
        return False