import cudf
import cupy as cp
import pandas as pd


def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


def print_job_drop_column_done(
    columns_to_delete: list[str]
) -> None:
    colored_names: list[str] = [
        print_text_yellow(name) for name in columns_to_delete
    ]
    
    msg: str = ", ".join(colored_names)

    print(
        print_text_green("Done!"),
        "column",
        msg,
        "was",
        print_text_red("dropped")
    )


class GPUAnalysisUtils:
    @staticmethod
    def drop_columns(
        current_df: cudf.DataFrame,
        column_names: list[str],
    ) -> None:

        columns_to_delete: list = []

        for column_name in column_names:

            if column_name in current_df.columns:
                columns_to_delete.append(column_name)


        if len(columns_to_delete) >= 1:

            current_df.drop(
                columns=columns_to_delete,
                inplace=True
            )
            print_job_drop_column_done(columns_to_delete)


    @staticmethod
    def cudf_size_info(current_df: cudf.DataFrame, print_info: bool = False) -> None:

        rows: int = current_df.shape[0]
        columns: int =  current_df.shape[1]
        vram_size_mb: float = round(current_df.memory_usage(index=True, deep=True).sum() / (1024 * 1024), 2)

        cudf_info: dict[str, any] = {
            "rows": rows,
            "columns": columns,
            "VRAM size Mb": vram_size_mb
        }

        if print_info:
            print(
                print_text_green("Done!"),
                "rows:",
                print_text_yellow(rows),
                "columns:",
                print_text_yellow(columns),
                "VRAM size Mb:",
                print_text_yellow(vram_size_mb),
            )
                
        return cudf_info


    @staticmethod
    def get_gpu_memory_info(device_id: int = 0) -> dict[str, int]:
        free_bytes, total_bytes = cp.cuda.runtime.memGetInfo()
        return {
            "free_mb": round(free_bytes / (1024 * 1024), 2),
            "total": round(total_bytes / (1024 * 1024), 2),
            "used_mb":  round((total_bytes - free_bytes) / (1024 * 1024), 2),
        }
    
    @staticmethod
    def is_vram_use_limit(device_id: int = 0) -> dict[str, int]:
        free_bytes, total_bytes = cp.cuda.runtime.memGetInfo()
        vram_percent_in_use: float = round(((total_bytes - free_bytes) / total_bytes) * 100, 1) >= 90
        
        if vram_percent_in_use >= 90:
            return True
        
        return False
    

    @staticmethod
    def pandasdf_size_info(current_df: pd.DataFrame) -> None:
        return {
            "rows": current_df.shape[0],
            "columns": current_df.shape[1],
            "RAM MB": round(current_df.memory_usage(index=True, deep=True).sum() / (1024 * 1024), 2)
        }
    

    def print_converted_column_type(
        column_name: str,
        column_type: str
    ) -> None:
        print(
            "\033[1;32mDone!\033[0m",
            "column",
            # f"{column_name}",
            f"\033[1;33m{column_name}\033[0m",
            "converted to", f"\033[1;33m{column_type}\033[0m"
        )
       
    # @staticmethod
    # def get_frequency_by_column(current_df: pd.DataFrame, column: str) -> pd.DataFrame:
    #     '''
    #     Gera um current_df com a frequencia de valores de uma coluna
    #     '''
    #     current_df_frequency: pd.DataFrame = (
    #         current_df[column]
    #         .value_counts()
    #         .reset_index()
    #     )
    
    #     current_df_frequency.columns = [column, 'frequency']
    
    #     current_df_frequency: pd.DataFrame = (
    #         current_df_frequency
    #         .sort_values(
    #             by='frequency',
    #             ascending=False
    #         )
    #     )
    
    #     return current_df_frequency


    # @staticmethod
    # def get_unique_values(current_df: pd.DataFrame, column: str) -> pd.DataFrame:
    #     '''
    #     Retorna um dataframe com os valores únicos de uma coluna
    #     '''
    #     df_unique = pd.DataFrame({
    #         column: current_df[column].dropna().unique()
    #     })

    #     df_unique = df_unique.sort_values(
    #         by=column,
    #         ascending=True
    #     ).reset_index(drop=True)

    #     return df_unique

    @staticmethod
    def get_index_samples(
        series: cudf.Series,
        n_parts: int = 10,
        n_samples: int = 10
    ) -> list[int]:
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

        return all_indices
    

    @staticmethod
    def infer_by_sample(
        series: cudf.Series,
        regex_patern: str,
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

        if series_size == 0:
            return False

        # Coluna sem dados
        if series.notna().sum() == 0:
            return False

        if series.dtype not in ["object", "string"]:
            return False
        

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

        if (samples.str.contains(regex_patern).sum() == samples.notna().sum()):
            return True

        return False
