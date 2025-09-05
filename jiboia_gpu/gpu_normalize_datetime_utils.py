import cudf

def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"


def print_job_create_datetime_column_done(
    column_name: str,
    date_column_name: str,
    time_column_name: str,
) -> None:
    print(
        print_text_green("Done!"),
        "column",
        print_text_yellow(column_name),
        "created from",
        print_text_yellow(date_column_name),
        "and",
        print_text_yellow(time_column_name),
        "type",
        print_text_yellow("datetime64[ns]")
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


class GPUNormalizeDateTimeUtils:
    @staticmethod
    def create_day_week_column(
        current_df: cudf.DataFrame,
        datetime_column_name: str,
        new_day_week_column_name: str = "day_week_i",
        before_column: None|str = None
    ) -> None:
        """
        Cria uma nova coluna datetime somando a coluna date e a coluna time (timedelta).
        """
        if str(current_df[datetime_column_name].dtype) not in ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]:
            raise TypeError(f"A coluna '{datetime_column_name}' deve estar em datetime.")

        if before_column is not None:
            if before_column not in current_df.columns:
                raise ValueError(f"A coluna '{before_column}' não existe no DataFrame.")
            insert_position: int = current_df.columns.get_loc(before_column)

            current_df.insert(
                loc=insert_position,
                column=new_day_week_column_name,
                value=current_df[datetime_column_name].dt.dayofweek
            )
        else:
            current_df[new_day_week_column_name] = current_df[datetime_column_name].dt.dayofweek


    # @staticmethod
    # def create_day_week_br_category_column(
    #     current_df: cudf.DataFrame,
    #     datetime_column_name: str,
    #     new_day_week_column_name: str = "day_week",
    #     before_column: None | str = None,
    # ) -> None:
    #     """
    #     Cria uma nova coluna com os dias da semana como tipo categórico ordenado.
    #     """
    #     if str(current_df[datetime_column_name].dtype) not in ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]:
    #         raise TypeError(f"A coluna '{datetime_column_name}' deve estar em datetime.")

    #     # Define a ordem correta dos dias da semana
    #     day_week_categories = [
    #         "segunda",
    #         "terca",
    #         "quarta",
    #         "quinta",
    #         "sexta",
    #         "sabado",
    #         "domingo",
    #     ]
        
    #     # Mapeia os números do dia da semana (0=segunda, 6=domingo) para nomes.
    #     day_week_mapping = {
    #         0: "segunda",
    #         1: "terca",
    #         2: "quarta",
    #         3: "quinta",
    #         4: "sexta",
    #         5: "sabado",
    #         6: "domingo",
    #     }
        
    #     # Obtém a Series com os números do dia da semana e mapeia para os nomes
    #     day_of_week_series = current_df[datetime_column_name].dt.dayofweek.astype("int8")
    #     category_series = day_of_week_series.map(day_week_mapping)
        
    #     # Define o tipo categórico ordenado
    #     ordered_dtype = cudf.CategoricalDtype(categories=day_week_categories, ordered=True)
        
    #     # Converte a série para o novo tipo categórico ordenado
    #     category_series = category_series.astype(ordered_dtype)

    #     if before_column is not None:
    #         if before_column not in current_df.columns:
    #             raise ValueError(f"A coluna '{before_column}' não existe no DataFrame.")
    #         insert_position: int = current_df.columns.get_loc(before_column)
            
    #         current_df.insert(
    #             loc=insert_position,
    #             column=new_day_week_column_name,
    #             value=category_series,
    #         )
    #     else:
    #         current_df[new_day_week_column_name] = category_series
    #     print_job_create_category_column_done(
    #         column_name=new_day_week_column_name,
    #         column_from=datetime_column_name
    #     )


    # @staticmethod
    # def create_month_category_column(
    #     current_df: cudf.DataFrame,
    #     datetime_column_name: str,
    #     new_month_category_column_name: str = "month",
    #     before_column: None | str = None,
    #     case_type: str = "lower"
    # ) -> None:
    #     """
    #     Cria uma nova coluna com os meses do ano como tipo categórico ordenado.
    #     """
    #     if str(current_df[datetime_column_name].dtype) not in ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]:
    #         raise TypeError(f"A coluna '{datetime_column_name}' deve estar em datetime.")

    #     # Define a ordem correta dos meses do ano
    #     month_categories = [
    #         "janeiro",
    #         "fevereiro",
    #         "marco",
    #         "abril",
    #         "maio",
    #         "junho",
    #         "julho",
    #         "agosto",
    #         "setembro",
    #         "outubro",
    #         "novembro",
    #         "dezembro",
    #     ]
        
    #     # Mapeia os números do mês (1=janeiro, 12=dezembro) para nomes.
    #     month_mapping = {
    #         1: "janeiro",
    #         2: "fevereiro",
    #         3: "marco",
    #         4: "abril",
    #         5: "maio",
    #         6: "junho",
    #         7: "julho",
    #         8: "agosto",
    #         9: "setembro",
    #         10: "outubro",
    #         11: "novembro",
    #         12: "dezembro",
    #     }
        
    #     # Obtém a Series com os números dos meses e mapeia para os nomes
    #     month_series = current_df[datetime_column_name].dt.month
    #     category_series = month_series.map(month_mapping)
        
    #     # Define o tipo categórico ordenado
    #     ordered_dtype = cudf.CategoricalDtype(categories=month_categories, ordered=True)
        
    #     # Converte a série para o novo tipo categórico ordenado
    #     category_series = category_series.astype(ordered_dtype)

    #     if before_column is not None:
    #         if before_column not in current_df.columns:
    #             raise ValueError(f"A coluna '{before_column}' não existe no DataFrame.")
    #         insert_position: int = current_df.columns.get_loc(before_column)
            
    #         current_df.insert(
    #             loc=insert_position,
    #             column=new_month_category_column_name,
    #             value=category_series,
    #         )
    #     else:
    #         current_df[new_month_category_column_name] = category_series

    #     print_job_create_category_column_done(
    #         column_name=new_month_category_column_name,
    #         column_from=datetime_column_name
    #     )

    @staticmethod
    def create_day_week_br_category_column(
        current_df: cudf.DataFrame,
        datetime_column_name: str,
        new_day_week_column_name: str = "day_week",
        before_column: None | str = None,
        case_type: str = "upper",
    ) -> None:
        """
        Cria uma nova coluna com os dias da semana como tipo categórico ordenado,
        com a opção de formatar o case dos nomes dos dias da semana.
        """
        if str(current_df[datetime_column_name].dtype) not in ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]:
            raise TypeError(f"A coluna '{datetime_column_name}' deve estar em datetime.")

        # Lista de dias da semana em minúsculas como base
        day_week_categories_base = [
            "SEGUNDA", "TERCA", "QUARTA", "QUINTA", "SEXTA", "SABADO", "DOMINGO",
        ]
        
        # Mapeia os números do dia da semana (0=segunda, 6=domingo) para nomes.
        day_week_mapping = {i: day for i, day in enumerate(day_week_categories_base)}
        
        # Obtém a série com os números do dia da semana e mapeia para os nomes
        day_of_week_series = current_df[datetime_column_name].dt.dayofweek
        category_series = day_of_week_series.map(day_week_mapping)
        
        # Aplica a formatação do case
        if case_type == "lower":
            day_week_categories = [d.lower() for d in day_week_categories_base]
            category_series = category_series.str.lower()

        elif case_type == "capitalize":
            day_week_categories = [d.capitalize() for d in day_week_categories_base]
            category_series = category_series.str.capitalize()

        elif case_type == "upper":
            day_week_categories = day_week_categories_base
            # Não precisa de conversão, pois a base já está em minúsculas
        else:
            raise ValueError("O parâmetro 'case_type' deve ser 'upper', 'lower' ou 'capitalize'.")
        
        # Define o tipo categórico ordenado
        ordered_dtype = cudf.CategoricalDtype(categories=day_week_categories, ordered=True)
        
        # Converte a série para o novo tipo categórico ordenado
        category_series = category_series.astype(ordered_dtype)

        if before_column is not None:
            if before_column not in current_df.columns:
                raise ValueError(f"A coluna '{before_column}' não existe no DataFrame.")
            insert_position: int = current_df.columns.get_loc(before_column)
            
            current_df.insert(
                loc=insert_position,
                column=new_day_week_column_name,
                value=category_series,
            )
        else:
            current_df[new_day_week_column_name] = category_series


    @staticmethod
    def create_month_br_category_column(
        current_df: cudf.DataFrame,
        datetime_column_name: str,
        new_month_category_column_name: str = "month",
        before_column: None | str = None,
        case_type: str = "upper",
    ) -> None:
        """
        Cria uma nova coluna com os meses do ano como tipo categórico ordenado,
        com a opção de formatar o case dos nomes dos meses.
        """
        if str(current_df[datetime_column_name].dtype) not in ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]:
            raise TypeError(f"A coluna '{datetime_column_name}' deve estar em datetime.")

        month_categories_base = [
            "JANEIRO", "FEVEREIRO", "MARCO", "ABRIL", "MAIO", "JUNHO",
            "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO",
        ]

        month_mapping = {i + 1: month for i, month in enumerate(month_categories_base)}

        if case_type == "lower":
            month_categories = [m.lower() for m in month_categories_base]
            category_series = current_df[datetime_column_name].dt.month.map(month_mapping).str.lower()

        elif case_type == "capitalize":
            month_categories = [m.capitalize() for m in month_categories_base]
            category_series = current_df[datetime_column_name].dt.month.map(month_mapping).str.capitalize()

        elif case_type == "upper":
            month_categories = month_categories_base
            category_series = current_df[datetime_column_name].dt.month.map(month_mapping)

        else:
            raise ValueError("O parâmetro 'case_type' deve ser 'upper', 'lower' ou 'capitalize'.")

        # Define o tipo categórico ordenado com os nomes formatados
        ordered_dtype = cudf.CategoricalDtype(categories=month_categories, ordered=True)
        
        # Converte a série para o novo tipo categórico ordenado
        category_series = category_series.astype(ordered_dtype)

        # Lógica de inserção da coluna
        if before_column is not None:
            if before_column not in current_df.columns:
                raise ValueError(f"A coluna '{before_column}' não existe no DataFrame.")
            insert_position: int = current_df.columns.get_loc(before_column)
            current_df.insert(loc=insert_position, column=new_month_category_column_name, value=category_series)
        else:
            current_df[new_month_category_column_name] = category_series

        print_job_create_category_column_done(
            column_name=new_month_category_column_name,
            column_from=datetime_column_name
        )



    @staticmethod
    def create_datetime_column_from_date_and_time(
        current_df: cudf.DataFrame,
        date_column_name: str,
        time_column_name: str,
        new_datetime_column_name: str = "datetime",
        before_column: None|str = None
    ) -> None:
        """
        Cria uma nova coluna datetime somando a coluna date e a coluna time (timedelta).
        """
        if str(current_df[date_column_name].dtype) not in ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]:
            raise TypeError(f"A coluna '{date_column_name}' deve estar em datetime.")

        if str(current_df[time_column_name].dtype) not in ["timedelta64[s]", "timedelta[ms]", "timedelta64[ms]", "timedelta[us]",  "timedelta64[us]", "timedelta[ns]", "timedelta64[ns]"]:
            raise TypeError(f"A coluna '{time_column_name}' deve estar em timedelta.")
        
        new_column_values = current_df[date_column_name] + current_df[time_column_name]

        if before_column is not None:
            if before_column not in current_df.columns:
                raise ValueError(f"A coluna '{before_column}' não existe no DataFrame.")
            insert_position: int = current_df.columns.get_loc(before_column)

            current_df.insert(
                loc=insert_position,
                column=new_datetime_column_name,
                value=new_column_values
            )
        else:
            current_df[new_datetime_column_name] = new_column_values

        print_job_create_datetime_column_done(
            column_name=new_datetime_column_name,
            date_column_name=date_column_name,
            time_column_name=time_column_name,
        )


    @staticmethod
    def create_hour_category_column(
        current_df: cudf.DataFrame,
        datetime_column_name: str,
        new_hour_category_column_name: str = "hour",
        before_column: None | str = None,
    ) -> None:
        """
        Cria uma nova coluna com a hora do dia (00 a 23) como tipo categórico ordenado.
        """
        if str(current_df[datetime_column_name].dtype) not in ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]:
            raise TypeError(f"A coluna '{datetime_column_name}' deve estar em datetime.")

        # Define a ordem correta das horas do dia (00 a 23)
        # A lista de categorias garante a ordem numérica correta.
        hour_categories = [f"{i:02d}" for i in range(24)]
        
        # Obtém a série com os números das horas (0 a 23)
        hour_series = current_df[datetime_column_name].dt.hour
        
        # Converte os números das horas para strings com zero à esquerda (ex: 5 -> "05")
        # Isso é necessário para o formato "HH" e para o tipo categórico.
        hour_string_series = hour_series.astype(str).str.zfill(2)
        
        # Define o tipo categórico ordenado
        ordered_dtype = cudf.CategoricalDtype(categories=hour_categories, ordered=True)
        
        # Converte a série de strings para o novo tipo categórico ordenado
        category_series = hour_string_series.astype(ordered_dtype)

        if before_column is not None:
            if before_column not in current_df.columns:
                raise ValueError(f"A coluna '{before_column}' não existe no DataFrame.")
            insert_position: int = current_df.columns.get_loc(before_column)
            
            current_df.insert(
                loc=insert_position,
                column=new_hour_category_column_name,
                value=category_series,
            )
        else:
            current_df[new_hour_category_column_name] = category_series

        print_job_create_category_column_done(
            column_name=new_hour_category_column_name,
            column_from=datetime_column_name
        )
