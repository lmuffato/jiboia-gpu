def print_text_red(text: str) -> str:
    return f"\033[1;31m{text}\033[0m"

def print_text_yellow(text: str) -> str:
    return f"\033[1;33m{text}\033[0m"

def print_text_green(text: str) -> str:
    return f"\033[1;32m{text}\033[0m"

def print_log(
    column_name: str,
    column_type: str,
    show_log: bool=True
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "column",
            print_text_yellow(column_name),
            "converted to",
            print_text_yellow(column_type)
        )

def print_normalize_type_log(
    column_name: str,
    value_original: str,
    value_final: str,
    show_log: bool=True
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "all values",
            print_text_yellow(value_original),
            "in column",
            print_text_yellow(column_name),
            "converted to",
            print_text_yellow(value_final)
        )

def print_normalize_space_log(
    column_name: str,
    show_log: bool=True,
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "all duplicate and edge",
            print_text_yellow("spaces"),
            "have been removed in column",
            print_text_yellow(column_name)
        )


def print_normalize_string_log(
    column_name: str,
    to_case: None|str=None,
    to_ASCII: bool = False,
    show_log: bool=True
) -> None:
    if show_log:
        to_case_msg: str = ''

        if to_case == "upper":
            to_case_msg = "uppercase"
        if to_case == "lower":
            to_case_msg = "lowercase"

        to_ASCII_msg: str = "ASCII" if to_ASCII else ""

        msg: str = " ".join(filter(None, [to_case_msg, to_ASCII_msg]))

        if len(msg) > 0:
            print(
                print_text_green("Done!"),
                "in column",
                print_text_yellow(column_name),
                "all",
                print_text_yellow("string"),
                "were converted to",
                print_text_yellow(msg)
            )


def print_normalize_df_null_log(
    show_log: bool=True
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "all",
            print_text_yellow("null"),
            "values has converted to",
            print_text_yellow("<NA>")
        )


def print_to_category_log(
    column_name: str,
    show_log: bool=True
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "the column",
            print_text_yellow(column_name),
            "was converted to a",
            print_text_yellow("category")
        )


def print_normalize_df_type_log(
    value_original: str,
    value_final: str,
    show_log: bool=True
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "all",
            print_text_yellow(value_original),
            "converted to",
            print_text_yellow(value_final)
        )


def print_normalize_df_type_log(
    value_original: str,
    value_final: str,
    show_log: bool=True
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "all",
            print_text_yellow(value_original),
            "converted to",
            print_text_yellow(value_final)
        )



def print_drop_column_log(
    columns_to_delete: list[str],
    show_log: bool=True
) -> None:
    if show_log:
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


def print_warning_encode_file_log(
    file_name: str,
    encode: str,
    show_log: bool=True
) -> None:
    if show_log:
        print(
            print_text_red("Warning!"),
            "the",
            print_text_yellow(file_name),
            "has encoding",
            print_text_yellow(encode),
            "and cudf only supports",
            print_text_yellow("utf-8")
        )


def print_normalize_df_space_log(
    show_log: bool=True,
) -> None:
    if show_log:
        print(
            print_text_green("Done!"),
            "all duplicate and edge",
            print_text_yellow("spaces"),
            "have been",
            print_text_yellow("removed")
        )


def print_normalize_df_string_log(
    to_case: None|str=None,
    to_ASCII: bool = False,
    show_log: bool=True
) -> None:
    if show_log:
        to_case_msg: str = ''

        if to_case == "upper":
            to_case_msg = "uppercase"
        if to_case == "lower":
            to_case_msg = "lowercase"

        to_ASCII_msg: str = "ASCII" if to_ASCII else ""

        msg: str = " ".join(filter(None, [to_case_msg, to_ASCII_msg]))

        if len(msg) > 0:
            print(
                print_text_green("Done!"),
                "all",
                print_text_yellow("strings"),
                "were converted to",
                print_text_yellow(msg)
            )
