from ..dataframe.df_utils import DfUtils
from .log_utils import (
    print_text_green,
    print_text_yellow,
    print_warning_encode_file_log
)
from pathlib import Path
import chardet
import csv
import cudf
import os


class CsvUtils:
    @staticmethod
    def get_csv_info(
        file_path: str,
        encoding: None|str = None,
        skiprows: None|int = 0,
        sample_characters: int = 2048,
        sample_bytes: int = 100000
    ) -> dict[str, any]:
        """
        Detecta o delimitador e o encoding de um arquivo CSV.

        :param file_path: caminho do arquivo CSV
        :param encoding: encoding forçado (se None, tenta detectar com chardet)
        :param skiprows: linhas iniciais a ignorar
        :param sample_characters: n de caracteres usados para sniffing
        :param sample_bytes: n de bytes usados para detectar encoding
        :return: (delimiter, encoding_detectado)
                :exemplo_de_retorno:
            {
                'encoding': 'utf-8',
                'confidence': 0.99,
                'language': 'pt',
                'delimiter': ','
            }
        """
        csv_info: dict[str, any] = {
            "encoding": encoding
        }

        # detecta encoding se não informado
        if encoding is None:
            with open(file_path, "rb") as f:
                rawdata = f.read(sample_bytes)
                enc_info = chardet.detect(rawdata)
                encoding = enc_info["encoding"] or "utf-8"

            csv_info = {**csv_info, **enc_info}

        # detecta delimitador
        with open(file_path, "r", encoding=encoding, errors="ignore") as file:
            for _ in range(skiprows):
                next(file, None)  # ignora linhas iniciais

            sample = file.read(sample_characters)
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                delimiter = dialect.delimiter
            except csv.Error:
                delimiter = ";"

            csv_info["delimiter"] =  delimiter

        print(
            print_text_green("Done!"),
            "file:",
            print_text_yellow(file_path),
            "delimiter:",
            print_text_yellow(f'"{csv_info["delimiter"]}"'),
            "encoding:",
            print_text_yellow(csv_info["encoding"]),
        )
        return csv_info


    @staticmethod
    def convert_csv_to_utf8(file_path: str, output_path: str = None) -> str:
        """
        Converte um arquivo CSV para UTF-8.

        :param file_path: caminho do arquivo original
        :param output_path: caminho do arquivo convertido (se None, gera com _utf8.csv)
        :return: caminho do arquivo convertido em UTF-8
        """
        file_path = Path(file_path)

        # se não passar output_path, cria automático
        if output_path is None:
            output_path = file_path.with_name(file_path.stem + "_utf8.csv")

        # detecta encoding
        with open(file_path, "rb") as f:
            rawdata = f.read(100000)
            enc_info = chardet.detect(rawdata)
            source_encoding = enc_info["encoding"] or "latin-1"
            print(f"Encoding detectado: {source_encoding} (confiança={enc_info['confidence']:.2f})")

        # lê no encoding original e salva em UTF-8
        with open(file_path, "r", encoding=source_encoding, errors="ignore") as f_in:
            data = f_in.read()

        with open(output_path, "w", encoding="utf-8") as f_out:
            f_out.write(data)

        print(f"Arquivo convertido salvo em: {output_path}")
        return str(output_path)


    @staticmethod
    def convert_all_csvs_to_utf8(folder_path: str, new_folder: bool = False) -> None:
        """
        Converte todos os arquivos CSV em uma pasta para UTF-8.
        Detecta o encoding de cada arquivo automaticamente.
        
        :param folder_path: caminho da pasta contendo os CSVs
        :param new_folder: se True, cria uma subpasta 'utf8_converted', 
                           se False, salva os arquivos na mesma pasta com sufixo '_utf8'
        """
        folder = Path(folder_path)

        if new_folder:
            output_folder = folder / "utf8_converted"
            output_folder.mkdir(exist_ok=True)
        else:
            output_folder = folder  # salva na mesma pasta

        for csv_file in folder.glob("*.csv"):

            # Detecta encoding
            with open(csv_file, "rb") as f:
                rawdata = f.read(100000)  # lê parte do arquivo
                result = chardet.detect(rawdata)
                source_encoding = result["encoding"] or "latin-1"
                confidence = result["confidence"]
                # print(f"{csv_file.name}: encoding detectado = {source_encoding} (confiança={confidence:.2f})")

                print(
                    "file:",
                    csv_file,
                    "encoding:",
                    print_text_yellow(result["encoding"]),
                    "confidence:",
                    print_text_yellow(confidence),
                )

            # Se já estiver em UTF-8, pula
            if source_encoding.lower() == 'utf-8':
                continue

            # Lê e converte para UTF-8
            with open(csv_file, "r", encoding=source_encoding, errors="ignore") as f_in:
                data = f_in.read()

            if new_folder:
                output_file = output_folder / csv_file.name
            else:
                output_file = output_folder / f"{csv_file.stem}_utf8.csv"

            with open(output_file, "w", encoding="utf-8") as f_out:
                f_out.write(data)

            # print(f"Arquivo convertido salvo em: {output_file}")
            print(
                print_text_green("Done!"),
                "file:",
                print_text_yellow(output_file),
                "converted to",
                print_text_yellow("utf-8"),
            )


    @staticmethod
    def read_files(
        folder_path: str,
        start_part: None|int = 1,
        end_part: None|int = None,
        sep_delimiter: None|str=None,
        skip_rows: int = 0
    ) -> cudf.DataFrame:

        files_csv = sorted(
            [file for file in os.listdir(folder_path) if file.endswith(".csv")]
        )
        
        # Seleciona os arquivos baseado no indice
        start_idx = (start_part - 1) if start_part is not None else 0
        end_idx = end_part if end_part is not None else len(files_csv)
        
        selected_files = files_csv[start_idx:end_idx]
        
        df_cudf: cudf.DataFrame = cudf.DataFrame()
        
        for file_name in selected_files:
            file_path: str = f'{folder_path}{file_name}'
            
            csv_info: dict[str, any] = CsvUtils.get_csv_info(
                file_path=file_path
            )

            if csv_info["encoding"] != "utf-8":
                print_warning_encode_file_log(
                    file_name=file_name,
                    encode=csv_info["encoding"],
                    show_log=True
                )

            if not sep_delimiter:
                sep: str = csv_info["delimiter"]
            else:
                sep: str = sep_delimiter
            
            if DfUtils.is_vram_use_limit():
                break

            df_cudf_part = cudf.read_csv(
                filepath_or_buffer=file_path,
                sep=sep,
                dtype=str,
                skiprows=skip_rows
            )
        
            df_cudf = cudf.concat([df_cudf, df_cudf_part], ignore_index=True)
            
            DfUtils.cudf_size_info(df_cudf, print_info=True)
            del df_cudf_part

        return df_cudf
