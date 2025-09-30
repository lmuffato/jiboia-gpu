
from .boolean.boolean_utils import BooleanUtils
from .utils.csv_utils import CsvUtils
from .utils.chunk_utils import (
    chunk_df,
    chunk_iterate,
    chunk_iterate_index
)
from .dataframe.df_utils import DfUtils
from .datetime.datetime_utils import DateTimeUtils
from .null.null_utils import NullUtils
from .number.number_utils import NumberUtils
from .string.string_utils import StringUtils
from .time.time_utils import TimeUtils
from typing import Literal


class JiboiaGPUConfig:
    def __init__(self) -> None:
        self.inplace: bool = False
        self.show_log: bool = True
        self.chunk_size: int = 500_000
        self.match_min_rate: int = 0
        self.null_values: list[str] = [],
        self.to_case: None|Literal['lower', 'upper']=None,
        self.to_ASCII: bool=False,
        self.bool_number: bool=False
        self.create_category: bool=True

class JiboiaGPU:
    @staticmethod
    def config(
        *,
        inplace: bool=False,
        show_log: bool=True,
        chunk_size: int = 500_000,
        match_min_rate: int = 0,
        null_values: list[str] = [],
        to_case: None|Literal['lower', 'upper']=None,
        to_ASCII: bool=False,
        bool_number: bool=False,
        create_category: bool=True
    ) -> None:     
        config.inplace = inplace
        config.show_log = show_log
        config.chunk_size = chunk_size
        config.match_min_rate = match_min_rate
        config.null_values = null_values
        config.to_case = to_case
        config.to_ASCII = to_ASCII
        config.bool_number = bool_number
        config.create_category = create_category

    @staticmethod
    def reset_config() -> None:
        """
        Reseta as configurações da JiboiaGPU para os valores padrão
        """
        global config
        config = JiboiaGPUConfig()


jiboia_gpu = JiboiaGPU()
config = JiboiaGPUConfig()

bool = BooleanUtils()
csv = CsvUtils()
dt = DateTimeUtils()
df = DfUtils()
null = NullUtils()
num = NumberUtils()
str = StringUtils()
time = TimeUtils()

__all__ = [
    "jiboia_gpu",
    "JiboiaGPU",
    "bool",
    "csv",
    "dt",
    "df",
    "null",
    "num",
    "str",
    "time"
]
