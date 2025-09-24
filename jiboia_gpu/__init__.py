
from .boolean.boolean_utils import BooleanUtils
from .csv_utils import CsvUtils
from .chunk_utils import chunk_df, chunk_iterate, chunk_iterate_index
from .dataframe.df_utils import DfUtils
from .datetime.datetime_utils import DateTimeUtils
from .null.null_utils import NullUtils
from .numeric.numeric_utils import NumericUtils
from .string.string_utils import StringUtils
from .time.time_utils import TimeUtils
from .utils import combine_regex
from functools import wraps
from typing import Any, Callable, Literal
import inspect


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


config = JiboiaGPUConfig()


class _Namespace:
    def __init__(self, cls: type) -> None:
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name)
            if callable(attr) and not attr_name.startswith("_"):
                setattr(self, attr_name, self._wrap(attr))

    def _wrap(self, func: Callable[..., Any]) -> Callable[..., Any]:
        sig = inspect.signature(func)
        has_inplace = "inplace" in sig.parameters
        has_show_log = "show_log" in sig.parameters

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if has_inplace:
                kwargs.setdefault("inplace", config.inplace)
            if has_show_log:
                kwargs.setdefault("show_log", config.show_log)
            return func(*args, **kwargs)

        return wrapper


class JiboiaGPU:
    bool = _Namespace(BooleanUtils)
    datetime = _Namespace(DateTimeUtils)
    df = _Namespace(DfUtils)
    csv = _Namespace(CsvUtils)
    num = _Namespace(NumericUtils)
    null = _Namespace(NullUtils)
    str = _Namespace(StringUtils)
    time = _Namespace(TimeUtils)


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

__all__ = ["jiboia_gpu", "JiboiaGPU"]
