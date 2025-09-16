from .string_utils import StringUtils
from .null_utils import NullUtils
from .df_utils import DfUtils
from .date_utils import DateUtils
from .time_utils import TimeUtils
from .datetime_utils import DateTimeUtils
from .boolean_utils import BooleanUtils
from .numeric.numeric_utils import NumericUtils
from .csv_utils import CsvUtils

from functools import wraps
from typing import Any, Callable
import inspect


class JiboiaGPUConfig:
    def __init__(self) -> None:
        self.inplace: bool = False
        self.show_log: bool = False


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
    str = _Namespace(StringUtils)
    num = _Namespace(NumericUtils)
    null = _Namespace(NullUtils)
    date = _Namespace(DateUtils)
    time = _Namespace(TimeUtils)
    datetime = _Namespace(DateTimeUtils)
    bool = _Namespace(BooleanUtils)
    df = _Namespace(DfUtils)
    csv = _Namespace(CsvUtils)

    @staticmethod
    def config(*, inplace: bool=False, show_log: bool=True) -> None:     
        config.inplace = inplace
        config.show_log = show_log

jiboia_gpu = JiboiaGPU()

__all__ = ["jiboia_gpu", "JiboiaGPU"]
