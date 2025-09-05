from .gpu_normalize_string_utils import GPUNormalizeStringUtils
from .gpu_normalize_null_utils import GPUNormalizeNullUtils
from .gpu_analysis_utils import GPUAnalysisUtils
from .gpu_normalize_date_utils import GPUNormalizeDateUtils
from .gpu_normalize_time_utils import GPUNormalizeTimeUtils
from .gpu_normalize_datetime_utils import GPUNormalizeDateTimeUtils
from .gpu_normalize_boolean_utils import GPUNormalizeBooleanUtils
from .gpu_normalize_numeric_utils import GPUNormalizeNumericUtils
from .gpu_file_analysis_utils import GPUFileAnalysisUtils


class JiboiaGPU:
    # STRING
    normalize_str = staticmethod(GPUNormalizeStringUtils.normalize_str)
    to_category = staticmethod(GPUNormalizeStringUtils.to_category)
    normalize_category = staticmethod(GPUNormalizeStringUtils.normalize_category)
    normalize_unique_values = staticmethod(GPUNormalizeStringUtils.normalize_unique_values)
    
    # NULL
    normalize_na = staticmethod(GPUNormalizeNullUtils.normalize_na)

    # BOOL
    normalize_bool = staticmethod(GPUNormalizeBooleanUtils.normalize_bool)
    
    # NUMBER
    normalize_number = staticmethod(GPUNormalizeNumericUtils.normalize_number)
    is_probably_number = staticmethod(GPUNormalizeNumericUtils.is_probably_number)

    # DATE
    normalize_date = staticmethod(GPUNormalizeDateUtils.normalize_date)
    normalize_datetime = staticmethod(GPUNormalizeDateUtils.normalize_datetime)
    is_date_column_by_samples = staticmethod(GPUNormalizeDateUtils.is_date_column_by_samples)
    is_date = staticmethod(GPUNormalizeDateUtils.is_date)
    to_date_iso = staticmethod(GPUNormalizeDateUtils.to_date_iso)
    to_datetime = staticmethod(GPUNormalizeDateUtils.to_datetime)


    # TIME
    normalize_time = staticmethod(GPUNormalizeTimeUtils.normalize_time)
    is_time = staticmethod(GPUNormalizeTimeUtils.is_time)

    # TIMEDELTA
    normalize_timedelta = staticmethod(GPUNormalizeTimeUtils.normalize_timedelta)

    # DATETIME
    create_datetime_column_from_date_and_time = staticmethod(GPUNormalizeDateTimeUtils.create_datetime_column_from_date_and_time)
    create_day_week_column = staticmethod(GPUNormalizeDateTimeUtils.create_day_week_column)
    create_month_br_category_column = staticmethod(GPUNormalizeDateTimeUtils.create_month_br_category_column)
    create_day_week_br_category_column = staticmethod(GPUNormalizeDateTimeUtils.create_day_week_br_category_column)
    create_hour_category_column = staticmethod(GPUNormalizeDateTimeUtils.create_hour_category_column)

    # ANALYSIS
    drop_columns = staticmethod(GPUAnalysisUtils.drop_columns)
    cudf_size_info = staticmethod(GPUAnalysisUtils.cudf_size_info)
    pandasdf_size_info = staticmethod(GPUAnalysisUtils.pandasdf_size_info)
    get_gpu_memory_info = staticmethod(GPUAnalysisUtils.get_gpu_memory_info)
    is_vram_use_limit = staticmethod(GPUAnalysisUtils.is_vram_use_limit)
    get_index_samples = staticmethod(GPUAnalysisUtils.get_index_samples)
    infer_by_sample = staticmethod(GPUAnalysisUtils.infer_by_sample)

    # FILE
    get_csv_info = staticmethod(GPUFileAnalysisUtils.get_csv_info)
    convert_csv_to_utf8 = staticmethod(GPUFileAnalysisUtils.convert_csv_to_utf8)
    convert_all_csvs_to_utf8 = staticmethod(GPUFileAnalysisUtils.convert_all_csvs_to_utf8)
    read_csv_files = staticmethod(GPUFileAnalysisUtils.read_csv_files)

# instância única para ser usada como namespace
jiboia_gpu = JiboiaGPU()

__all__ = ["jiboia_gpu", "JiboiaGPU"]
