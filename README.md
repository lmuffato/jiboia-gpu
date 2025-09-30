# jiboia-gpu

<img src="jiboia_icon.svg" width="120" alt="Jiboia Icon"/>

**jiboia-gpu** is a Python library designed to **normalize data and optimize data types** of a DataFrame efficiently using **NVIDIA GPUs** through the **RAPIDS** ecosystem.

## Requirements

- NVIDIA GPU with CUDA support (CUDA 12+ recommended)
- Compatible cuDF version (>=25.8)
- Python >= 3.9

![Python](https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Nvidia](https://img.shields.io/badge/nvidia-76B900?style=for-the-badge&logo=nvidia&logoColor=white)
![Pytest](https://img.shields.io/badge/pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)
![Spyder](https://img.shields.io/badge/spyder-8C0000?style=for-the-badge&logo=spyderide&logoColor=white)
 ![GoogleColab](https://img.shields.io/badge/googlecolab-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white)
 ![Jupyter](https://img.shields.io/badge/jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)
 
**By [Lucas Muffato](https://www.linkedin.com/in/lucasmuffato/) 🇧🇷**


## Key Features

### String Normalization
- Trims leading and trailing spaces.
- Removes extra whitespace.
- Detects data inconsistencies (e.g., numeric columns containing strings).

### Numeric Conversion**
  - Converts numeric strings and floats ending with `.0` into integers (`int8`, `int16`, `int32`, …).
  - Optimizes numeric types for minimal memory usage.
  - Recognizes many numeric formats, including:
    `"0.1"`, `"10"`, `".0452"`, `"1000.00"`, `"0,1"`, `",50"`, `"1000.000,00"`, `"1,1"`, `"10000"`, `"5e6"`, `"2.5e-2"`, `"0e1"`, `".3e-2"`
  - Categories of numeric recognition:
    - **Standard integers and floats:** `"10"`, `"0.1"`, `"10000"`, `"1000.00"`
    - **Floats without leading zero:** `".0452"`, `".3e-2"`
    - **Scientific notation:** `"5e6"`, `"2.5e-2"`, `"0e1"`
    - **European-style decimals:** `"0,1"`, `",50"`, `"1000.000,00"`
    - **False floats converted to integers:** `"1000.00"`, `"0.0"`, `"1.0"`

### Date/Time Conversion
  - Parses strings representing dates in multiple formats: `yyyy?mm?dd`, `dd?mm?yyyy`, `yyyymmdd`, `dd?mm?yy` → `datetime`.
  - Converts time strings like `hhmm UTC`, `hh:mm:ss`, `hh:mm:ss.s` → `timedelta`.

### Null Standardization
- Converts various null representations to `cudf.NA`.

### Automatic CSV Detection
- Detects the correct delimiter.
- Detects file encoding automatically.

### Memory Optimization
- Provides memory usage information for DataFrames.
- Automatically converts columns to the most memory-efficient types.

### Test coverage with pytest

- ✅ <span style="color:green; font-weight:bold">100%</span> in Numeric Normalization.
- ✅ <span style="color:green; font-weight:bold">100%</span> in String Normalization.


## Table of Contents
- [Requirements](#requirements)
- [Key Features](#key-features)
- [Usage](#usage)
  - [DataFrame Normalization](#dataframe-normalization)
  - [Numeric Normalization](#numeric-normalization)
  - [Date and Time Normalization](#date-and-time-normalization)
  - [Null Normalization](#null-normalization)
  - [String Normalization](#string-normalization)
  - [Time Normalization](#time-normalization)
  - [Boolean Normalization](#boolean-normalization)
  - [Automatic CSV Reading](#automatic-csv-reading)
- [Full Example](#full-example)
- [Contribution Guidelines](#contribution-guidelines)

---

## Usage

### DataFrame Normalization
```python
from jiboia_gpu import jiboia_gpu as jb

jb.df.normalize(df)          # Normalize the entire DataFrame
jb.df.cudf_size_info(df)     # Show RAM and VRAM memory usage
```

### Numeric Normalization
```python
jb.num.normalize(df, column_name)  # Normalize numeric columns
```

### Date and Time Normalization
```python
jb.dt.normalize(df, column_name)   # Convert strings to datetime
jb.time.normalize(df, column_name) # Convert strings to timedelta
```

### Null Normalization
```python
jb.null.normalize(df, column_name) # Convert null values to cudf.NA
```

### String Normalization
```python
jb.str.normalize(df, column_name)  # Remove extra spaces and create categories when appropriate
```

### Time Normalization
```python
jb.time.normalize(df, column_name) # Convert time strings to timedelta
```

### Boolean Normalization
```python
jb.bool.normalize(df, column_name) # Convert values to boolean
```

### Automatic CSV Reading
```python
jb.csv.read_files(
    folder_path="my_folder/",
    start_part=1,
    end_part=10
)  # Read multiple CSV files automatically
```

---

## Full Example
> Below is a detailed example of creating a DataFrame with messy data and performing full normalization.

```python
import jiboia_gpu as jb
import cudf

```python
from jiboia-gpu import jiboia_gpu as jb

# ---- Creating a DataFrame with Mixed Dirty Data --- #

# Strings with multiple spaces, at the beginning and end
col_str = "col_str"
col_str_val = [
    "Surucuçu",
    "Cobra Cipó",
    "King  Cobra",
    "Jararacuçu",
    "NA",
    " Jiboia",
    " Coral Verdadeira ",
    "Jararaca  ",
    "Surucucu",
    "NA",
    None,
    "solid snake",
    "null",
    "unknown",
    "Sea Snake ",
]

# Numbers in string with varied shapes
col_number_str = "col_number_str"
col_number_str_val = [
    "1",
    "0.1",
    "NA",
    ".1",
    "0,1",
    ",50",
    "1000.000,00",
    "1,1",
    "5e6",
    "2.5e-2",
    "0e1",
    "unknown",
    None,
    "0e1",
    ".3e-2",
]

# Integers in string with varied shapes
col_number_false_float_str = "col_number_false_float_str"
col_number_false_float_str_val = [
    "10",
    ".0",
    "1000.00",
    "0.0",
    "100,00",
    "1",
    None,
    "Na",
    "10000",
    "5e2",
    "[1,2,3,4]",
    "0e1",
    None,
    "10",
    "1"
]

# Boolean data in string of varying form
col_bool = "col_bool"
col_bool_val = [
    'YES',
    'No',
    None,
    'invalid',
    'YES',
    'No',
    None,
    'YEs',
    'invalido',
    'yes',
    'NA',
    'on',
    'off',
    None,
    'on'
]

# Date data in string and in various formats
col_date = "col_date"
col_date_val = [
    '15.06.2018',
    '28/01/1990',
    None,
    'invalid',
    '1988 02 08',
    '20211212',
    '01-02-2024',
    '2023/12/20',
    '20234550',
    '2023-12-12',
    '8-2-86',
    '8-12-25',
    '12-12-12',
    '08-12-25',
    None
]

# Datetime in string
col_datetime = "col_datetime"
col_datetime_val = [
    "2025-01-01 01:10:10",
    "2026-02-02 02:20:20",
    "2027-03-03 03:30:30",
    "2028-04-04 04:40:40",
    "2029-05-05 05:50:50",
    "2030-06-06 06:00:00",
    "2031-07-07 07:10:10",
    "invalid",
    "2033-09-09 09:30:30",
    "2034-10-10 10:40:40",
    "2035-11-11 11:50:50",
    "2036-12-12 12:00:00",
    "2037-01-13 13:10:10",
    "2038-02-14 14:20:20",
    "2039-03-15 15:30:30", 
]

# Time data in string and various forms
col_time = "col_time"
col_time_val = [
    "0000UTC",
    "0130UTC",
    "NA",
    "0540 UTC",
    "1200UTC",
    "00:21",
    "1545UTC",
    "invalid",
    "2359UTC",
    "null",
    "23",
    "20:23",
    "10:12:12",
    "07:32:12.1247",
    None
]

# Strings that can be reduced into categories
col_cat = "col_cat"
col_cat_val = [
    "constrictor snake",
    "sea snake",
    "poisonous snake",
    "constrictor snake",
    "invalid",
    "poisonous snake",
    "constrictor snake",
    "sea snake",
    "poisonous snake",
    "constrictor snake",
    None,
    "poisonous snake",
    "constrictor snake",
    "sea snake",
    "poisonous snake",
]

# Normalizing the dataframe
df: cudf.DataFrame = cudf.DataFrame({
    col_str: col_str_val,
    col_number_str: col_number_str_val,
    col_number_false_float_str: col_number_false_float_str_val,
    col_bool: col_bool_val,
    col_date: col_date_val,
    col_datetime: col_datetime_val,
    col_time: col_time_val,
    col_cat: col_cat_val
})

Done! all values null in column col_time converted to <NA>
Done! all duplicate and edge spaces have been removed in column col_cat
Done! all values null in column col_cat converted to <NA>
Done! column col_str converted to object
Done! column col_number_str converted to float64
Done! column col_number_false_float_str converted to int16
Done! column col_bool converted to bool
Done! column col_date converted to object
Done! column col_date converted to datetime64[s]
Done! column col_datetime converted to object
Done! column col_time converted to timedelta64[ns]
Done! column col_cat converted to object
Done! the column col_cat was converted to a category

print(df_normalized.dtypes)
# results:
col_str                                object
col_number_str                        float64
col_number_false_float_str              int16
col_bool                                 bool
col_date                        datetime64[s]
col_datetime                   datetime64[ns]
col_time                      timedelta64[ns]
col_cat                              category
dtype: object
```

---

## Contribution Guidelines

1. **Typing** is mandatory for all variables, attributes, and functions.  
2. **Automated tests** with `pytest` are required for all contributions.  
3. **Project standards** must be followed.  
4. **Imports must be declared in alphabetical order**.
