# Jiboia

**jiboia-gpu** is a Python package to **normalize and optimize DataFrames automatically** efficiently using the Nvidia GPU in the RAPIDS ecosystem.

Key features:
- **String normalization**:
  - Removes extra spaces.
  - Strips leading and trailing spaces.
  - Detects data pollution (e.g., columns that should be numeric but contain strings).
- **Type conversion**:
  - Numeric strings and floats ending in `.0` → integers (`int8`, `int16`, `int32`, …).
  - Converts floats and integers to the most memory-efficient type.
  - Converts strings in various date formats to `datetime` (`yyyy?mm?dd`, `dd?mm?yyyy`, `yyyymmd`, `dd?mm?yy`).
  - Converts time strings (`hhmm UTC`, `hh:mm:ss`, `hh:mm:ss.s`) to `timedelta`.
- **Null standardization** → converts different null representations to `pd.NA`.
- **Automatic CSV detection**:
  - Detects delimiter.
  - Detects encoding.
- **Memory optimization**:
  - Provides memory usage information for DataFrames.
  - Converts columns to the most compact types possible.

---

## Example Usage

```python
from jiboia-gpu import jiboia_gpu as jb

pd.normalize_category("data_frame_cudf")

```
[ ] 1. PADRÃO:
[x] 1.1. Métodos com o padrão pandas/cudf
[ ] 1.2. Implementar dataframe de testes

[ ] 2. STR:

[ ] 2.1 ESPAÇOS
[ ] 2.1.1 [ ] normalizar espaços no início, final e multiplos
[ ] 2.1.2 [ ] função info, para informar dados da coluna (maior e menor string, se há nulos)
[ ] 2.1.3 [ ] Testes Unitários

[ ] 2.2. STRING
[ ] 2.2.1. To uppercase, lowercase e ASCII
[ ] 2.1.2 [ ] Testes Unitários

[ ] 3. NUMEROS
[ ] 3.1.1. Verificar se há tipos não numeros na coluna
[ ] 3.1.1. 
[ ] 3.1.1. Converter colunas string para numeros

- [ ] normalize str testes
- [ ] normalize numeric
- [ ] normalize bool
- [ ] normalize null
- [ ] normalize time
- [ ] normalize time
- [ ] normalize time