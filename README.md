# bearhouse

<img src="assets/bearhouse.png" width="200" />

A toolkit for working date-partitioned Parquet data lakes.


## Data Organization

Bearhouse expects data organized as date-partitioned Parquet files following this convention:

- **File format:** `{type}_{YYYYMMDD}.parquet`
- **Required column:** each file must contain a `date` column of datetime type

**Example:**
```
data/
├── events_20240101.parquet
├── events_20240102.parquet
├── metrics_20240101.parquet
└── metrics_20240102.parquet
```

## Usage

Use `bearhouse.execute()` to run SQL queries directly against your Parquet files. The query's `WHERE` clause on the `date` column determines which files are loaded — only the relevant date range is read from disk.

```python
import bearhouse

df = bearhouse.execute(
    sql="SELECT * FROM events WHERE date >= '2024-01-01' AND date <= '2024-01-31'",
    date_directory="/path/to/data"
)
```

It supports all standard sql functionalities. Below is an example sql with joins:

```sql
SELECT e._index0_ as idx, e.id AS event_id, e.event_type, m.value_int, m.value_float, e.date
FROM events e
JOIN metrics m ON e._index0_ = m._index0_ AND e.date = m.date
WHERE e.date BETWEEN '2026-03-01' AND '2026-03-02'
ORDER BY e._index0_
```

## Installation

```bash
pip install bearhouse
```

### Supported date filter syntax

| Syntax | Example |
|---|---|
| Range (`>=`, `<=`) | `WHERE date >= '2024-01-01' AND date <= '2024-03-31'` |
| Greater/less than (`>`, `<`) | `WHERE date > '2024-06-01'` |
| Exact date (`=`) | `WHERE date = '2024-12-25'` |
| `BETWEEN` | `WHERE date BETWEEN '2024-01-01' AND '2024-12-31'` |

When no date bounds are specified, bearhouse defaults to `2000-01-01` through today.

## Requirements

- Python 3.8+
- [Polars](https://pola.rs/)
- [sqlglot](https://github.com/tobymao/sqlglot)
