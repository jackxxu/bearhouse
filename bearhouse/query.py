from pathlib import Path
from sqlglot import parse_one, exp
from datetime import date, timedelta
from typing import Tuple
import polars as pl


def _table_data(prefix: str, start_date: date, end_date: date, directory: str) -> pl.LazyFrame:
    base = Path(directory)
    lazy_frames = []
    current = start_date
    while current <= end_date:
        path = base / f"{prefix}_{current.strftime('%Y%m%d')}.parquet"
        if path.exists():
            lazy_frames.append(
                pl.scan_parquet(path).with_columns(pl.lit(current).alias("fn_date"))
            )
        current += timedelta(days=1)

    if not lazy_frames:
        raise FileNotFoundError(f"No parquet files found for {prefix} between {start_date} and {end_date}")

    return pl.concat(lazy_frames, how="vertical_relaxed")


def _extract_date_range(parsed: exp.Expression) -> Tuple[date, date]:
    """Return (start_date, end_date) inferred from WHERE filters on the `date` column.

    Handles >=, <=, >, <, =, and BETWEEN.  Defaults to 2000-01-01 / today when
    a bound is not present in the query.
    """
    start_date = None
    end_date = None

    for comp in parsed.find_all((exp.GTE, exp.LTE, exp.GT, exp.LT, exp.EQ)):
        left, right = comp.left, comp.right

        if isinstance(left, exp.Column) and left.name == 'date' and isinstance(right, exp.Literal):
            # Pattern: date <op> 'value'
            date_val = date.fromisoformat(right.this)
            if isinstance(comp, (exp.GTE, exp.GT)):
                start_date = date_val
            elif isinstance(comp, (exp.LTE, exp.LT)):
                end_date = date_val
            elif isinstance(comp, exp.EQ):
                start_date = end_date = date_val

        elif isinstance(right, exp.Column) and right.name == 'date' and isinstance(left, exp.Literal):
            # Pattern: 'value' <op> date  (reversed operands)
            date_val = date.fromisoformat(left.this)
            if isinstance(comp, (exp.GTE, exp.GT)):
                end_date = date_val
            elif isinstance(comp, (exp.LTE, exp.LT)):
                start_date = date_val
            elif isinstance(comp, exp.EQ):
                start_date = end_date = date_val

    for between in parsed.find_all(exp.Between):
        if isinstance(between.this, exp.Column) and between.this.name == 'date':
            # Pattern: date BETWEEN 'low' AND 'high'
            start_date = date.fromisoformat(between.args['low'].this)
            end_date = date.fromisoformat(between.args['high'].this)

    return (
        start_date or date(2000, 1, 1),
        end_date or date.today(),
    )


def execute(sql: str, date_directory: str) -> pl.DataFrame:
    """Load parquet files for the queried tables and date range, then run the SQL.

    The `date` column filters in the WHERE clause determine which parquet files
    are loaded.  Missing bounds default to 2000-01-01 / today.
    """
    parsed = parse_one(sql)
    tables = {table.name for table in parsed.find_all(exp.Table)}
    start_date, end_date = _extract_date_range(parsed)

    ctx = pl.SQLContext({
        name: _table_data(name, start_date, end_date, date_directory)
        for name in tables
    })
    return ctx.execute(sql).collect()
