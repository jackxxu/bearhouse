from sqlglot import parse_one, exp
from datetime import date, timedelta
import polars as pl
import os


def _table_data(prefix: str, start_date: date, end_date: date, directory: str) -> pl.LazyFrame:
    filtered_files = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y%m%d")
        path = os.path.join(directory, f"{prefix}_{date_str}.parquet")
        if os.path.exists(path):
            filtered_files.append(path)
        current += timedelta(days=1)

    if not filtered_files:
        raise FileNotFoundError(f"No parquet files found for {prefix} between {start_date} and {end_date}")

    lazy_frames = [pl.scan_parquet(f) for f in filtered_files]
    return pl.concat(lazy_frames, how="vertical_relaxed")  # Use vertical_relaxed to allow for None columns


def execute(sql: str, date_directory: str) -> pl.DataFrame:
    """Parse table names and date range from SQL, load parquet data, and execute the query.

    The `date` column filters in the WHERE clause are used to determine which
    parquet files to load.  Both start_date and end_date are optional – when
    omitted the range defaults to 2000-01-01 / today.
    """
    parsed = parse_one(sql)

    # Extract table names
    tables = {table.name for table in parsed.find_all(exp.Table)}

    # Extract start_date / end_date from WHERE comparisons on the `date` column
    start_date = None
    end_date = None

    for comp in parsed.find_all((exp.GTE, exp.LTE, exp.GT, exp.LT, exp.EQ)):
        left, right = comp.left, comp.right

        if isinstance(left, exp.Column) and left.name == 'date' and isinstance(right, exp.Literal):
            date_val = date.fromisoformat(right.this)
            if isinstance(comp, (exp.GTE, exp.GT)):
                start_date = date_val
            elif isinstance(comp, (exp.LTE, exp.LT)):
                end_date = date_val
            elif isinstance(comp, exp.EQ):
                start_date = end_date = date_val
        elif isinstance(right, exp.Column) and right.name == 'date' and isinstance(left, exp.Literal):
            date_val = date.fromisoformat(left.this)
            if isinstance(comp, (exp.GTE, exp.GT)):
                end_date = date_val
            elif isinstance(comp, (exp.LTE, exp.LT)):
                start_date = date_val
            elif isinstance(comp, exp.EQ):
                start_date = end_date = date_val

    # Handle BETWEEN: date BETWEEN '...' AND '...'
    for between in parsed.find_all(exp.Between):
        if isinstance(between.this, exp.Column) and between.this.name == 'date':
            start_date = date.fromisoformat(between.args['low'].this)
            end_date = date.fromisoformat(between.args['high'].this)

    # Sensible defaults when dates are not specified in the query
    if start_date is None:
        start_date = date(2000, 1, 1)
    if end_date is None:
        end_date = date.today()

    ctx = pl.SQLContext({name: _table_data(name, start_date, end_date, date_directory) for name in tables})
    return ctx.execute(sql).collect()
