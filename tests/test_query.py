import os
import polars as pl

from bearhouse import query


def _write_parquet(dirpath, prefix, iso_date, rows):
    # filenames are expected to be <prefix>_YYYYMMDD.parquet
    fname = f"{prefix}_{iso_date.replace('-', '')}.parquet"
    path = os.path.join(dirpath, fname)
    df = pl.DataFrame(rows)
    df.write_parquet(path)
    return path


def test_execute_between_and_eq(tmp_path):
    # prepare fixture files
    d1 = '2026-03-01'
    d2 = '2026-03-02'

    events1 = [{'id': 1, 'event_type': 'click', 'date': d1}]
    events2 = [{'id': 2, 'event_type': 'view', 'date': d2}]

    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir()

    _write_parquet(str(fixtures_dir), 'events', d1, events1)
    _write_parquet(str(fixtures_dir), 'events', d2, events2)

    # query a range -> expect two rows
    sql = """
    SELECT id, event_type, date
    FROM events
    WHERE date BETWEEN '2026-03-01' AND '2026-03-02'
    ORDER BY id
    """

    df = query.execute(sql, str(fixtures_dir))
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 2
    assert df['id'].to_list() == [1, 2]

    # query a single date -> expect one row
    sql2 = "SELECT * FROM events WHERE date = '2026-03-01'"
    df2 = query.execute(sql2, str(fixtures_dir))
    assert df2.shape[0] == 1
    assert df2['id'].to_list() == [1]


def test_execute_with_multiple_data_types(tmp_path):
    # create fixtures with integer and float columns
    d1 = '2026-03-01'
    d2 = '2026-03-02'

    metrics1 = [{'id': 1, 'value_int': 10, 'value_float': 1.5, 'date': d1}]
    metrics2 = [{'id': 2, 'value_int': 20, 'value_float': 2.5, 'date': d2}]

    fixtures_dir = tmp_path / "fixtures2"
    fixtures_dir.mkdir()

    _write_parquet(str(fixtures_dir), 'metrics', d1, metrics1)
    _write_parquet(str(fixtures_dir), 'metrics', d2, metrics2)

    sql = """
    SELECT id, value_int, value_float
    FROM metrics
    WHERE date BETWEEN '2026-03-01' AND '2026-03-02'
    ORDER BY id
    """

    df = query.execute(sql, str(fixtures_dir))
    assert df.shape[0] == 2
    assert df['value_int'].to_list() == [10, 20]
    assert df['value_float'].to_list() == [1.5, 2.5]


def test_join_metrics_and_events(tmp_path):
    # write events and metrics into the same parquet directory and join them
    d1 = '2026-03-01'
    d2 = '2026-03-02'

    # include explicit _index0_ so we can join across different physical types
    events1 = [{'_index0_': 0, 'id': 1, 'event_type': 'click', 'date': d1}]
    events2 = [{'_index0_': 1, 'id': 2, 'event_type': 'view', 'date': d2}]

    # metrics use same _index0_ and date values so join keys match
    metrics1 = [{'_index0_': 0, 'id': 1, 'value_int': 100, 'value_float': 10.5, 'date': d1}]
    metrics2 = [{'_index0_': 1, 'id': 2, 'value_int': 200, 'value_float': 20.5, 'date': d2}]

    fixtures_dir = tmp_path / "fixtures_join"
    fixtures_dir.mkdir()

    _write_parquet(str(fixtures_dir), 'events', d1, events1)
    _write_parquet(str(fixtures_dir), 'events', d2, events2)
    _write_parquet(str(fixtures_dir), 'metrics', d1, metrics1)
    _write_parquet(str(fixtures_dir), 'metrics', d2, metrics2)

    sql = """
    SELECT e._index0_ as idx, e.id AS event_id, e.event_type, m.value_int, m.value_float, e.date
    FROM events e
    JOIN metrics m ON e._index0_ = m._index0_ AND e.date = m.date
    WHERE e.date BETWEEN '2026-03-01' AND '2026-03-02'
    ORDER BY e._index0_
    """

    df = query.execute(sql, str(fixtures_dir))
    assert df.shape[0] == 2
    assert df['idx'].to_list() == [0, 1]
    assert df['event_id'].to_list() == [1, 2]
    assert df['event_type'].to_list() == ['click', 'view']
    assert df['value_int'].to_list() == [100, 200]
    assert df['value_float'].to_list() == [10.5, 20.5]

