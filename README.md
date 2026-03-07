# bearhouse

toolkit for working with parquet data lakes

## Install
pip install bearhouse

## Data Organization

1. Data are organized by data type and date into parquet files with the format of {type}_{YYYYMMDD}.parquet
2. each parquet file has same index column as `_index0_` (str type) and `date` (datetime type) column

