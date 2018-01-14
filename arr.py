
import pyarrow.parquet as pq

import numpy as np

import pandas as pd

import pyarrow as pa


# This could be coming from a Kafka Stream or File from Disk

# It will be very efficient to use Apache Arrow to save data to disk.

# We can do this in speed with Python



df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                   'two': ['foo', 'bar', 'baz'],
                   'three': [True, False, True]})

table = pa.Table.from_pandas(df)

pq.write_table(table, 'example.parquet')


table2 = pq.read_table('example.parquet')

print(table2.to_pandas())

print(pq.read_table('example.parquet', columns=['one', 'three']))

parquet_file = pq.ParquetFile('example.parquet')

print(parquet_file.metadata)

print(parquet_file.schema)

# Reading Multiples Files and Partitioned Datasets
# Multiple Parquet files constitute a Parquet dataset. These may present in a number of ways:
#
# A list of Parquet absolute file paths
# A directory name containing nested directories defining a partitioned dataset
# A dataset partitioned by year and month may look like on disk:
#
# dataset_name/
#   year=2007/
#     month=01/
#        0.parq
#        1.parq
#        ...
#     month=02/
#        0.parq
#        1.parq
#        ...
#     month=03/
#     ...
#   year=2008/
#     month=01/