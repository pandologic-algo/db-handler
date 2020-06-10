import json
import time
import sys
import os

sys.path.append(os.getcwd())
from db_handler.mssql import MSSQLTableReader

def read_table_offset(reader, table_name):
    start = time.time()
    table = reader.read_table(table_name=table_name, offset=0, chunk_size=10000, column=None)
    elapsed = time.time() - start

    print('Elapsed time - {}'.format(elapsed))

    print('Table is empty - {}'.format(table.empty))

    print('Table has duplications - {}'.format(table.duplicated().any()))

    return table


def read_table_col(reader, table_name):
    start = time.time()
    table = reader.read_table(table_name=table_name, offset=1, chunk_size=10000, column='ident')
    elapsed = time.time() - start

    print('Elapsed time - {}'.format(elapsed))

    print('Table is empty - {}'.format(table.empty))

    print('Table has duplications - {}'.format(table.duplicated().any()))

    return table


if __name__ == '__main__':
    # config
    config_file_path = 'tests/config/db_conn.json'
    with open(config_file_path, 'r') as fp:
        db_config = json.load(fp)

    table_name = db_config.pop('table')

    # reader
    reader = MSSQLTableReader(**db_config)

    # tests
    # table = read_table_offset(reader, table_name)
    # print(table.shape)
    table = read_table_col(reader, table_name)
    print(table.shape)
