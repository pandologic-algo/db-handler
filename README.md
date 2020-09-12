# Introduction
The Following package include a simplier API of reading operations from common databases to output pandas as DataFrames.


## 1.	Installation process

stable:

`pip install git+https://github.com/pandologic-algo/db-handler.git`

dev:

`pip install git+https://github.com/pandologic-algo/db-handler.git@dev`

specific version:

```pip install git+https://github.com/pandologic-algo/db-handler.git@v<x.x.x>```

## 2. Examples
Using a MSSQLTableReader:
``` python
from db_handler.mssql import MSSQLTableReader

# db config
db_config = {
  "server": "server",
  "database": "database",
  "schema": "schema",
  "table": "table",
  "user": "user",
  "password": "password",
  "driver": "driver"
}

# reader
reader = MSSQLTableReader(**db_config)

# read table
table1 = reader.read_table(table_name='table_name', chunk_size=10000)

# read table top 1000 row
table1 = reader.read_table(table_name='table_name', top=1000, chunk_size=10000)

# read table by column cursor - "ident"
# starting from cursor column value = 1
table2 = reader.read_table(table_name=table_name, offset=1, chunk_size=10000, column='ident')

# read table by column cursor - "ident"
# starting from cursor column value = 1 until value 10000 (not included)
table3 = reader.read_table(table_name=table_name, top=10000, offset=1, chunk_size=10000, column='ident')

```


## 3. Future
Testing:
- Pytest tests
- Docker testing

Databases support:
- MySQL 
- PostgreSQL
- ElasticSearch
