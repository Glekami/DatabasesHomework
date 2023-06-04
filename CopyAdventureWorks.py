# https://datatofish.com/how-to-connect-python-to-sql-server-using-pyodbc/

import pandas as pd
import pyodbc
import re
from sqlalchemy import URL
from sqlalchemy import create_engine

src = 'Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-5HHDLGR;Database=AdventureWorks2019;Trusted_Connection=yes;'
rslt = 'Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-5HHDLGR;Database=AdventureTraining2;Trusted_Connection=yes;'

conn_src = pyodbc.connect(src)
conn_rslt = pyodbc.connect(rslt)

mssqlserver_table_query = """
    SELECT
          t.name AS table_name
        , s.name AS schema_name
    FROM sys.tables t
    INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id

    UNION

    SELECT
          v.name AS table_name
        , s.name AS schema_name
    FROM sys.views v
    INNER JOIN sys.schemas s
    ON v.schema_id = s.schema_id

    ORDER BY schema_name, table_name;
"""

#creating dictionary with table names and schema

df_src = pd.read_sql_query(mssqlserver_table_query, conn_src)
mssql_tables = dict(zip(df_src.table_name, df_src.schema_name))

conn_url_src = URL.create("mssql+pyodbc", query={"odbc_connect": src})
conn_url_rslt = URL.create("mssql+pyodbc", query={"odbc_connect": rslt})

engine_src = create_engine(conn_url_src)
engine_rslt = create_engine(conn_url_rslt)

for table_name, schema_name in mssql_tables.items():
    conn_url_src = engine_src.connect()
    conn_url_rslt = engine_rslt.connect()

    full_table = f"""
        SELECT
        *
        FROM {schema_name}.{table_name};
    """

    df = pd.read_sql(full_table, conn_url_src)
    df.to_sql(schema=schema_name, name=table_name, con=conn_url_rslt, chunksize=5000, index=False,
              index_label=False, if_exists='replace')

    conn_url_rslt.close()
    conn_url_src.close()

engine_src.dispose()
engine_rslt.dispose()