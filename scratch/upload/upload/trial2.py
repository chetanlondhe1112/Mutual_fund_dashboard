from database_conn import connect
import pandas as pd
from sqlalchemy import create_engine
database="stocks dashboard"
master_table="master_sheet_table"
query_table="query_storage"
filter_table="master_filter"

conn=connect()
print(conn)
sq_conn=create_engine()
q="SELECT * FROM "+filter_table
df=pd.read_sql_query(q,conn)
print(df)