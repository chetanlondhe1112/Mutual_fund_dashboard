import mysql.connector                                                # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect

database="streamlit_database"
master_table="master_sheet_table"



# Creating Connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database=database
)
sq_conn = create_engine('mysql://root:@localhost/{}'.format(database))

cur=conn.cursor()


df=pd.read_csv("master_filterV5.2.csv",index_col=0)
print(df)
df.columns = [x.replace(" ", "_") for x in df.columns]

df.to_sql(master_table, sq_conn,if_exists='append',index=False)