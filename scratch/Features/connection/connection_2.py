import mysql.connector  # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd # from database_conn import connect
import streamlit as st 
import time

# credentials declaration
database="mutual_fund_dashboard"
mf_sheet_table="mf_master_sheets"
mf_filter_table="mf_master_filter"
user="localhost"
host="root"
password=""
port="3306"

#,pool_pre_ping=True
sq_conn = create_engine('mysql://root:@localhost/{}'.format(database))

#conn = mysql.connector.connect(
#            host="localhost",
#            user="root",
#            passwd="",
#            database=database)

#ms_cur=conn.cursor()
sq_cur=sq_conn.connect()

#st.write(conn)
#st.write(ms_cur)
st.write(sq_conn)
st.write(sq_cur)
if st.button("Close"):
    sq_conn.close()
    st.info(sq_conn)