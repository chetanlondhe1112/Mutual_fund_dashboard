import mysql.connector                                                # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
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

# Creating Connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database=database
)
sq_conn = create_engine('mysql://root:@localhost/{}'.format(database))

cur=conn.cursor()

