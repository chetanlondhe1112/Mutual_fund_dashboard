# eliminating the unrequired funds

import mysql.connector   # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
import streamlit as st
import time
import numpy as np
import plotly.express as px
#with open('css/upload_sheet.css') as f:
#    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)

st.set_page_config(layout='wide')

# credentials declaration
database="mutual_fund_dashboard"
mf_sheet_table="demo_master_sheet"
mf_filter_table="mf_master_filter"
mf_rolling_return_table="mf_rolling_return_sheet"
user="localhost"
host="root"
password=""
port="3306"


_="""
try: 
    # Creating Connection
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database=database
    )
    sq_conn = create_engine('mysql://root:@localhost/{}'.format(database))
    sq_cur=sq_conn.connect()
    with st.spinner("Connected to sever"):
        time.sleep(1)
except:
    st.error("Connection lost to server!!!")
    st.stop()"""

# Creating Connection
conn = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database=database
    )
sq_conn = create_engine('mysql://root:@localhost/{}'.format(database))
sq_cur=sq_conn.connect()
cur=conn.cursor()
sq_cur=sq_conn.connect()

_="""
with st.expander("Connection objects:"):
    st.write("MySQL connection:{}".format(conn))
    st.write("MySQL curser:{}".format(cur))
    st.write("SQLAlchemy connection:{}".format(sq_conn))
    st.write("SQLAlchemy curser:{}".format(sq_cur))
    st.write("check [link](http://localhost:8501/upload_test2)")
"""

# title
st.title("Filter:")


# Layout designing
layout_col=st.columns((1,1))


layout_col[0].subheader("my files")
file_names_q="SELECT sheet_name FROM " + mf_rolling_return_table 
file_names_df=pd.read_sql(file_names_q,sq_conn).drop_duplicates()



selected_file_name=layout_col[0].selectbox("",options=file_names_df)

#show df
selected_file_q="SELECT * FROM " + mf_rolling_return_table+" WHERE sheet_name='"+selected_file_name+"'"
selected_file_df=pd.read_sql(selected_file_q,sq_conn).dropna(axis=0,how='any').dropna(axis=1,how='all')


#with st.expander("My file"):
#selected_file_df=selected_file_df.dropna(axis=0,how='any')
layout_col[0].write("Results:{}".format(len(selected_file_df)))
layout_col[0]._legacy_dataframe(selected_file_df)

with layout_col[1]:
    st.subheader("Funds")
    st._legacy_dataframe(selected_file_df['Funds'].dropna(axis=0,how='any'))
    
    st.subheader("Parameters list")
    parameters_list=selected_file_df.columns.to_list()[3:]
    st.write(parameters_list)

    match_sheet_parameter=[]
    search_word='Avg'
    for parameter in parameters_list:
        if search_word in parameter:
            match_sheet_parameter.append(parameter)
    
with layout_col[0]:
    st.subheader("ELiminated bad funds:")

    # remove None from file
    selected_file_df=selected_file_df.loc[(selected_file_df[match_sheet_parameter]!=0).all(1)]
    st.write("Results:{}".format(len(selected_file_df)))
    st._legacy_dataframe(selected_file_df)

st.write(match_sheet_parameter)
st._legacy_dataframe(selected_file_df[match_sheet_parameter]) 

#addition of all years average as total average
