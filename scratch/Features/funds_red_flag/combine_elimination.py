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



_="""Function Definations"""

# function to fetch sheet names
#@st.experimental_memo(show_spinner=True)
def sheet_names(table_name,_connection):
    try:
        file_names_q="SELECT sheet_name FROM " + table_name 
        file_names_df=pd.read_sql(file_names_q,_connection).drop_duplicates()

        return file_names_df,len(file_names_df)
    except:
        st.error("Server lost.....")
        st.error("Please check connection.....")

# function to fetch table
#@st.experimental_memo(show_spinner=True)
def fetch_table(table_name,sheet_name,_connection):
    try:
        selected_file_q="SELECT * FROM " + table_name+" WHERE sheet_name='"+sheet_name+"'"
        selected_file_df=pd.read_sql(selected_file_q,_connection).dropna(axis=1,how='all').dropna(axis=0,how='any')

        lenght_of_selected_file_df=len(selected_file_df)

        return selected_file_df,lenght_of_selected_file_df
    except:
        st.error("Server lost.....")
        st.error("Please check connection.....")


_="""session state"""
with st.expander("Session state variables"):
    st.write(st.session_state)


_=""" on load session state variables data"""
# fetch main file
main_file_names_df,length_main_file_names_df=sheet_names(mf_sheet_table,sq_conn)
# fetch main file
rr_file_names_df,length_rr_file_names_df=sheet_names(mf_rolling_return_table,sq_conn)


_="""Sessionstate variables"""

# Main file dataframe
if 'main_file_name' not in st.session_state:
    st.session_state['main_file_name']=main_file_names_df.iloc[0]['sheet_name']
if 'main_file_df' not in st.session_state:
    st.session_state['main_file_df'],length_main_file_df=fetch_table(mf_sheet_table,st.session_state['main_file_name'],sq_conn)

# Rolling return dataframe
if 'rr_file_name' not in st.session_state:
    st.session_state['rr_file_name']=rr_file_names_df.iloc[0]['sheet_name']
if 'rolling_return_file_df' not in st.session_state:
    st.session_state['rolling_return_file_df'],length_rolling_return_file_df=fetch_table(mf_rolling_return_table,st.session_state['rr_file_name'],sq_conn)


_=""" Session state Buttons """

# FIles capture button

# main file button
if 'main_file_bt' not in st.session_state:
    st.session_state['main_file_bt'] = False  
def main_file_bt_callback():
    st.session_state['main_file_bt'] = True

# rolling return button
if 'rr_file_bt' not in st.session_state:
    st.session_state['rr_file_bt'] = False 
def search_button_callback():
    st.session_state['rr_file_bt'] = True


# title
st.title("Filter:")

# Layout designing
layout_col=st.columns((5,1,5,1))


# Get tables from the database table

_=""" table of main file"""

layout_col[0].subheader("Main Files:")

selected_file_name=layout_col[0].selectbox("",options=main_file_names_df,key=1)

# button 
if layout_col[1].button("<",key=1):
    main_file_df,length_main_file_df=fetch_table(mf_sheet_table,selected_file_name,sq_conn) #fetch table
    st.session_state['main_file_df']=main_file_df   #update sessionsate main file df
    st.experimental_rerun()

# show table
if len(st.session_state['main_file_df']):
    layout_col[0].write("Results:{}".format(len(st.session_state['main_file_df'])))
    layout_col[0].write(st.session_state['main_file_df'])
else:
    st.info("Empty.....")

_=""" table of rolling return file """

layout_col[2].subheader("Rolling Return Files:")

selected_file_name=layout_col[2].selectbox("",options=rr_file_names_df,key=2)

# button
if layout_col[3].button("<",key=2):
    rolling_return_file_df,length_rolling_return_file_df=fetch_table(mf_rolling_return_table,selected_file_name,sq_conn)
    st.session_state['rolling_return_file_df']=rolling_return_file_df
    st.experimental_rerun()

# show table
if len(st.session_state['rolling_return_file_df']):
    layout_col[2].write("Results:{}".format(len(st.session_state['rolling_return_file_df'])))
    layout_col[2].write(st.session_state['rolling_return_file_df'])
else:
    st.info("Empty.....")


_=""" Extract Averages of multiple years from rolling return dataframe """

# Collect average columns from sheet
rr_sheet_df=st.session_state['rolling_return_file_df'].copy()
with st.expander("Parameters list"):
    parameters_list=rr_sheet_df.columns.to_list()
    st.write(parameters_list)
    match_sheet_parameter=[]
    search_word='Avg'
    for parameter in parameters_list:
        if search_word in parameter:
            match_sheet_parameter.append(parameter)
    st.write(match_sheet_parameter)
    rr_avg_df=pd.concat([rr_sheet_df['ISIN'], rr_sheet_df[match_sheet_parameter]],axis = 1)
    st.write(rr_avg_df)


_=""" Combine the average dataframe from rolling return with main sheet dataframe """

# concat the main file dataframe with  average dataframe
main_sheet_df=st.session_state['main_file_df'].copy()   # copying session state dataframe in temporary datframe for analysis

#pd.concat([df1.set_index('A'),df2.set_index('A')], axis=1, join='inner').reset_index() ....method to concat dataframes with one specific column
_="""
    The legal names from main files are not similar 
    with the Funds names in Rolling return file,
    so we going to select 'ISIN' as specific column 
    as a index for concatenating the two dataframe with base of 'ISIN'
"""
combined_sheet_df=pd.concat([main_sheet_df.set_index('ISIN'), rr_avg_df.set_index('ISIN')],axis = 1).reset_index().dropna(axis=1,how='all').dropna(axis=0,how='any')
st.write(combined_sheet_df)


_=""" Eliminating the bad funds (with no available data for 3 years and 5 years) """

# Collecting columns with elimination year number: 3,5 yrs
with st.expander("Parameters list"):
    st.write('Collecting columns with elimination year number: 3,5 yrs')
    parameters_list=combined_sheet_df.columns.to_list()
    st.write(parameters_list)
    match_sheet_parameter=[]
    years=['3','5']
    for parameter in parameters_list:
        for yr in years:
            if yr in parameter:
                match_sheet_parameter.append(parameter)
    st.write(match_sheet_parameter)

# Forwarding the elimination years dataframe for elimination of funds
st.subheader("Good funds:")
st.write("Eliminated bad funds( funds with no data available for 3,5 yrs)")

# elimination method
good_funds_df=combined_sheet_df.loc[(combined_sheet_df[match_sheet_parameter]!=0).all(1)]

# show df
st.write("Results:{}".format(len(good_funds_df)))
st.write(good_funds_df)




