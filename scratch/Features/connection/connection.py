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

#st.experimental_rerun()
if "ms_connection_obj" not in st.session_state:
    st.session_state["ms_connection_obj"]=0

if "sq_connection_obj" not in st.session_state:
    st.session_state["sq_connection_obj"]=0

if "ms_cur_obj" not in st.session_state:
    st.session_state["ms_cur_obj"]=0

if "sq_cur_obj" not in st.session_state:
    st.session_state["sq_cur_obj"]=0

if not st.session_state["sq_cur_obj"]:
    try: 
        # Creating Connection
        
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database=database
        )
        st.session_state["ms_connection_obj"]=conn
        st.session_state["ms_cur_obj"]=conn.cursor()
        
        sq_conn = create_engine('mysql://root:@localhost/{}'.format(database))
        st.session_state["sq_connection_obj"]=sq_conn
        st.session_state["sq_cur_obj"]=sq_conn.connect()
        #sq_cur=sq_conn.connect()
        #with st.spinner("Connecting"):
        #    time.sleep(1)
        st.experimental_rerun()
    except:
        #time.sleep(1)
        #st.spinner("Connecting...")
        st.info("Connecting.............")
        st.markdown("")
        st.spinner("connecting...")
        st.experimental_rerun()
        st.stop()

    #cur=conn.cursor()
    #sq_cur=sq_conn.connect()

else:
    ms_conn=st.session_state["ms_connection_obj"]
    ms_cur=st.session_state["ms_cur_obj"]
    sq_conn=st.session_state["sq_connection_obj"]
    sq_cur=st.session_state["sq_cur_obj"]

    with st.expander("Connection objects:"):
        st.write("MySQL connection:{}".format(ms_conn))
        st.write("MySQL curser:{}".format(ms_cur))
        st.write("SQLAlchemy connection:{}".format(sq_conn))
        st.write("SQLAlchemy curser:{}".format(sq_cur))
        st.write("check [link](http://localhost:8501/upload_test2)")

    # title
    st.title("Filter:")
    if st.button("Hello"):
        st.write("hello")
