# streamlit database connection with validation for:SQLAlchemy

# import libraries
import mysql.connector                                                          # to setup mysql connection
from sqlalchemy import create_engine                                            # to setup mysql connection
import pandas as pd
import streamlit as st
import time

# credentials declaration
user="localhost"
password=""
host="root"
port="3306"
database="mutual_fund_dashboard"
mf_sheet_table="mf_master_sheets"
mf_filter_table="mf_master_filter"

#sessionstate elements

# Session state objects of database connection 
if "sq_connection_obj" not in st.session_state:                                 # session state SQL connection object         
    st.session_state["sq_connection_obj"]=0

if "sq_cur_obj" not in st.session_state:                                        # session state SQL cursor object
    st.session_state["sq_cur_obj"]=0

# Session state object of database major fectched information


#with st.expander("Session State Objects"):
#    st.write(st.session_state)


# Declaring Functions

# SQL alchemy function
def sqlalchemy_connection(user="root",password="",host="localhost",port=0,database=None):
    try:
        if not database==None:
            connect_string = "mysql://{}:{}@{}:{}/{}".format(user,password,host,port,database)
            return create_engine(connect_string)
    except:
        error="No database passed to function!!!"
        return error

# Function to execute Queries on database with validations
def query_run(query,connection):
    try:
        df=pd.read_sql_query(query,connection)
        return df
    except:
        try:
            sq_conn2 = sqlalchemy_connection(port=3306,database=database)
            sq_cur2=sq_conn2.connect()
        except:
            st.error("Connection lost")
        st.error("Something went wrong!!!")
        st.session_state["sq_cur_obj"]=0       
        time.sleep(2)
        st.experimental_rerun()
        #st.stop()
        return 0


if not st.session_state["sq_cur_obj"]:                                          # Check the SQL cursor object is present is session state or not
    try: 
        # Creating Connection       
        sq_conn = sqlalchemy_connection(port=3306,database=database)
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
        #st.markdown("")
        #st.spinner("connecting...")
        time.sleep(1)
        st.experimental_rerun()
        st.stop()

    #cur=conn.cursor()
    #sq_cur=sq_conn.connect()

else:

    sq_conn=st.session_state["sq_connection_obj"]
    sq_cur=st.session_state["sq_cur_obj"]

    with st.expander("Connection objects:"):
        st.write("SQLAlchemy connection:{}".format(sq_conn))
        st.write("SQLAlchemy curser:{}".format(sq_cur))
        st.write("check [link](http://localhost:8501/upload_test2)")

    # title
    st.title("Filter:")
    if st.button("Hello"):
        st.write("hello")

    # trial query
    #if st.button("Show Sheets"):
    q="SELECT * FROM "+mf_sheet_table
    df=query_run(q,sq_conn)
    st.write(df)

    if st.button("Hey"):
        st.write("hello")
