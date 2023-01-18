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


# title
st.title("📤Upload File:")

# upload file feature
file = st.file_uploader("Upload your file:")

# cheak file upload or not
uploaded=st.checkbox("Upload")

if uploaded:
    if file:

        # read file in dataframe
        file_df = pd.read_csv(file,index_col=None)

        # show the uploaded file
        with st.expander(" Your selected sheet:"):         
            st._legacy_dataframe(file_df)
        sheet_name=st.text_input("")
        # uploading process
        submit=st.button("Submit")
        if submit and sheet_name:
            with st.spinner('Uploading...'):
                time.sleep(3)
                    
                #st.write(df)
                file_df.insert(0, "sheet_name", sheet_name)
                file_df.columns = [x.replace(" ", "_") for x in file_df.columns]                         # replacing space with underscore
                file_df.to_sql(mf_sheet_table, sq_conn, if_exists='replace', index=False)           # uploading file to database
                #conn.commit()
                time.sleep(2)
                st.success("Sheet has uploaded!")
                time.sleep(2)
                upload=False
                st.experimental_rerun()
                
        else:
            st.error("Please give your sheet a name")
