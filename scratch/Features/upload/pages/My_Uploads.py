import mysql.connector                                                # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
import streamlit as st
import time

#with open('css/upload_sheet.css') as f:
#    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)

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

cur=conn.cursor()


# Functions Declaration

def Rolling_return_upload():
    with st.expander("+ Rolling Return File"):

                # upload file feature
                file = st.file_uploader("Upload your file:",key=3)

                # cheak file upload or not
                uploaded=st.checkbox("Upload",key=3)

                if uploaded:

                    if file:

                        # read file in dataframe
                        file_df = pd.read_csv(file,index_col=None)

                        # show the uploaded file
                        with st.expander(" Your selected sheet:"):         
                            st._legacy_dataframe(file_df)

                        # Take a name for sheet    
                        sheet_name=st.text_input("")

                        # start of uploading process
                        submit=st.button("Submit")

                        if submit and sheet_name:

                            # Uploading process
                            with st.spinner('Uploading...'):

                                time.sleep(3)
                                #st.write(df)
                                file_df.insert(0, "sheet_name", sheet_name)
                                file_df.columns = [x.replace(" ", "_") for x in file_df.columns]                         # replacing space with underscore
                                file_df.to_sql(mf_sheet_table, sq_conn, if_exists='append', index=False)           # uploading file to database
                                #conn.commit()
                                time.sleep(2)
                                st.success("Sheet has uploaded!")
                                time.sleep(2)
                                upload=False
                                st.experimental_rerun()
                                
                        else:
                            st.error("Please give your sheet a name")


# title
st.title("📤My Uploads:")

MF_tab1,MF_tab2=st.tabs(["Equity","Mutual Fund"])

with MF_tab2:

    # layout columns
    MF_tab21,MF_tab22=st.tabs(["My Files","Upload File"])

    #col1_MF=st.columns((5,5,2))

    # Add subheader 
    #col1_MF[0].subheader("My Files")

    with MF_tab22:
    # Uploading files features under button
    #if col1_MF[2].button("Upload File"):

        # expanding container for Upload file
        with st.expander("Quartile File"):
        
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

                    # Take a name for sheet    
                    sheet_name=st.text_input("")

                    # Add rolling return
                    Rolling_return_upload()
                    # start of uploading process
                    submit=st.button("Submit")

                    if submit and sheet_name:

                        # Uploading process
                        with st.spinner('Uploading...'):

                            time.sleep(3)
                            #st.write(df)
                            file_df.insert(0, "sheet_name", sheet_name)
                            file_df.columns = [x.replace(" ", "_") for x in file_df.columns]                         # replacing space with underscore
                            file_df.to_sql(mf_sheet_table, sq_conn, if_exists='append', index=False)           # uploading file to database
                            #conn.commit()
                            time.sleep(2)
                            st.success("Sheet has uploaded!")
                            time.sleep(2)
                            upload=False
                            st.experimental_rerun()
                            
                    else:
                        st.error("Please give your sheet a name")
        
        with st.expander("+ Rolling Return File"):

                # upload file feature
                file = st.file_uploader("Upload your file:",key=2)

                # cheak file upload or not
                uploaded=st.checkbox("Upload",key=2)

                if uploaded:

                    if file:

                        # read file in dataframe
                        file_df = pd.read_csv(file,index_col=None)

                        # show the uploaded file
                        #with st.expander(" Your selected sheet:"):         
                        st._legacy_dataframe(file_df)

                        # Take a name for sheet    
                        sheet_name=st.text_input("")

                        # start of uploading process
                        submit=st.button("Submit")

                        if submit and sheet_name:

                            # Uploading process
                            with st.spinner('Uploading...'):

                                time.sleep(3)
                                #st.write(df)
                                file_df.insert(0, "sheet_name", sheet_name)
                                file_df.columns = [x.replace(" ", "_") for x in file_df.columns]                         # replacing space with underscore
                                file_df.columns = [x.replace(".", "_") for x in file_df.columns]                         # replacing space with underscore

                                file_df.to_sql(mf_rolling_return_table, sq_conn, if_exists='append', index=False)           # uploading file to database
                                #conn.commit()
                                time.sleep(2)
                                st.success("Sheet has uploaded!")
                                time.sleep(2)
                                upload=False
                                st.experimental_rerun()
                                
                        else:
                            st.error("Please give your sheet a name")

    with MF_tab21:

        # Show all file's register
        # Querys to fetch all names of sheets
        file_names_q="SELECT sheet_name FROM " + mf_sheet_table 
        file_names_df=pd.read_sql(file_names_q,sq_conn).drop_duplicates()

        st._legacy_dataframe(file_names_df)

        # selection features to show single file
        selected_file_name=st.selectbox("",options=file_names_df)

        #show df
        selected_file_q="SELECT * FROM " + mf_sheet_table+" WHERE sheet_name='"+selected_file_name+"'"
        selected_file_df=pd.read_sql(selected_file_q,sq_conn).dropna(axis=1,how='all')
        st._legacy_dataframe(selected_file_df)