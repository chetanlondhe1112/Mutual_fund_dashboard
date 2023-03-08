import mysql.connector                                                # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
import streamlit as st
import time

#with open('CSS/upload_sheet.css') as f:
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

_=""" Creating Connection """

# Connection defaults
if "serverout_time" not in st.session_state:
    st.session_state["serverout_time"]=10

# SQL alchemy  connection function
def sqlalchemy_connection(user="root",password="",host="localhost",port=0,database=None):
    try:
        if not database==None:
            connect_string = "mysql://{}:{}@{}:{}/{}".format(user,password,host,port,database)
            return create_engine(connect_string)
    except:
        error="No database passed to function!!!"
        return error

# Session state objects of database connection 
if "sq_connection_obj" not in st.session_state:                                 # session state SQL connection object         
    st.session_state["sq_connection_obj"]=0
if "sq_cur_obj" not in st.session_state:                                        # session state SQL cursor object
    st.session_state["sq_cur_obj"]=0

# Connection & disconnection process
if not st.session_state["sq_cur_obj"]:                                          # Check the SQL cursor object is present is session state or not
    try: 
        # Creating Connection       
        sq_conn = sqlalchemy_connection(port=3306,database=database)
        st.session_state["sq_connection_obj"]=sq_conn
        st.session_state["sq_cur_obj"]=sq_conn.connect()
        st.experimental_rerun()
    except:
        
        st.info("Connecting.............")
        st.experimental_rerun()
        st.stop()
else:
    sq_conn=st.session_state["sq_connection_obj"]
    sq_cur=st.session_state["sq_cur_obj"]
    #with st.expander("Connection objects:"):
    #    st.write("SQLAlchemy connection:{}".format(sq_conn))
    #    st.write("SQLAlchemy curser:{}".format(sq_cur))
    #    st.write("check [link](http://localhost:8501/upload_test2)")
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

def rem_under_scr_col_list(df):
        if str(type(df))== "<class 'pandas.core.frame.DataFrame'>":
            columns = []
            for i in df:
                print(i)
                columns.append(i)
            rem_under_scored = [x.replace("_", " ")for x in df]        #returns list of underscore removed columns
            return rem_under_scored
        elif type(df)==list:
            rem_under_scored = [x.replace("_", " ") for x in df]  # returns list of underscore removed columns
            return rem_under_scored
        elif str(type(df))=="<class 'pandas.core.series.Series'>":
            rem_under_scored = [x.replace("_", " ") for x in list(df)]
            return rem_under_scored

def add_col_db(df,table_name,_cursor):
        for i in range(len(df)):
            col=list(df.iloc[i])
            #print(col)
            #q = "ALTER TABLE arkonet_employee_temp ADD project varchar(50);"
            query="ALTER TABLE "+table_name+" ADD "
            for x in col:
                if str(type(x))!="<class 'numpy.float64'>" and str(type(x))!="<class 'numpy.int64'>" :
                    x = x.replace(" ", "_")
                    query+=" "+ x
                else:
                    query+=" ("+str(x)+");"
            #print(query)
            _cursor.execute(query)
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
        with st.expander("+ Main File"):
        
            # upload file feature
            file = st.file_uploader("Upload your file:")

            # cheak file upload or not
            uploaded=st.checkbox("Upload")

            if uploaded:
  
                if 'df_temp' not in st.session_state:
                    st.session_state.df_temp = pd.DataFrame({'Parameter Name': [], 'Datatype': [], 'Range': []})
                if file:
                    # Take a name for sheet 
                    file_df = pd.read_csv(file,thousands=',',index_col=None)

                    st.write(" Your selected sheet:") 

                    st._legacy_dataframe(file_df)
                    sheet_name=st.text_input("Name")

                    master_sheet_column_set = set(file_df.columns)
                    # uploading new column
                    q = "SHOW COLUMNS FROM "+mf_sheet_table
                    database_column = pd.read_sql_query(q, conn)        #type=pandas.core.series.Series
                    db_col_list = database_column['Field']
                    rem_us_col = rem_under_scr_col_list(db_col_list[1:])
                    database_column_set = set(rem_us_col)
                    new_column_set = master_sheet_column_set.difference(database_column_set)

                    if new_column_set:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.subheader("Your New sheets columns:{}".format(len(file_df.columns)))
                            st._legacy_dataframe(list(file_df.columns), height=200)
                        with col2:
                            st.subheader("your database columns:{}".format(len(rem_us_col)))
                            st._legacy_dataframe(rem_us_col, height=200)
                        with col3:
                            st.subheader("Extra new columns:{}".format(len(new_column_set)))
                            st._legacy_dataframe(new_column_set, height=200)

                        copy_new_column_set = new_column_set
                        #st.experimental_rerun()
                        column_list = copy_new_column_set.difference(set(st.session_state.df_temp['Parameter Name']))
                        col1, col2, col3, col4 = st.columns(4)
                        with col2:
                            datatype = st.selectbox("Select Datatype for column :",options=('FLOAT','VARCHAR'))
                        with col3:
                            length = st.number_input("Select Range for column:",min_value=10, max_value=50, value=20, step=1)
                        with col1:
                            column = st.selectbox("Choose new column:", options=column_list)

                        if col4.button("AND"):
                            selection = {'Parameter Name': column, 'Datatype': datatype, 'Range':length}
                            st.session_state.df_temp=st.session_state.df_temp.append({'Parameter Name': column, 'Datatype': datatype, 'Range': length}, ignore_index=True)
                            st.write("Selected Columns List:")
                            st.write(st.session_state.df_temp)
                            st.experimental_rerun()
                        if len (st.session_state.df_temp)>0:
                            if st.button("clear"):
                                st.session_state.df_temp = st.session_state.df_temp.drop(len(st.session_state.df_temp) - 1)
                                st.experimental_rerun()
                                    
                            else:
                                st.write("Selected Columns List:")
                                st.write(st.session_state.df_temp)

                            if st.button("Add Columns"):
                                with st.spinner('Uploading...'):
                                    time.sleep(1)
                                    add_col_db(st.session_state.df_temp, mf_sheet_table, sq_cur)
                                    st.success("Columns Has successfully added to database!")
                                    time.sleep(2)
                                    st.session_state.df_temp.drop(st.session_state.df_temp.index, inplace=True)
                                    st.experimental_rerun()
                    # Add rolling return
                    #Rolling_return_upload()
                    # start of uploading process
                    else:
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
                else:
                    st.warning("Please upload file.....")


        with st.expander("+ Rolling Return File"):

                # upload file feature
                file = st.file_uploader("Upload your file:",key=2)

                # cheak file upload or not
                uploaded=st.checkbox("Upload",key=2)

                if uploaded:
                    if 'df_temp2' not in st.session_state:
                        st.session_state.df_temp2 = pd.DataFrame({'Parameter Name': [], 'Datatype': [], 'Range': []})
                    if file:

                        # read file in dataframe
                        file_df = pd.read_csv(file,index_col=None)

                        # show the uploaded file
                        st.write(" Your selected sheet:")    
                        st._legacy_dataframe(file_df)

                        # Take a name for sheet    
                        sheet_name=st.text_input("")

                        master_sheet_column_set = set(file_df.columns)
                        # uploading new column
                        q = "SHOW COLUMNS FROM "+mf_rolling_return_table
                        database_column = pd.read_sql_query(q, conn)        #type=pandas.core.series.Series
                        db_col_list = database_column['Field']
                        rem_us_col = rem_under_scr_col_list(db_col_list[1:])
                        database_column_set = set(rem_us_col)
                        new_column_set = master_sheet_column_set.difference(database_column_set)

                        if new_column_set:
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.subheader("Your New sheets columns:{}".format(len(file_df.columns)))
                                st._legacy_dataframe(list(file_df.columns), height=200)
                            with col2:
                                st.subheader("your database columns:{}".format(len(rem_us_col)))
                                st._legacy_dataframe(rem_us_col, height=200)
                            with col3:
                                st.subheader("Extra new columns:{}".format(len(new_column_set)))
                                st._legacy_dataframe(new_column_set, height=200)
                                #st.write(list(file_df.columns))
                                #st.write(rem_us_col)
                                #st.write(new_column_set)

                            copy_new_column_set = new_column_set
                            #st.experimental_rerun()
                            column_list = copy_new_column_set.difference(set(st.session_state.df_temp2['Parameter Name']))
                            col1, col2, col3, col4 = st.columns(4)
                            with col2:
                                datatype = st.selectbox("Select Datatype for column :",options=('FLOAT','VARCHAR'))
                            with col3:
                                length = st.number_input("Select Range for column:",min_value=10, max_value=50, value=20, step=1)
                            with col1:
                                column = st.selectbox("Choose new column:", options=column_list)

                            if col4.button("AND"):
                                selection = {'Parameter Name': column, 'Datatype': datatype, 'Range':length}
                                st.session_state.df_temp=st.session_state.df_temp.append({'Parameter Name': column, 'Datatype': datatype, 'Range': length}, ignore_index=True)
                                st.write("Selected Columns List:")
                                st.write(st.session_state.df_temp2)
                                st.experimental_rerun()
                            if len (st.session_state.df_temp2)>0:
                                if st.button("clear"):
                                    st.session_state.df_temp2 = st.session_state.df_temp2.drop(len(st.session_state.df_temp2) - 1)
                                    st.experimental_rerun()
                                        
                                else:
                                    st.write("Selected Columns List:")
                                    st.write(st.session_state.df_temp2)

                                if st.button("Add Columns"):
                                    with st.spinner('Uploading...'):
                                        time.sleep(1)
                                        add_col_db(st.session_state.df_temp2, mf_rolling_return_table, sq_cur)
                                        st.success("Columns Has successfully added to database!")
                                        time.sleep(2)
                                        st.session_state.df_temp2.drop(st.session_state.df_temp2.index, inplace=True)
                                        st.experimental_rerun()
                        else:
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
                    else:
                        st.warning("Please upload file.....")

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