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


with st.expander("Connection objects:"):
    st.write("MySQL connection:{}".format(conn))
    st.write("MySQL curser:{}".format(cur))
    st.write("SQLAlchemy connection:{}".format(sq_conn))
    st.write("SQLAlchemy curser:{}".format(sq_cur))
    st.write("check [link](http://localhost:8501/upload_test2)")

# title
st.title("Filter:")

st.subheader("my files")
file_names_q="SELECT sheet_name FROM " + mf_sheet_table 
file_names_df=pd.read_sql(file_names_q,sq_conn).drop_duplicates()

selected_file_name=st.selectbox("",options=file_names_df)

#show df
selected_file_q="SELECT * FROM " + mf_sheet_table+" WHERE sheet_name='"+selected_file_name+"'"
selected_file_df=pd.read_sql(selected_file_q,sq_conn)

with st.expander("My file"):
    st._legacy_dataframe(selected_file_df)

st.subheader("my filter")
with st.expander("Filter"):
    filter_tab,update_tab=st.tabs(["Filter","Update"])
    with filter_tab:
        # show filter
        filter_show_q="SELECT * FROM " + mf_filter_table
        filter_df=pd.read_sql(filter_show_q,sq_conn)
        st.write(filter_df)

    with update_tab:
        if 'df_filter' not in st.session_state:
            st.session_state.df_filter = pd.DataFrame({'parameter': [], 'condition_1': [],
                                'condition_2': [], 'weightage_1': [], 'sort': [], 'weightage_2': []})

        #update filter interface
        u_parameter=st.text_input("Enter Parameter")
        ut_col=st.columns((1,1,1,1,1))
        u_condition_1=ut_col[0].selectbox("Select 1st Condition",options=['Average'])
        u_condition_2=ut_col[1].selectbox("Select 2nd Condition",options=['Above Average','Below Average'])
        u_weightage_1=ut_col[2].number_input("1st Weighatge")
        u_sort=ut_col[3].selectbox("Select sort Condition",options=['Top 5','Bottom 5'])
        u_weightage_2=ut_col[4].number_input("2nd Weightage")

        if st.button('AND'):
            selection = {'parameter':u_parameter, 'condition_1': u_condition_1,
                                'condition_2': u_condition_2, 'weightage_1': u_weightage_1, 'sort': u_sort, 'weightage_2': u_weightage_2}

            st.session_state.df_filter = st.session_state.df_filter.append(selection, ignore_index=True)
            st.write("Selected Conditions List:")
            st.table(st.session_state.df_filter)
            st.experimental_rerun()

        else:
            if len(st.session_state.df_filter):
                st.write("Selected Conditions List:")
                st.table(st.session_state.df_filter)

        if len(st.session_state.df_filter):
                if st.button("Clear"):
                    st.session_state.df_filter = st.session_state.df_filter.drop(len(st.session_state.df_filter) - 1)
                    st.experimental_rerun()

                if st.button("Save"):
                
                        with st.spinner('Saving...'):
                            time.sleep(2)
                            
                            st.session_state.df_filter.to_sql(mf_filter_table, sq_conn, if_exists='append', index=False)
                            time.sleep(2)
                            st.success("Done")
                            st.session_state.df_filter = st.session_state.df_filter.drop(x for x in range(len(st.session_state.df_filter)))
                            time.sleep(2)
                            st.experimental_rerun()    
        # show filter
        filter_show_q="SELECT * FROM " + mf_filter_table
        filter_df=pd.read_sql(filter_show_q,sq_conn)
        st.write(filter_df)



test_df=selected_file_df
columns=test_df.columns.to_list()
st.write(columns)

for parameter in filter_df['parameter']:
    for parameter_x in 



if st.checkbox("Apply"):

        parameter=1
        condition_1=2
        condition_2=3
        weightage_1=4
        select=5
        weightage_2=6

        # temprory column
        sharp_ratio_df=test_df[['Legal_Name','Sharpe_Ratio_1_Yr']]
        #st.write(sharp_ratio_df)
        test_column_df=sharp_ratio_df['Sharpe_Ratio_1_Yr']
        average=test_column_df.sum()/len(test_column_df)
        st.write(average)
        arr=[]

        #st.write(test_column_df.to_list())
        for value in test_column_df.to_list():
            if value > average:
                arr.append(1)
            else:
                arr.append(0)
        #st.write(arr)

        weightage_df=pd.DataFrame({'weightage':arr})
        #st.write(weightage_df)

        sharp_ratio_df2=pd.concat([sharp_ratio_df,weightage_df],axis=1)
        #st.write(sharp_ratio_df2)

        # second step of weightage

        top5_df=sharp_ratio_df.sort_values(by='Sharpe_Ratio_1_Yr',ascending=False).head(5)['Legal_Name'].index.to_list()
        st.write(top5_df)
        for i in top5_df:
            sharp_ratio_df2['weightage'][i]=2

        st.write(sharp_ratio_df2)









