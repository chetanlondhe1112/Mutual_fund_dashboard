import mysql.connector                                                # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
import streamlit as st
import time
import numpy as np
import plotly.express as px
with open('css/upload_sheet.css') as f:
    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)

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
        st._legacy_dataframe(filter_df)

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
        st._legacy_dataframe(filter_df)


test_df=selected_file_df
columns=test_df.columns.to_list()
#st.write(columns)
columns_r=[x.replace("_", " ") for x in columns]
#st.write(columns_r)

match_sheet_parameter={}
for parameter in filter_df['parameter']:
    arr=[]
    for x in columns_r:
        if parameter in x:
            arr.append(x)
            #st.write(sheet_param)
    match_sheet_parameter[parameter]=[x.replace(" ", "_") for x in arr]



task={}
process={}

filter_parameters=filter_df.columns.to_list()[1:]
#st.write(filter_parameter)
for parameter in filter_df['parameter']:
    ind=np.where(filter_df["parameter"] == parameter)
    par_process={}
    for column in filter_parameters:
        value1=filter_df.at[ind[0][0], column]
        #st.write(value1)
        par_process[column]=value1
    process[parameter]=par_process
    

with st.expander("Parameters matching in sheet"):
    st.write(match_sheet_parameter) 
with st.expander("Process dictionary"):
    st.write(process)
    print(process)

#for key in process:
#    st.write(key)
#    st.write(process[key])
#    for task in process[key]:
#        st.write("{}={}".format(task,process[key][task]))

#st.write("filter creation")
#extracting parameter from filter
task_dictionary={}
for filter_parameter in filter_df['parameter']:
    #st.write(filter_parameter)
    matched_sheet_param_list=match_sheet_parameter[filter_parameter]
    #st.write(matched_sheet_param_list)
    for sheet_param in matched_sheet_param_list:
        task_dictionary[sheet_param]=process[filter_parameter]
        #st.write(sheet_param)
        #st.write(filter_parameter)
        #st.write(process[filter_parameter])

#st.write(task_dictionary)

if st.checkbox("Apply"):
    total_weightage=test_df['Legal_Name']
    for parameter_name in task_dictionary:
        fund_name_col_name='Legal_Name'
        parameter=parameter_name
        condition_1=task_dictionary[parameter]["condition_1"]
        condition_2=task_dictionary[parameter]["condition_2"]
        weightage_1=task_dictionary[parameter]["weightage_1"]
        sort=task_dictionary[parameter]["sort"]
        weightage_2=task_dictionary[parameter]["weightage_2"]

        # temprory column
        parameter_col_df=test_df[[fund_name_col_name,parameter]]
        #st.write(parameter_col_df)
        test_column_df=parameter_col_df[parameter]

        # first Step
        #st.write(test_column_df.sum())
        average=test_column_df.sum()/len(test_column_df)
        #st.write(average)

        # Second step
        arr=[]
        if condition_2=="Above Average":
            for value in test_column_df.to_list():
                if value > average:
                    arr.append(weightage_1)
                else:
                    arr.append(0)
        elif condition_2=='Below Average':
            for value in test_column_df.to_list():
                if value < average:
                    arr.append(weightage_1)
                else:
                    arr.append(0)

        #st.write(arr)
        weightage_df=pd.DataFrame({parameter+'_w':arr})
        #st.write(weightage_df)
        parameter_col_df2=pd.concat([parameter_col_df,weightage_df],axis=1)
        #st.write(parameter_col_df2)

        # second step of weightage
        if sort=="Top 5":
            sorted_df=parameter_col_df.sort_values(by=parameter,ascending=False).head(5)['Legal_Name'].index.to_list()
            #st.write(sorted_df)
        elif sort=="Bottom 5":
            sorted_df=parameter_col_df.sort_values(by=parameter,ascending=False).tail(5)['Legal_Name'].index.to_list()

        for i in sorted_df:
            parameter_col_df2[parameter+'_w'][i]=weightage_2
        total_weightage=pd.concat([total_weightage,parameter_col_df2[parameter+'_w']],axis=1)

        #st._legacy_dataframe(parameter_col_df2)

    #st._legacy_dataframe(total_weightage)
    Result=total_weightage.sum(axis=1)
    total_weightage.insert(1,"Result",Result)
    st._legacy_dataframe(total_weightage)

    st.subheader("Top 5 Funds")

    top_fund_df=total_weightage[['Legal_Name','Result']].sort_values(by='Result',ascending=False).head(5)
    st._legacy_dataframe(top_fund_df)
    
    top_fund_bar = px.bar(top_fund_df, x='Legal_Name',y='Result',text_auto='.2s',template='none')
    #top_fund_bar.update_xaxes(rangeslider_visible=True)
    top_fund_bar.update_xaxes(title_text='')
    top_fund_bar.update_yaxes(title_text='')
    top_fund_bar.update_xaxes(title_font=dict(size=2, family='Courier', color='crimson'))
    top_fund_bar.update_xaxes( tickfont=dict(family='Rockwell', color='black', size=13))
    top_fund_bar.update_yaxes( tickfont=dict(family='Rockwell', color='black', size=11))
    top_fund_bar.update_layout(coloraxis_showscale=False)
    top_fund_bar.update_xaxes(ticklen=0)
    top_fund_bar.update_yaxes(ticklen=0)
    st.plotly_chart(top_fund_bar, use_container_width=True)






