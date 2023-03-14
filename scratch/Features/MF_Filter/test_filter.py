# Update in methodology of filtearation of data
# Instead of taking single column for filteration take same parameters various years data and avearage out combinlly and perform filter conditions

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

# function to average the column
def avg(parameter,dataframe):
    _sum=dataframe[parameter].sum()
    average=_sum/len(dataframe[parameter])
    return average

# function for filters 2nd condition
def filter_condition_2(parameter,condition,dataframe,average,weightage_1,weightage_2):

        _parameter=parameter
        _condition=condition
        _average=average
        _weightage_1=weightage_1
        _weightage_2=weightage_2
        
        arr=[]

        if _condition=="Above Average":
            #st.write(test_column_df)
            for value in dataframe[parameter].to_list():
                if value > _average:
                    arr.append(_weightage_1)
                else:
                    arr.append(0)
        elif _condition=='Below Average':
            for value in dataframe[parameter].to_list():
                #st.write(value)
                if value < _average:
                    arr.append(_weightage_1)
                else:
                    arr.append(0)
        elif _condition=='Gold':
            for value in dataframe[parameter].to_list():
                if value == 'Gold':
                    arr.append(_weightage_1)
                else:
                    arr.append(_weightage_2)

        #st.write(arr)
        weightage_df=pd.DataFrame({_parameter+'_w':arr})

        return weightage_df

# function to sort weightage table
def filter_condition_3(fund_name_col,parameter,sort,dataframe_1,dataframe_2,weightage_2):
    # second step of weightage
        _fund_name_col=fund_name_col
        _parameter=parameter
        _sort=sort
        _dataframe_1=dataframe_1
        _dataframe_2=dataframe_2
        _weightage_2=weightage_2

        sorted_df=[]
        if _sort=="Top 5":
            sorted_df=_dataframe_1.sort_values(by=_parameter,ascending=False).head(5)[_fund_name_col].index.to_list()
            #st.write(sorted_df)
        elif _sort=="Bottom 5":
            sorted_df=_dataframe_1.sort_values(by=_parameter,ascending=False).tail(5)[_fund_name_col].index.to_list()

        for i in sorted_df:
            _dataframe_2[_parameter+'_w'][i]=_weightage_2

        return _dataframe_2


def mutual_fund_filter(test_df,process_dic):
    with st.spinner("Applying....."):
        time.sleep(2)
        main_df=test_df.copy()
        #main_df.columns=[x.replace("_", " ") for x in main_df.columns]  
        st.write(main_df)
        total_weightage=main_df[['Legal_Name','ISIN']]
        average_dict={}
        for each_process in process_dic:

            if each_process in main_df.columns.to_list():

                fund_name_col_name='Legal_Name'
                parameter=each_process
                condition_1=process_dic[parameter]["condition_1"]
                condition_2=process_dic[parameter]["condition_2"]
                weightage_1=process_dic[parameter]["weightage_1"]
                sort=process_dic[parameter]["sort"]
                weightage_2=process_dic[parameter]["weightage_2"]

                parameter_col_df=main_df[[fund_name_col_name,parameter]]

                #task 1)
                _condition_1=condition_1
                if _condition_1=='Average':
                    average_value=avg(parameter,parameter_col_df)
                    #st.write(average_value)
                    average_dict[parameter]=average_value

                #task 2)
                _condition_2=condition_2
                weightage_df=filter_condition_2(parameter=parameter,condition=_condition_2,dataframe=parameter_col_df,average=average_value,weightage_1=weightage_1,weightage_2=weightage_2)
                parameter_col_df2=pd.concat([parameter_col_df,weightage_df],axis=1)

                #task 3)
                _sort=sort
                parameter_col_df2=filter_condition_3(fund_name_col_name,parameter,sort,parameter_col_df,parameter_col_df2,weightage_2)
                total_weightage=pd.concat([total_weightage,parameter_col_df2[parameter+'_w']],axis=1)  

        Result=total_weightage.sum(axis=1)
        total_weightage.insert(2,"Result",Result)

    return total_weightage


# title
st.title("Filter:")

st.subheader("my files")
file_names_q="SELECT sheet_name FROM " + mf_sheet_table 
file_names_df=pd.read_sql(file_names_q,sq_conn).drop_duplicates()

selected_file_name=st.selectbox("",options=file_names_df)

#show df
selected_file_q="SELECT * FROM " + mf_sheet_table+" WHERE sheet_name='"+selected_file_name+"'"
selected_file_df=pd.read_sql(selected_file_q,sq_conn).dropna(axis=1,how='all')

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
        u_condition_1=ut_col[0].selectbox("Select 1st Condition",options=['Average','-'])
        u_condition_2=ut_col[1].selectbox("Select 2nd Condition",options=['Above Average','Below Average','Gold'])
        u_weightage_1=ut_col[2].number_input("1st Weighatge")
        u_sort=ut_col[3].selectbox("Select sort Condition",options=['Top 5','Bottom 5','Others'])
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
    

col=st.columns((1,1,1))

with col[0].expander("Parameters matching in sheet"):
    st.write(match_sheet_parameter) 
with col[1].expander("Process dictionary"):
    st.write(process)
#    print(process)
with col[0].expander("Key-Process"):
    for key in process:
        for task in process[key]:
            st.write("{}={}".format(task,process[key][task]))

st.write("filter creation")

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
with col[2].expander("task_dictionary"):
    st.write(task_dictionary)

total_data_df=test_df.copy()
for each_param in match_sheet_parameter:
    if len(match_sheet_parameter[each_param])>1:
        #st.write(match_sheet_parameter[each_param])
        #st.write(len(match_sheet_parameter[each_param]))
        total_data_df[each_param] = total_data_df[match_sheet_parameter[each_param]].sum(axis=1)/len(match_sheet_parameter[each_param])
        total_data_df= total_data_df.drop(columns= match_sheet_parameter[each_param],axis=1)
st._legacy_dataframe(total_data_df)


columns_t=total_data_df.columns.to_list()
#st.write(columns)
columns_t=[x.replace("_", " ") for x in columns_t]

match_sheet_parameter_2={}
for parameter in filter_df['parameter']:
    arr=[]
    for x in columns_t:
        if parameter in x:
            arr.append(x)
            #st.write(sheet_param)
    match_sheet_parameter_2[parameter]=[x.replace(" ", "_") for x in arr]
with col[1].expander("match_sheet_parameter_2"):
    st.write(match_sheet_parameter_2)

process2={}
filter_parameters=filter_df.columns.to_list()[1:]
#st.write(filter_parameter)
for parameter in filter_df['parameter']:
    ind=np.where(filter_df["parameter"] == parameter)
    par_process={}
    for column in filter_parameters:
        value1=filter_df.at[ind[0][0], column]
        #st.write(value1)
        par_process[column]=value1
    process2[parameter]=par_process

task_dictionary2={}
for filter_parameter in filter_df['parameter']:
    #st.write(filter_parameter)
    matched_sheet_param_list=match_sheet_parameter_2[filter_parameter]
    #st.write(matched_sheet_param_list)
    for sheet_param in matched_sheet_param_list:
        task_dictionary2[sheet_param]=process2[filter_parameter]

with col[2].expander("task_dictionary2"):
    st.write(task_dictionary2)

for parameter_name in task_dictionary:
        fund_name_col_name='Legal_Name'
        parameter=parameter_name
        condition_1=task_dictionary[parameter]["condition_1"]
        condition_2=task_dictionary[parameter]["condition_2"]
        weightage_1=task_dictionary[parameter]["weightage_1"]
        sort=task_dictionary[parameter]["sort"]
        weightage_2=task_dictionary[parameter]["weightage_2"]

total_data_df.columns= [x.replace(" ",'_') for x in total_data_df.columns]
st.write(total_data_df)
if st.checkbox("Apply"):
    total_weightage=mutual_fund_filter(total_data_df,task_dictionary2)
    st._legacy_dataframe(total_weightage)


