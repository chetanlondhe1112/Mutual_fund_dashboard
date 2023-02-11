import mysql.connector   # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
import streamlit as st
import time
import numpy as np
import plotly.express as px



_=""" Layout definition """

st.set_page_config(layout='wide')

#with open('CSS/upload_sheet.css') as f:
#    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)

_="""credentials declaration """

database="mutual_fund_dashboard"
mf_sheet_table="demo_master_sheet"
mf_filter_table="mf_master_filter"
mf_rolling_return_table="mf_rolling_return_sheet"
user="localhost"
host="root"
password=""
port="3306"



_=""" Creating Connection """

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
with st.expander("Connections"):
    st.write(sq_conn)
    st.write(sq_cur)

_=""" Default values """
rolling_return_param='Avg'


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
def fetch_table(table_name,_connection,sheet_name=None):
    if sheet_name:
        try:
            selected_file_q="SELECT * FROM " + table_name+" WHERE sheet_name='"+sheet_name+"'"
            selected_file_df=pd.read_sql(selected_file_q,_connection).dropna(axis=1,how='all').dropna(axis=0,how='any')
            lenght_of_selected_file_df=len(selected_file_df)

            return selected_file_df,lenght_of_selected_file_df
        except:
            st.error("Server lost.....")
            st.error("Please check connection.....")
    else:
        try:
            selected_file_q="SELECT * FROM " + table_name
            selected_file_df=pd.read_sql(selected_file_q,_connection).dropna(axis=1,how='all').dropna(axis=0,how='any')
            lenght_of_selected_file_df=len(selected_file_df)

            return selected_file_df,lenght_of_selected_file_df
        except:
            st.error("Server lost.....")
            st.error("Please check connection.....")

# function to find filters parameters matching & available in sheet

def paramter_map(test_df,filter_parameter_df):
    columns=test_df.columns.to_list()
    #st.write(columns)
    columns_r=[x.replace("_", " ") for x in columns]
    #st.write(columns_r)
    match_sheet_parameter={}
    for parameter in filter_parameter_df:
        arr=[]
        for x in columns_r:
            if parameter in x:
                arr.append(x)
                #st.write(sheet_param)
        match_sheet_parameter[parameter]=[x.replace(" ", "_") for x in arr]
    return match_sheet_parameter

# function to find word in list of words
def find_words(words,words_list):
    match_sheet_parameter=[]
    f_words=words
    p_words_list=words_list
    for p_word in p_words_list:
        for word in f_words:
            if word in p_word:
                match_sheet_parameter.append(p_word)
    return match_sheet_parameter

# function to average the column
def avg(parameter,dataframe):
    average=dataframe[parameter].sum()/len(dataframe[parameter])
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
    st.session_state['main_file_df'],length_main_file_df=fetch_table(mf_sheet_table,sq_conn,st.session_state['main_file_name'])

# Rolling return dataframe
if 'rr_file_name' not in st.session_state:
    st.session_state['rr_file_name']=rr_file_names_df.iloc[0]['sheet_name']
if 'rolling_return_file_df' not in st.session_state:
    st.session_state['rolling_return_file_df'],length_rolling_return_file_df=fetch_table(mf_rolling_return_table,sq_conn,st.session_state['rr_file_name'])

# filter table
if 'filter_df' not in st.session_state:
    st.session_state['filter_df']=0


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



_=""" title declaration """

st.title("Filter:")



_=""" Layout designing """

layout_col=st.columns((5,1,5,1))



_="""Get tables from the database table """

_=""" table of main file """
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
    layout_col[0]._legacy_dataframe(st.session_state['main_file_df'])
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
    layout_col[2]._legacy_dataframe(st.session_state['rolling_return_file_df'])
else:
    st.info("Empty.....")



_=""" Extract Averages of multiple years from rolling return dataframe """

# Collect average columns from sheet
rr_sheet_df=st.session_state['rolling_return_file_df'].copy()
with st.expander("Parameters list"):
    parameters_list=rr_sheet_df.columns.to_list()
    avg_list=find_words([rolling_return_param],parameters_list)
    st.write(avg_list)
    rr_avg_df=pd.concat([rr_sheet_df['ISIN'], rr_sheet_df[avg_list]],axis = 1)
    st._legacy_dataframe(rr_avg_df)



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
with st.expander("Combined Sheet"):
    st._legacy_dataframe(combined_sheet_df)



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
good_funds_df=combined_sheet_df.loc[(combined_sheet_df[match_sheet_parameter]!=0).all(1)].loc[(combined_sheet_df[match_sheet_parameter]!=0).all(1)].reset_index()

# show df
st.write("Results:{}".format(len(good_funds_df)))
st._legacy_dataframe(good_funds_df)


_=""" Filter """
# FIlter creation
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



_=""" Filter process 
    1) Add average parameters into one column of rolling returns
    2) Average out the multiple years of same paramaters in good funds df in one single parameter coulumn name taken from filter
    3) prepare filter process
    4) apply filter
"""

_=""" 1)Adding the Average parameters with mulitple years data and combining in one Rolling return Column"""
# Find average parameters columns

avg_columns_list=find_words([rolling_return_param],good_funds_df.columns.to_list())
st.write(avg_columns_list)
good_funds_df['Rolling_Return'] = good_funds_df[avg_columns_list].sum(axis=1)/len(avg_columns_list)
st.write(good_funds_df)

# find the filter parameters matching with main combined sheet
matched_parameter_dict=paramter_map(good_funds_df,filter_df['parameter'])
st.write(matched_parameter_dict)

# for each parameters multiple data take average with result in single column with name matching in filter table
for each_param in matched_parameter_dict:
    if len(matched_parameter_dict[each_param])>1:
        good_funds_df[each_param] = good_funds_df[matched_parameter_dict[each_param]].sum(axis=1)/len(avg_columns_list)
st.write(good_funds_df)

with st.expander("fitler to dict"):
    process_dic=filter_df.set_index('parameter').to_dict(orient='index')
    st.write(process_dic)

# Apply filter
test_df=good_funds_df.copy()
# remove testing table columns underscores so that filter paramters can easily map with 
if st.checkbox("Apply"):

    test_df.columns=[x.replace("_", " ") for x in test_df.columns]  
    st.write(test_df.columns.to_list())

    total_weightage=test_df['Legal Name']
    average_dict={}

    for each_process in process_dic:
        st.subheader(each_process)

        if each_process in test_df.columns.to_list():

            fund_name_col_name='Legal Name'
            parameter=each_process
            condition_1=process_dic[parameter]["condition_1"]
            condition_2=process_dic[parameter]["condition_2"]
            weightage_1=process_dic[parameter]["weightage_1"]
            sort=process_dic[parameter]["sort"]
            weightage_2=process_dic[parameter]["weightage_2"]

            parameter_col_df=test_df[[fund_name_col_name,parameter]]
            st.write('parameter_col_df')
            st.write(parameter_col_df)
            #st.write(parameter,condition_1,condition_2,weightage_1,weightage_2,sort)

            #task 1)
            _condition_1=condition_1
            if _condition_1=='Average':
                average_value=avg(parameter,parameter_col_df)
                average_dict[parameter]=average_value


            #task 2)
            _condition_2=condition_2
            weightage_df=filter_condition_2(parameter=parameter,condition=_condition_2,dataframe=parameter_col_df,average=average_value,weightage_1=weightage_1,weightage_2=weightage_2)
            st.write('weightage_df'+' '+parameter+' '+str(average_value))
            st.write(weightage_df)
            parameter_col_df2=pd.concat([parameter_col_df,weightage_df],axis=1)
            st.write('parameter_col_df2')
            st.write(parameter_col_df2)

            #task 3)
            _sort=sort
            parameter_col_df2=filter_condition_3(fund_name_col_name,parameter,sort,parameter_col_df,parameter_col_df2,weightage_2)
            st.write(parameter_col_df2)
            total_weightage=pd.concat([total_weightage,parameter_col_df2[parameter+'_w']],axis=1)    
    st.write(total_weightage)         
    st.write(average_dict)
    #st._legacy_dataframe(total_weightage)
    Result=total_weightage.sum(axis=1)
    total_weightage.insert(1,"Result",Result)
    Weighatge_file=total_weightage.to_csv()
    st.download_button("📥Export",key=0, data=Weighatge_file, file_name='weightage_'+selected_file_name+'.csv')
            
    show_df=total_weightage.copy()
    x=list(range(1, len(show_df)+1))
    show_df.set_index([pd.Index(x), 'Legal Name'],inplace = True)
    st._legacy_dataframe(show_df)
