import mysql.connector   # to setup mysql connection
from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
import streamlit as st
import time
import numpy as np
import plotly.express as px
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
from datetime import datetime
from sqlalchemy import create_engine,text
from Home import mf_sheet_table,mf_rolling_return_table,mf_filter_table
from Home import sqlalchemy_connection
import plotly.graph_objects as go
import seaborn as sns
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb


_=""" Layout definition """

#st.set_page_config(layout='wide', page_icon='📈')


_=""" CSS of dashboard """
with open('CSS/master_filter.css') as f:
    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)



_=""" Refresh warning process"""
if len(st.session_state) == 0:
    st.warning("Please! Don't Refresh your browser.")
    st.info("Always, use dashboard Default Refresh!")
    with st.expander('Do This:'):
        video_file = open('video/dashboard_refresh.mp4', 'rb')
        video_bytes = video_file.read()
        st.video(video_bytes)
    st.stop()   


_=""" Authentiation Cheak"""
if not st.session_state["authentication_status"]:
    st.error("Please Login...")
    st.stop()
    


_=""" Defaults"""
username=st.session_state["username"]
current_date = datetime.now()
private_columns=['date_time','lable','username']
ignore_columns_parameter_list=['Name','BSE_Code','NSE_Code','Industry']

user=st.session_state["username"]
label=st.session_state["lable"]

template='none'
#'plotly_white'   
#hover_data=['Legal Name', 'ISIN','Quartile', 'Rolling_Return', 'Standard Deviation', 'Annual Return', 'SIP Return', 'Sharpe Ratio', 'Alpha', 'Beta', 'Morningstar_Category']
barmode='group'
text_auto='.2s'


_=""" title declaration """

st.title("Mutual Fund Filter")

_=""" Layout designing """

layout_col=st.columns((5,1,5,1))
selct_box_col=st.columns((8,1,8,1))
selct_box_col[1].write('')
selct_box_col[1].write('')
selct_box_col[3].write('')
selct_box_col[3].write('')
select_name_col=st.columns((5,2,5,2))
show_df_col=st.columns((5,5))

#good funds columns
merge_but_col=st.columns((5,1,5))
#name_col=st.columns((2,1,2))
info_col=st.columns((5,1,5))
show_df_col2=st.columns((1))

#filter table
filter_col=st.columns((1))


_="""credentials declaration """


_=""" Connection establishment """
if not st.session_state["sq_cur_obj"]:  
    try: 
        # Creating Connection       
        sq_conn = sqlalchemy_connection()
        st.session_state["sq_connection_obj"]=sq_conn
        st.session_state["sq_cur_obj"]=sq_conn.connect()
        st.experimental_rerun()
    except:
        st.info("Reconnecting.............")
        time.sleep(1)
        st.experimental_rerun()
        st.stop()                                        # Check the SQL cursor object is present is session state or not
else:
    sq_conn=st.session_state["sq_connection_obj"]
    sq_cur=st.session_state["sq_cur_obj"]    



_=""" Default values """
rolling_return_avg_param='Avg'
rolling_return_total_param='Total_1'
rolling_return_total_replace_param='Quartile'


_="""
Function Definations
"""

_=""" Supportive function """

#   1.function to fetch sheet_names
def sheet_names(username,table_name,connection):
    try:
        file_names_q='SELECT sheet_name FROM ' + table_name +' Where username="'+username+'"'
        file_names_df=pd.read_sql_query(file_names_q,connection).drop_duplicates()
        return file_names_df
    except:
        st.error("Something were wrong......")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.warning("Please check connection.....")
            st.experimental_rerun()

#   2.function to fetch filter names
def filter_names(username,table_name,connection):
    try:
        file_names_q='SELECT name FROM ' + table_name +' Where username="'+username+'"'
        file_names_df=pd.read_sql_query(file_names_q,connection).drop_duplicates()
        return file_names_df
    except:
        st.error("Something were wrong......")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.warning("Please check connection.....")
            st.experimental_rerun()

#   3.function to fetch filter
def fetch_filter(table_name,connection,name,username):

    try:
        selected_file_q='SELECT * FROM '+ table_name+' WHERE username="'+username+'" and name="'+name+'"'
        selected_file_df=pd.read_sql(selected_file_q,connection).dropna(axis=1,how='all')
        selected_file_datetime=selected_file_df['date_time'][0]
        return selected_file_df.drop(columns=['username','name','lable','date_time'],axis=1),selected_file_datetime
    except:
        st.error("Server lost.....")
        st.error("Please check connection.....")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.experimental_rerun()


#   4.function to fetch table
#@st.experimental_memo(show_spinner=True)
def fetch_table(table_name,connection,sheet_name=None,username=None):
    if sheet_name and username:
        try:
            selected_file_q='SELECT * FROM '+ table_name+' WHERE username="'+username+'" and sheet_name="'+sheet_name+'"'
            selected_file_df=pd.read_sql(selected_file_q,connection).dropna(axis=1,how='all').dropna(axis=0,how='any')
            selected_file_datetime=selected_file_df['date_time'][0]
            return selected_file_df.drop(columns=['date_time','username','lable','sheet_name'],axis=1),selected_file_datetime
        except:
            st.error("Server lost.....")
            st.error("Please check connection.....")
            if not st.session_state["sq_cur_obj"]:
                st.session_state["sq_cur_obj"]=0   
                time.sleep(2)
                st.experimental_rerun()
    else:
        try:        
            selected_file_q="SELECT * FROM " + table_name
            selected_file_df=pd.read_sql(selected_file_q,connection).dropna(axis=1,how='all')
            lenght_of_selected_file_df=len(selected_file_df)
            return selected_file_df,lenght_of_selected_file_df
        except:
            st.error("Server lost.....")
            st.error("Please check connection.....")
            st.session_state["sq_cur_obj"]=0
            time.sleep(2)
            st.experimental_rerun()

#   5.Function to execute Queries on database with validations
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

# function to export df styler to excel
def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=True, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1 = workbook.add_format({'num_format': '0.00'}) 
    worksheet.set_column('A:A', None, format1)  
    writer.save()
    processed_data = output.getvalue()
    return processed_data

#function for charts style
def charts_style(form,df):
    if form=='Bar':
        df.update_layout(legend=dict(orientation="v"))
        df.update_xaxes(title_text='')
        df.update_yaxes(title_text='')
        df.update_xaxes(title_font=dict(size=3))
        df.update_xaxes(tickfont=dict(size=12))
        df.update_yaxes(tickfont=dict(size=12))
        df.update_layout(coloraxis_showscale=False)
        df.update_xaxes(ticklen=0)
        df.update_yaxes(ticklen=0)
        df.update_layout(plot_bgcolor='rgba(0, 0, 0, 0)',paper_bgcolor='rgba(0,0,0,0)')
        return df
    
#functions for charts
def df_to_chart(form,df,x_axis,y_axis,barmode,template,color,hover_data,orientation,title):
    width=500
    height=300
    text_auto='.2s'
    if form=='Bar':    
        styled_df = px.bar(df, y=y_axis, x=x_axis,barmode=barmode,template=template,text_auto='.2s',color=color,hover_data=hover_data,orientation=orientation,title=title,height=300)
        styled_df_final=charts_style('Bar',styled_df)
        return styled_df_final

    
# function to get first names of funds from thier list of names for charts xaxis data
def first_name(legal_names):
    first_names=[]
    for name in legal_names:
        _words=name.split()
        _first_word=_words[0]
        first_names.append(_first_word)
    first_name_df=pd.DataFrame(data={'First_Name':first_names})
    return first_name_df

# function filter charts
def filter_charts():
    setting_col=st.columns((10,1,1))
    if setting_col[2].button('⚙️',key=1):
        total_col=st.columns((3,1))
        with total_col[1].form("Filter"):
            st.subheader("Filter")
            legal_names=st.multiselect("Select your fund",options=st.session_state['sorted_total_data_df']['Legal Name'].to_list())
            x_axis=st.selectbox('X axis',options=['Legal Name','ISIN','First_Name'])
            y_axis=st.selectbox('Y axis',options=st.session_state['sorted_total_data_df'].columns.to_list())
            if st.form_submit_button("Select"):
                st.experimental_rerun()
        if setting_col[1].button('Hide',key=1):
            st.experimental_rerun()
    else:
        total_col=st.columns((1))

# function to get red flag list
def red_flag_ind(df,parameter,consentration):
    avg=df[parameter].sum()/len(df[parameter])
    if consentration=='High':
        rf_ind_list=df.loc[df[parameter]> avg].index.to_list()
    elif consentration=='Low':
        rf_ind_list=df.loc[df[parameter]< avg].index.to_list()
    else:
        rf_ind_list=[]
    return rf_ind_list


_=""" Working Functions """

# function to extract good funds form combining main sheet with rr sheet
def good_funds(rr_df,main_df):

    _=""" Extract Averages of multiple years from rolling return dataframe """

    # Collect average columns from sheet
    rr_sheet_df=rr_df.copy()
    #with st.expander("Parameters list"):
    parameters_list=rr_sheet_df.columns.to_list()
    avg_list=find_words([rolling_return_avg_param],parameters_list)
    #st.write(avg_list)

    #quartile column extraction
    quartile_list=find_words([rolling_return_total_param],parameters_list)

    rr_avg_quart_df=pd.concat([rr_sheet_df['ISIN'], rr_sheet_df[avg_list], rr_sheet_df[quartile_list]],axis = 1)
    #st._legacy_dataframe(rr_avg_df)




    _=""" Combine the average dataframe from rolling return with main sheet dataframe """

    # concat the main file dataframe with  average dataframe
    main_sheet_df=main_df.copy()   # copying session state dataframe in temporary datframe for analysis

    #pd.concat([df1.set_index('A'),df2.set_index('A')], axis=1, join='inner').reset_index() ....method to concat dataframes with one specific column
    _="""
        The legal names from main files are not similar 
        with the Funds names in Rolling return file,
        so we going to select 'ISIN' as specific column 
        as a index for concatenating the two dataframe with base of 'ISIN'
    """
    
    combined_sheet_df=pd.concat([main_sheet_df.set_index('ISIN'), rr_avg_quart_df.set_index('ISIN')],axis = 1).reset_index().dropna(axis=1,how='all').dropna(axis=0,how='any')
    #with st.expander("Combined Sheet"):
    #    st._legacy_dataframe(combined_sheet_df)

    _=""" Eliminating the bad funds (with no available data for 3 years and 5 years) """

    # Collecting columns with elimination year number: 3,5 yrs
    #with st.expander("Parameters list"):
    #    st.write('Collecting columns with elimination year number: 3,5 yrs')
    parameters_list=combined_sheet_df.columns.to_list()
    #st.write(parameters_list)
    match_sheet_parameter=[]
    years=['3','5']
    for parameter in parameters_list:
        for yr in years:
            if yr in parameter:
                match_sheet_parameter.append(parameter)
    #st.write(match_sheet_parameter)

    # Forwarding the elimination years dataframe for elimination of funds
    # elimination method
    good_funds_df=combined_sheet_df.loc[(combined_sheet_df[match_sheet_parameter]!=0).all(1)].loc[(combined_sheet_df[match_sheet_parameter]!=0).all(1)].reset_index()
    good_funds_df= good_funds_df.replace(0,np.nan).dropna(axis=1,how="all")
    good_funds_df.rename(columns = {'Total_1':rolling_return_total_replace_param}, inplace = True)

    return good_funds_df

# function for filter
#@st.experimental_memo(show_spinner=True)
def mutual_fund_filter(test_df,process_dic):
    with st.spinner("Applying....."):
        time.sleep(2)
        main_df=test_df.copy()
        main_df.columns=[x.replace("_", " ") for x in main_df.columns]  
        total_weightage=main_df[['Legal Name','ISIN']]
        average_dict={}
        
        for each_process in process_dic:

            if each_process in main_df.columns.to_list():

                fund_name_col_name='Legal Name'
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


def mf_apply_filter(total_df,process_dict):
    with st.spinner("Applying filter..."):
        time.sleep(1)
        try:
            st.session_state['total_weightage']=mutual_fund_filter(total_df,process_dict)
            show_total_weightage_table=st.session_state['total_weightage'].copy().set_index([pd.Index(list(range(1, len(st.session_state['total_weightage'].copy())+1))), 'Legal Name'],inplace = True)
            first_name_df=first_name(total_data_df['Legal_Name'].to_list())
            st.session_state['total_data_df']=pd.concat([st.session_state['total_weightage'],first_name_df,total_data_df.drop(['Legal_Name','ISIN','index'],axis=1)],axis=1)
            st.session_state['sorted_total_weightage_df']=st.session_state['total_weightage'].sort_values(by='Result',ascending=False)
            st.session_state['sorted_total_data_df']=st.session_state['total_data_df'].sort_values(by='Result', ascending=False)
            st.success("Filter applied successfully")
            time.sleep(1)
        except:
            st.error("Error to apply filter..")
            st.warning("Please apply filter again!..")
            st.stop()            



def mf_dashboard_data(sorted_total_df):
    template='plotly_white'   
    hover_data=['Legal Name', 'ISIN','Quartile', 'Rolling_Return', 'Standard Deviation', 'Annual Return', 'SIP Return', 'Sharpe Ratio', 'Alpha', 'Beta', 'Morningstar_Category']
    barmode='group'
    text_auto='.2s'
    with st.spinner("Preparing Dashboard......"):
        try:
            st.session_state['total_data_df_chart']=df_to_chart(form='Bar',df=sorted_total_df,title='Funds Weightage',x_axis='First_Name',y_axis='Result',barmode='group',template=template,hover_data=hover_data,color='Result',orientation='v')
            st.session_state['total_assets_chart']=df_to_chart(form='Bar',df=sorted_total_df.head(5).sort_values(by='Total_Assets_?MM',ascending=True),title='Total Assets',x_axis='Total_Assets_?MM',y_axis='First_Name',template=template,hover_data=['Legal Name', 'ISIN','Quartile','Total_Assets_?MM','Total Assets ?MM_w'],barmode='group',color='Total_Assets_?MM',orientation='h')
            st.session_state['Assets_Holdings_chart']=df_to_chart(form='Bar',df=sorted_total_df.head(5).sort_values(by='%_Assets_in_Top_10_Holdings',ascending=False),title='Assets Holdings',x_axis='%_Assets_in_Top_10_Holdings',y_axis='First_Name',template=template,hover_data=['Legal Name', 'ISIN','Quartile','%_Assets_in_Top_10_Holdings','% Assets in Top 10 Holdings_w'],barmode='group',color='Total_Assets_?MM',orientation='h')
            st.success("Dashboard generated...")
            time.sleep(1)
            return 1
        except:
            st.error("Error to generated dashboard")
            st.warning("Please apply filter again!..")
            time.sleep(1)
            return 0

def mf_report_data(sorted_total_df,sorted_weightage_df):
    with st.spinner("Preparing Report....."):
        show_df_TAMM =sorted_total_df[['ISIN','Legal Name','Total_Assets_?MM']]
        show_df_AITH=sorted_total_df[['ISIN','Legal Name','%_Assets_in_Top_10_Holdings']]
        try:
            st.session_state['analysis_report']=sorted_weightage_df
            ta_rf_l=red_flag_ind(show_df_TAMM.head(5),parameter='Total_Assets_?MM',consentration='Low')
            st.session_state['rf_TAMM_df']=show_df_TAMM.drop('ISIN',axis=1).style.apply(lambda x: ['background: lightgreen' if x.name in ta_rf_l else '' for i in x], axis=1)
            ta_rf_l2=red_flag_ind(show_df_AITH.head(5),parameter='%_Assets_in_Top_10_Holdings',consentration='High')
            st.session_state['rf_AITH_df']=show_df_AITH.drop("ISIN",axis=1).style.apply(lambda x: ['background: lightblue' if x.name in ta_rf_l2 else '' for i in x], axis=1)
            st.success("Report Generated...")
            time.sleep(1)
        except:
            st.error("Error to generated report")
            st.warning("Please apply filter again!..")
            time.sleep(1)



_="""

Sessionstate variables

"""

# Main file name & dataframe
#   1.main files names
if 'main_file_names' not in st.session_state:
    st.session_state['main_file_names']=sheet_names(username=username,table_name=mf_sheet_table,connection=sq_conn)

#   2.selected main file name
if 'selected_main_file_name' not in st.session_state:
    st.session_state['selected_main_file_name']=st.session_state['main_file_names'].iloc[0]['sheet_name']

#   3.selected main file df
if 'selected_main_file_df' and 'selected_main_file_date' not in st.session_state:
    st.session_state['selected_main_file_df'],st.session_state['selected_main_file_date']=fetch_table(mf_sheet_table,sheet_name=st.session_state['selected_main_file_name'],connection=sq_conn,username=username)

# Rolling return name & dataframe
# fetch rr file names
#   4.rr files names
if 'rr_file_names' not in st.session_state:
    st.session_state['rr_file_names']=sheet_names(username=username,table_name= mf_rolling_return_table,connection= sq_conn)

if 'selected_rr_file_name' not in st.session_state:
    st.session_state['selected_rr_file_name']=st.session_state['rr_file_names'].iloc[0]['sheet_name']

if 'selected_rr_file_df' and 'selected_rr_file_date' not in st.session_state:
    st.session_state['selected_rr_file_df'],st.session_state['selected_rr_file_date']=fetch_table(mf_rolling_return_table,sheet_name=st.session_state['selected_rr_file_name'],connection=sq_conn,username=username)

# selected file names
if "select_main_file" not in st.session_state:
    st.session_state['select_main_file']=''

if "select_rr_file" not in st.session_state:
    st.session_state['select_rr_file']=''

# Good funds dataframe
if 'good_funds_df' not in st.session_state:
    st.session_state['good_funds_df']=good_funds(rr_df=st.session_state['selected_rr_file_df'],main_df=st.session_state['selected_main_file_df'])

#sourcefiles
if 'source_file1' not in st.session_state:
    st.session_state['source_file1']=st.session_state['selected_main_file_name']

if 'source_file2' not in st.session_state:
    st.session_state['source_file2']= st.session_state['selected_rr_file_name']

# filter table
if 'mf_filter_names' not in st.session_state:
    st.session_state['mf_filter_names']=filter_names(username=username,table_name=mf_filter_table,connection=sq_conn)

if 'selected_mf_filter_name' not in st.session_state:
    st.session_state['selected_mf_filter_name']=st.session_state['mf_filter_names'].iloc[0]['name']

if 'selected_mf_filter_df' and 'selected_mf_filter_date' not in st.session_state:
    st.session_state['selected_mf_filter_df'],st.session_state['selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['selected_mf_filter_name'])

# total weightage table
if 'total_weightage_table' not in st.session_state:
    st.session_state['total_weightage_table']=0

if 'sorted_total_weightage_df' not in st.session_state:
    st.session_state['sorted_total_weightage_df']=0

if 'show_total_weightage_table' not in st.session_state:
    st.session_state['show_total_weightage_table']=0

# Final total table
if 'total_data_df' not in st.session_state:
    st.session_state['total_data_df']=pd.DataFrame()

if 'sorted_total_data_df' not in st.session_state:
    st.session_state['sorted_total_data_df']=0

if 'dash_generated' not in st.session_state:
    st.session_state['dash_generated']=0

if 'total_data_df_chart' and 'total_assets_chart' and 'Assets_Holdings_chart' not in st.session_state:
    st.session_state['total_data_df_chart'],st.session_state['total_assets_chart'],st.session_state['Assets_Holdings_chart']=0,0,0

if 'analysis_report'and 'rf_TAMM_df' and 'rf_AITH_df' not in st.session_state:
    st.session_state['analysis_report'],st.session_state['rf_TAMM_df'],st.session_state['rf_AITH_df']=0,0,0


_="""  

Session state Buttons

"""
#Filter apply button
if "Apply" not in st.session_state:
    st.session_state['Apply']=False
def Apply_callback():
    st.session_state['Apply']=True
#if (del_col[2].button("Delete",on_click=del_callback2) or st.session_state.delete_clicked2):


_=""" Connection establishment """





_="""1.Fetch Files:Get tables from the database table """

_=""" table of main file """
#subheader for main files
layout_col[0].header("Main Files:")
#Select box for main files selection
selected_main_file_name=selct_box_col[0].selectbox("",options=st.session_state['main_file_names'],key='main_files_names')
# buttons
if selct_box_col[1].button("🔍",key=1):
    st.session_state['selected_main_file_name']=selected_main_file_name
    st.session_state['selected_main_file_df'],st.session_state['selected_main_file_date']=fetch_table(mf_sheet_table,sheet_name=st.session_state['selected_main_file_name'],_connection=sq_conn)
    st.experimental_rerun()
# show table
if len(st.session_state['selected_main_file_df']):
    select_name_col[0].subheader(st.session_state['selected_main_file_name'])
    select_name_col[1].write('Results found: {}'.format(len(st.session_state['selected_main_file_df'])))
    with show_df_col[0].expander("Show my sheet"):
        st.write('Created at: '+str(st.session_state['selected_main_file_date']))
        st._legacy_dataframe(st.session_state['selected_main_file_df'])
else:
    st.info("Empty.....")



_=""" table of rolling return file """
layout_col[2].header("Rolling Return Files:")
# selectbox
selected_rr_file_name=selct_box_col[2].selectbox("",options=st.session_state['rr_file_names'],key=2)
# button
if selct_box_col[3].button("🔍",key=2):
    st.session_state['selected_rr_file_name']=selected_rr_file_name
    st.session_state['selected_rr_file_df'],st.session_state['selected_rr_file_date']=fetch_table(mf_rolling_return_table,sheet_name=st.session_state['selected_rr_file_name'],connection=sq_conn,username=username)
    st.experimental_rerun()
# show table
if len(st.session_state['selected_rr_file_df']):
    select_name_col[2].subheader(st.session_state['selected_rr_file_name'])
    select_name_col[3].write('Results found: {}'.format(len(st.session_state['selected_rr_file_df'])))
    with show_df_col[1].expander("Show my sheet"):
        st.write("Created at:{}".format(st.session_state['selected_rr_file_date']))
        st._legacy_dataframe(st.session_state['selected_rr_file_df'])
else:
    st.info("Empty.....")


_="""2.Merger: goodfunds function"""
# 1.button
if merge_but_col[1].button("Merge"):
    st.session_state['source_file1']=st.session_state['main_file_name']
    st.session_state['source_file2']=st.session_state['rr_file_name']
    # Merging of sheets
    st.session_state['good_funds_df']=good_funds(rr_df=st.session_state['rolling_return_file_df'],main_df=st.session_state['main_file_df'])
    st.experimental_rerun()

# 2.title and info
info_col[0].header("Good funds")
info_col[2].info("_Source Files_:**_{}_**_|_**_{}_**".format(st.session_state['source_file1'],st.session_state['source_file2']))
info_col[2].markdown("_Eliminated bad funds( funds with no data available for 3,5 yrs)_")
info_col[0].write('')
info_col[0].write("Results:{}".format(len(st.session_state['good_funds_df'])))

# 3.show df
if len(st.session_state['good_funds_df']):
    with show_df_col2[0].expander("Show my sheet",expanded=True):
        st._legacy_dataframe(st.session_state['good_funds_df'])
else:
    st.warning("Sorry wrong merge....")
    st.error("Please check your files:**_{}_**_|_**_{}_**".format(st.session_state['source_file1'],st.session_state['source_file2']))

# 4.Create Copy of good funds data
total_data_df=st.session_state['good_funds_df'].copy()
#st.write('good funds added to total_data_df')
#st._legacy_dataframe(total_data_df)

_=""" Filter """
# Copying Filter
filter_df=st.session_state['selected_mf_filter_df'].copy()

# FIlter creation
#with st.expander("Show Filter"):
filter_tab,create_tab,add_tab,update_tab=st.tabs(["Filter","Create","Add","Update"])
with filter_tab:
    select_filter_col=st.columns((8,8,1))
    select_filter_col[2].write("")
    select_filter_col[2].write("")

    select_filter_col[0].header("My Filter")

    ft_selected_filter_name=select_filter_col[1].selectbox("",options=st.session_state['mf_filter_names'])
    if select_filter_col[2].button("🔍",key='select_filter_col'):
        st.session_state['selected_mf_filter_name']=ft_selected_filter_name
        st.session_state['selected_mf_filter_df'],st.session_state['selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['selected_mf_filter_name'])
        st.experimental_rerun()
        
    # show filter
    if len(st.session_state['selected_mf_filter_df']):
        select_filter_col[0].subheader(st.session_state['selected_mf_filter_name'])
        select_filter_col[1].write('Created at:'+str(st.session_state['selected_mf_filter_date']))
        with st.expander("Show filter"):
            st._legacy_dataframe(st.session_state['selected_mf_filter_df'],height=1000)
    else:
        st.info('Empty...')


with create_tab:

    if 'ct_df_filter' not in st.session_state:
        st.session_state['ct_df_filter'] = pd.DataFrame({'parameter': [], 'condition_1': [],
                        'condition_2': [], 'weightage_1': [], 'sort': [], 'weightage_2': []})

    st.header("Create Filter")

    ct_name=st.text_input("Filter Name",key='create_tab')

    #update filter interface
    u_parameter=st.text_input("Enter Parameter",key='create_tab')
    ct_col=st.columns((1,1,1,1,1))
    u_condition_1=ct_col[0].selectbox("Select 1st Condition",options=['Average','-'],key='create_tab')
    u_condition_2=ct_col[1].selectbox("Select 2nd Condition",options=['Above Average','Below Average','Gold'],key='create_tab')
    u_weightage_1=ct_col[2].number_input("1st Weighatge",key='create_tab')
    u_sort=ct_col[3].selectbox("Select sort Condition",options=['Top 5','Bottom 5','Others'],key='create_tab')
    u_weightage_2=ct_col[4].number_input("2nd Weightage",key='create_tab')

    if ct_col[2].button('Add Parameter',key='create_tab'):
        if u_parameter in st.session_state['ct_df_filter']['parameter'].to_list():
            st.warning("paramter already added")
            if len(st.session_state['ct_df_filter']):
                st.write("New Parameters:")
                st.table(st.session_state['ct_df_filter'])
            
        else:
            selection = {'parameter':u_parameter, 'condition_1': u_condition_1,
                                'condition_2': u_condition_2, 'weightage_1': u_weightage_1, 'sort': u_sort, 'weightage_2': u_weightage_2}

            st.session_state['ct_df_filter'] = st.session_state['ct_df_filter'].append(selection, ignore_index=True)
            st.write("New Parameters:")
            st.table(st.session_state['ct_df_filter'])
            st.experimental_rerun()

    else:
        if len(st.session_state['ct_df_filter']):
            st.write("New Parameters:")
            st.table(st.session_state['ct_df_filter'])

    sc_but_col=st.columns((2,17,2))
    if len(st.session_state['ct_df_filter']):
        if sc_but_col[0].button("Clear"):
            st.session_state['ct_df_filter'] = st.session_state['ct_df_filter'].drop(len(st.session_state['ct_df_filter']) - 1)
            st.experimental_rerun()

        if sc_but_col[2].button("Save"):  
            if ct_name=='':
                st.error("Please,give a name to this filter!")
                st.stop()
            if ct_name in st.session_state['mf_filter_names']['name'].to_list():
                st.error("Sorry!,filter name already used..")
                st.warning("Please,give different name!")

            else:
                with st.spinner('Adding new filter...'):
                    time.sleep(1)
                    st.session_state['ct_df_filter'].insert(0, "date_time", str(current_date))
                    st.session_state['ct_df_filter'].insert(1, "lable", st.session_state["lable"])
                    st.session_state['ct_df_filter'].insert(2, "username", st.session_state["username"])
                    st.session_state['ct_df_filter'].insert(3, "name", ct_name)

                    try:
                        st.session_state['ct_df_filter'].to_sql(mf_filter_table, sq_conn, if_exists='append', index=False)
                        st.success("Added...")
                        time.sleep(1)
                        st.session_state['ct_df_filter'] = st.session_state['ct_df_filter'].drop(x for x in range(len(st.session_state['ct_df_filter'])))
                        st.session_state['ct_df_filter'].drop(st.session_state['ct_df_filter'].columns[[0,1,2,3]], axis=1, inplace=True)
                    except:
                        st.error("Error to add this filter...")
                        st.warning("please try again....")
                        time.sleep(2)
                        st.experimental_rerun()
                    st.session_state['mf_filter_names']=filter_names(username=username,table_name=mf_filter_table,connection=sq_conn)
                    st.experimental_rerun()


with add_tab:
        
        if 'at_df_filter' not in st.session_state:
            st.session_state['at_df_filter'] = pd.DataFrame({'parameter': [], 'condition_1': [],
                                'condition_2': [], 'weightage_1': [], 'sort': [], 'weightage_2': []})

        if 'at_selected_mf_filter_name' not in st.session_state:
            st.session_state['at_selected_mf_filter_name']=st.session_state['mf_filter_names'].iloc[0]['name']
        
        if 'at_selected_mf_filter_df' and 'at_selected_mf_filter_date' not in st.session_state:
            st.session_state['at_selected_mf_filter_df'],st.session_state['at_selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['at_selected_mf_filter_name'])

        at_select_filter_col=st.columns((8,8,1))
        at_select_filter_col[2].write("")
        at_select_filter_col[2].write("")

        at_select_filter_col[0].header("Add Parameter")

        at_selected_filter_name=at_select_filter_col[1].selectbox("",options=st.session_state['mf_filter_names'],key='add_tab')
        if at_select_filter_col[2].button("🔍",key='at_select_filter_col'):
            st.session_state['at_selected_mf_filter_name']=at_selected_filter_name
            st.session_state['at_selected_mf_filter_df'],st.session_state['at_selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['at_selected_mf_filter_name'])
            st.experimental_rerun()


        #update filter interface
        u_parameter=st.text_input("Enter Parameter")
        ut_col=st.columns((1,1,1,1,1))
        u_condition_1=ut_col[0].selectbox("Select 1st Condition",options=['Average','-'])
        u_condition_2=ut_col[1].selectbox("Select 2nd Condition",options=['Above Average','Below Average','Gold'])
        u_weightage_1=ut_col[2].number_input("1st Weighatge")
        u_sort=ut_col[3].selectbox("Select sort Condition",options=['Top 5','Bottom 5','Others'])
        u_weightage_2=ut_col[4].number_input("2nd Weightage")

        if ut_col[2].button('Add Parameter'):
            selection = {'parameter':u_parameter, 'condition_1': u_condition_1,
                                'condition_2': u_condition_2, 'weightage_1': u_weightage_1, 'sort': u_sort, 'weightage_2': u_weightage_2}

            st.session_state['at_df_filter'] = st.session_state['at_df_filter'].append(selection, ignore_index=True)
            st.write("New Parameters:")
            st.table(st.session_state['at_df_filter'])
            st.experimental_rerun()

        else:
            if len(st.session_state['at_df_filter']):
                st.write("New Parameters:")
                st.table(st.session_state['at_df_filter'])
        sc_but_col=st.columns((2,17,2))
        if len(st.session_state['at_df_filter']):
                if sc_but_col[0].button("Clear"):
                    st.session_state['at_df_filter'] = st.session_state['at_df_filter'].drop(len(st.session_state['at_df_filter']) - 1)
                    st.experimental_rerun()

                if sc_but_col[2].button("Save"):  

                    with st.spinner('Adding new paramteres...'):
                        time.sleep(1)
                        st.session_state['at_df_filter'].insert(0, "date_time", st.session_state['at_selected_mf_filter_date'])
                        st.session_state['at_df_filter'].insert(1, "lable", st.session_state["lable"])
                        st.session_state['at_df_filter'].insert(2, "username", st.session_state["username"])
                        st.session_state['at_df_filter'].insert(3, "name", st.session_state['at_selected_mf_filter_name'])

                        try:
                            st.session_state['at_df_filter'].to_sql(mf_filter_table, sq_conn, if_exists='append', index=False)
                            st.success("Added...")
                            time.sleep(1)
                            st.session_state['at_df_filter'] = st.session_state['at_df_filter'].drop(x for x in range(len(st.session_state['at_df_filter'])))
                            st.session_state['at_df_filter'].drop(st.session_state['at_df_filter'].columns[[0,1,2,3]], axis=1, inplace=True)
                            st.session_state['at_selected_mf_filter_df'],st.session_state['at_selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['at_selected_mf_filter_name'])

                        except:
                            st.error("Error to save this filter...")
                            st.warning("please try again....")
                            time.sleep(2)
                            st.experimental_rerun()
                        st.experimental_rerun()

        # show filter
        if len(st.session_state['at_selected_mf_filter_df']):
            st.subheader(st.session_state['at_selected_mf_filter_name'])
            st.write('Created at:'+str(st.session_state['at_selected_mf_filter_date']))
            with st.expander("Show filter",expanded=True):
                st._legacy_dataframe(st.session_state['at_selected_mf_filter_df'],height=1000)
        else:
            st.info('Empty...')


with update_tab:
        
        if 'ut_selected_mf_filter_name' not in st.session_state:
            st.session_state['ut_selected_mf_filter_name']=st.session_state['mf_filter_names'].iloc[0]['name']
        
        if 'ut_selected_mf_filter_df' and 'ut_selected_mf_filter_date' not in st.session_state:
            st.session_state['ut_selected_mf_filter_df'],st.session_state['ut_selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['ut_selected_mf_filter_name'])

        ut_select_filter_col=st.columns((8,8,1))
        ut_select_filter_col[2].write("")
        ut_select_filter_col[2].write("")

        ut_select_filter_col[0].header("Update Paramter")

        ut_selected_filter_name=ut_select_filter_col[1].selectbox("",options=st.session_state['mf_filter_names'],key='update_tab')
        if ut_select_filter_col[2].button("🔍",key='ut_select_filter_col'):
            st.session_state['ut_selected_mf_filter_name']=ut_selected_filter_name
            st.session_state['ut_selected_mf_filter_df'],st.session_state['ut_selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['ut_selected_mf_filter_name'])
            st.experimental_rerun()            

            
        ut_filter_test_df=st.session_state['ut_selected_mf_filter_df'].copy()

        Options=st.session_state['ut_selected_mf_filter_df']['parameter']
        o_condition_1=['Average','-']
        o_condition_2=['Above Average','Below Average','Gold']
        o_sort=['Top 5','Bottom 5','Others']
        conditions=['>', '<', '=']
        results=[-1, 1]

        new_col=st.columns((15,8,5))
        ut_mf_select_param2 = st.selectbox("Select parameter:",options=Options,key="ut_mf_select_param2")

        # to get index value of the parameter
        ind=np.where(ut_filter_test_df['parameter'] == ut_mf_select_param2)
        #st.write(ind[0][0])

        # to get values of columns of selected parameter
        condition_1=ut_filter_test_df.at[ind[0][0], 'condition_1']
        condition_2=ut_filter_test_df.at[ind[0][0], 'condition_2']
        weightage_1=ut_filter_test_df.at[ind[0][0], 'weightage_1']
        sort=ut_filter_test_df.at[ind[0][0], 'sort']
        weightage_2=ut_filter_test_df.at[ind[0][0], 'weightage_2']

        # defaults generation for selection
        if condition_1==o_condition_1[0]:
            cond_id_1=0
        elif condition_1==o_condition_1[1]:
            cond_id_1=1
        

        if condition_2==o_condition_2[0]:
            cond_id_2=0
        elif condition_2==o_condition_2[1]:
            cond_id_2=1
        else :
            cond_id_2=2
    

        if sort==o_sort[0]:
            sort_id=0 
        elif sort==o_sort[1]:
            sort_id=1
        else:
            sort_id=2                   


        ut_col=st.columns((1,1,1,1,1))
        u_condition_1=ut_col[0].selectbox("Select 1st Condition",options=o_condition_1,key=1,index=cond_id_1)
        u_condition_2=ut_col[1].selectbox("Select 2nd Condition",options=o_condition_2,key=2,index=cond_id_2)
        u_weightage_1=ut_col[2].number_input("1st Weighatge",value=weightage_1,key=2)
        u_sort=ut_col[3].selectbox("Select sort Condition",options=o_sort,key=3,index=sort_id)
        u_weightage_2=ut_col[4].number_input("2nd Weightage",value=weightage_2,key=2)

    
        if 'ut_temp_df3' not in st.session_state:
            st.session_state.ut_temp_df3=pd.DataFrame({'parameter': [], 'condition_1': [],
                    'condition_2': [], 'weightage_1': [], 'sort': [], 'weightage_2': []})

        updf_bcol=st.columns((1,10,1))

        ut_filter_test_df.at[ind[0][0], 'condition_1'] = u_condition_1
        ut_filter_test_df.at[ind[0][0], 'condition_2'] = u_condition_2
        ut_filter_test_df.at[ind[0][0], 'weightage_1'] = u_weightage_1
        ut_filter_test_df.at[ind[0][0], 'sort'] = u_sort
        ut_filter_test_df.at[ind[0][0], 'weightage_2'] = u_weightage_2
                    
        
        if updf_bcol[0].button("Update"):
            selection = {'parameter':u_parameter, 'condition_1': u_condition_1,
                    'condition_2': u_condition_2, 'weightage_1': u_weightage_1, 'sort': u_sort, 'weightage_2': u_weightage_2}

            st.session_state.ut_temp_df3=st.session_state.ut_temp_df3.append(selection, ignore_index=True)
            ut_filter_test_df.index = np.arange(1, len(ut_filter_test_df) + 1)
           
            st.session_state.ut_temp_df3.index = np.arange(1, len(st.session_state.ut_temp_df3) + 1)

            for i in range(len(st.session_state.ut_temp_df3)):
                #st.write(st.session_state.temp_df.iloc[[i]])
                upd_df=st.session_state.ut_temp_df3.iloc[[i]]

                condition__1=str(upd_df['condition_1'][i+1])
                condition__2=str(upd_df['condition_2'][i+1])
                weightage__1=float(upd_df['weightage_1'][i+1])
                weightage__2=float(upd_df['weightage_2'][i+1])
                sort__=str(upd_df['sort'][i+1])
                
                update_q="UPDATE "+mf_filter_table+" SET condition_1 = '"+str(condition__1)+\
                    "', condition_2 = '"+str(condition__2)+"', weightage_1 = '"+str(weightage__1)+\
                    "', weightage_2 = '"+str(weightage__2)+"', sort = '"+str(sort__)+"' WHERE parameter='"+ut_mf_select_param2+"'"
                
                #'fmlxgnx,;v.b x[lmkn kpZ<G
                sq_cur.execute(update_q)
                st.success("Filter Updated")
                st.session_state.ut_temp_df3.drop(st.session_state.ut_temp_df3.index, inplace=True)
                time.sleep(1)
                st.session_state['ut_selected_mf_filter_df'],st.session_state['ut_selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['ut_selected_mf_filter_name'])
                st.experimental_rerun()

        if updf_bcol[2].button("Delete"):
        #  Delete query:DELETE FROM master_filter WHERE date_time = "2022-09-05 06:46:14" and `user`="chetan" and `name`="new" and `parameter_name`="EPS"
            mf_ut_del_row = "DELETE FROM " + mf_filter_table +" WHERE date_time='"+str(st.session_state['ut_selected_mf_filter_date'])+"' and username='"+st.session_state["username"]+"' and name='"+st.session_state['ut_selected_mf_filter_name']+"' and parameter_name='"+ut_mf_select_param2+"'"
            with st.spinner("Deleting...."):
                time.sleep(1)
                try:
                    sq_cur.execute(text(mf_ut_del_row))
                    st.success("Deleted...")
                    time.sleep(1)
                    #st.experimental_rerun()
                except:
                    st.error("Error to delete...")
                    st.warning("please try again....")
                    time.sleep(1)
                st.session_state['ut_selected_mf_filter_df'],st.session_state['ut_selected_mf_filter_date']=fetch_filter(mf_filter_table,connection=sq_conn,username=username,name=st.session_state['ut_selected_mf_filter_name'])
                st.experimental_rerun()
        # show filter
        if len(st.session_state['ut_selected_mf_filter_df']):
            st.subheader(st.session_state['ut_selected_mf_filter_name'])
            st.write('Created at:'+str(st.session_state['ut_selected_mf_filter_date']))
            with st.expander("Show filter",expanded=True):
                st._legacy_dataframe(st.session_state['ut_selected_mf_filter_df'],height=1000)
        else:
            st.info('Empty...')
    

_=""" Filter process """
        #1) Add average parameters into one column of rolling returns
        #2) Average out the multiple years of same paramaters in good funds df in one single parameter coulumn name taken from filter
        #3) prepare filter process
        #4) apply filter
     
_=""" 1)Adding the Average parameters with mulitple years data and combining in one Rolling return Column"""

# 1.1) Find average parameters columns
avg_columns_list=find_words([rolling_return_avg_param],total_data_df.columns.to_list())

# 1.2) Combining Average columns data into one single column of Rolling returns and adding it in googd funds table
total_data_df['Rolling_Return'] = total_data_df[avg_columns_list].sum(axis=1)/len(avg_columns_list)
#st._legacy_dataframe(total_data_df)
_=""" 2) Average out the multiple years of same paramaters in good funds df in one single parameter coulumn name taken from filter"""

# 2.1) find the filter parameters matching with main combined sheet
matched_parameter_dict=paramter_map(total_data_df,filter_df['parameter'])

# 2.2) for each parameters multiple data take average with result in single column with name matching in filter table
for each_param in matched_parameter_dict:
    if len(matched_parameter_dict[each_param])>1:
        total_data_df[each_param] = total_data_df[matched_parameter_dict[each_param]].sum(axis=1)/len(avg_columns_list)
#st.write('each multi yr data into single column added to total_data_df')
#st._legacy_dataframe(total_data_df)

_=""" 3) prepare filter process"""
# 3.1) converting filter table into one dictionary of process
process_dic=filter_df.set_index('parameter').to_dict(orient='index')

_=""" 4) Apply filter """
# 4.1) Apply filter
# layout
with filter_tab:
    apply_col=st.columns((5,2,5))
    apply_cheak=apply_col[1].button("Apply",key='mf_filter_apply')

    #apply_only_cheak=apply_col[2].checkbox("Apply Only")
    if apply_cheak:
        try:
            mf_apply_filter(total_df=total_data_df,process_dict=process_dic)
            st.session_state['dash_generated']=mf_dashboard_data(sorted_total_df=st.session_state['sorted_total_data_df'])
            mf_report_data(sorted_total_df=st.session_state['sorted_total_data_df'],sorted_weightage_df=st.session_state['sorted_total_weightage_df'])   
        except:
            st.warning("Sorry something were wrong!!!")  
            st.error("Plese check your files and try again..!!")
            st.stop()
        time.sleep(1)  
        st.experimental_rerun()


with filter_tab:
    _=""" Filter Output """
    #len(st.session_state['total_weightage']
    if len(st.session_state['total_data_df']):

        # layout tabs: 1) Dashboard for charts 2) report for download
        dashboard_tab,report_tab=st.tabs(["Dashboard",'Report'])

        

        with dashboard_tab:
            if st.session_state['dash_generated']:
                title_col=st.columns((4,5))
                title_col[0].header("Dashboard")
                title_col[1].info("_Source Files_:**_{}_**_|_**_{}_**".format(st.session_state['source_file1'],st.session_state['source_file2']))

                # Create required data for Output layout: Dashboard and report

                _=""" 1) Top list"""
                Funds_Weightage_info_col=st.columns((4,10,4))
                # 1.Sorted total data df
                
                Funds_Weightage_col=st.columns((1))
                #total_data_df_chart=df_to_chart(form='Bar',df=st.session_state['sorted_total_data_df'],title='Funds Weightage',x_axis='First_Name',y_axis='Result',barmode='group',template=template,hover_data=hover_data,color='Result',orientation='v')
                Funds_Weightage_col[0].plotly_chart(st.session_state['total_data_df_chart'], use_container_width=True)

                red_flag_charts=st.columns((1,1))
                #2.Total assets red flag chart
                #total_assets=df_to_chart(form='Bar',df=st.session_state['sorted_total_data_df'].head(5).sort_values(by='Total_Assets_?MM',ascending=True),title='Total Assets',x_axis='Total_Assets_?MM',y_axis='First_Name',template=template,hover_data=['Legal Name', 'ISIN','Quartile','Total_Assets_?MM','Total Assets ?MM_w'],barmode='group',color='Total_Assets_?MM',orientation='h')
                red_flag_charts[0].plotly_chart(st.session_state['total_assets_chart'], use_container_width=True)

                #3.% Assets in Top 10 Holdings red flag chart
                #Assets_Holdings=df_to_chart(form='Bar',df=st.session_state['sorted_total_data_df'].head(5).sort_values(by='%_Assets_in_Top_10_Holdings',ascending=False),title='Assets Holdings',x_axis='%_Assets_in_Top_10_Holdings',y_axis='First_Name',template=template,hover_data=['Legal Name', 'ISIN','Quartile','%_Assets_in_Top_10_Holdings','% Assets in Top 10 Holdings_w'],barmode='group',color='Total_Assets_?MM',orientation='h')
                red_flag_charts[1].plotly_chart(st.session_state['Assets_Holdings_chart'], use_container_width=True)
            else:
                st.error("Sorry,error to generate dashboard!!!")            

                
                
        with report_tab:
            title_col=st.columns((4,5))
            title_col[0].header("Report")
            title_col[1].info("_Source Files_:**_{}_**_|_**_{}_**".format(st.session_state['source_file1'],st.session_state['source_file2']))

            #export button
            exp_but_col=st.columns((5,5,2))
            
            #total_weightage_styled=total_sorted.style.apply(lambda x: ['background: lightgreen' if x.name in hi_list else '' for i in x], axis=1).apply(lambda x: ['background: lightblue' if x.name in hi_list2 else '' for i in x], axis=1)
            
            wt_col=st.columns((5,3,5))
            wt_col[1].subheader("Weightage Table")
            st._legacy_dataframe(st.session_state['analysis_report'])
            exp_but_col[2].download_button("📥Export",key=2, data=to_excel(st.session_state['analysis_report']), file_name='weightage_'+st.session_state['source_file1']+'.xlsx')

            rf_col=st.columns((1,1))

            with rf_col[0]:
                st.subheader("Total Assets ?MM")
                st._legacy_dataframe(st.session_state['rf_TAMM_df'])
                st.download_button("📥Export",key=3, data=to_excel(st.session_state['rf_TAMM_df']), file_name='rf_TA_'+st.session_state['source_file1']+'.xlsx')

            
            with rf_col[1]:
                st.subheader("% Assets in Top 10 Holdings")
                st._legacy_dataframe(st.session_state['rf_AITH_df'])
                st.download_button("📥Export",key=4, data=to_excel(st.session_state['rf_AITH_df']), file_name='rf_AITH_'+st.session_state['source_file1']+'.xlsx')

        
