import pandas as pd 
import numpy as np     
import matplotlib.pyplot as plt                                         # to read the uploaded csv
import streamlit as st
import plotly.express as px
import time
from datetime import datetime
from sqlalchemy import create_engine,text
from Home import filter_table,master_table,sentiment_table
from Home import sqlalchemy_connection,refresh_dashboard
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
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
st.title("🤵 Equity Filter 🧮")

_=""" main Layout designing """
mf_col1=st.columns((5,4,1))



_=""" Layout designing """



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




_=""" 

    Functions definition

"""

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

def pagination(df,key_name,index_param):
    
    _="""
        ## 📑 Pagination
        
        Too much data to display? Now you can paginate through items (e.g. a table), 
        storing the current page number in `st.session_state`. 
    """
    
    if "page"+key_name not in st.session_state:
        st.session_state["page"+key_name]  = 0

    def next_page():
        st.session_state["page"+key_name]  += 1

    def prev_page():
        st.session_state["page"+key_name] -= 1


    col0,col1, col2, col3, _ = st.columns((0.7,0.1, 0.17, 0.1, 0.63))

    max_page=len(df)/10


    if st.session_state["page"+key_name] < max_page-1:
        col3.button(">", on_click=next_page,key=key_name+'_1')
    else:
        col3.write("")  # this makes the empty column show up on mobile

    if st.session_state["page"+key_name] > 0:
        col1.button("<", on_click=prev_page,key=key_name+'_1')
    else:
        col1.write("")  # this makes the empty column show up on mobile

    #col2.write(f"Page {1+st.session_state.page} of {max_page+0.1}")
    col2.write("Page {}".format(1 + st.session_state["page"+key_name]))
    start = 10 * st.session_state["page"+key_name]
    end = start + 10
    x=list(range(1, len(df)+1))
    df.set_index([pd.Index(x), index_param],inplace = True)
    df3=df.iloc[start:end]

    return df3

def color_negative_red(value):
  """
  Colors elements in a dateframe
  green if positive and red if
  negative. Does not color NaN
  values.
  """

  if value < 0:
    color = 'color: red;'
  elif value > 0:
    color = 'color: green;'
  else:
    color = 'black'

  return 'color: %s' % color
  #return color

def cell_color(value):
  """
  Colors elements in a dateframe
  green if positive and red if
  negative. Does not color NaN
  values.
  """

  if value == 1:
    color = 'background-color: #6bf556;'
  elif value ==-1:
    color = 'background-color: #f75631;'
  else:
    color = 'background-color: blue;'

  #return 'color: %s' % color
  return color

def cell_color2(value):
  """
  Colors elements in a dateframe
  green if positive and red if
  negative. Does not color NaN
  values.
  """
#background-color: #6bf556;
  if value == 'Positive':
    color = 'background-color: green;'
  elif value =='Negative':
    color = 'background-color: red;'
  else:
    color = 'background-color: blue;'

  #return 'color: %s' % color
  return color
# to remove and add underscores
def under_scr_col_list(df):
        if str(type(df))== "<class 'pandas.core.frame.DataFrame'>":
            columns = []
            for i in df:
                print(i)
                columns.append(i)
            under_scored = [x.replace(" ", "_").replace("?", "").replace("-", "_").replace("/", "_")
                            .replace("\\", "_").replace("%", "").replace(")", "").replace("(", "").replace("$", "") for x in
                            df]        #returns list of underscored columns
            return under_scored
        elif type(df)==list:
            under_scored = [x.replace(" ", "_").replace("?", "").replace("-", "_").replace("/", "_")
                            .replace("\\", "_").replace("%", "").replace(")", "").replace("(", "").replace("$", "") for x in
                            df]  # returns list of underscored columns
            return under_scored
        elif str(type(df))=="<class 'pandas.core.series.Series'>":
            under_scored = [x.replace(" ", "_").replace("?", "").replace("-", "_").replace("/", "_")
                            .replace("\\", "_").replace("%", "").replace(")", "").replace("(", "").replace("$", "") for x in
                            list(df)]
            return under_scored

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
        
def filter_validate(filter,df):
        l=set(df.columns.values)
        c=set(filter['parameter_name'])
        x=c.difference(l)
        if x:
            return False
        else:
            return True
    # Assigning weightage
   
def wightage_dataframe(user,connection,master_sheet_table_name,master_table,master_table_date,selected_filter_name,filter_table,filter_date):

        parameters_list = list(filter_table['parameter_name'])                            # fetched parameters list
        # creating temporary dataframe from master sheetr
        st.session_state['weightage_result_df'] = master_table[['Name','BSE_Code','NSE_Code','Industry']]
        # creating temporary weightage table to attach to above dataframe for each iteration
        df_weightage_table = pd.DataFrame()

        for parameter in parameters_list:
            parameter_condition_q = "SELECT * FROM `"+selected_filter_name+"` WHERE parameter_name='"+parameter+"' and" \
                                            " username='"+user+"' and date_time='" + str(filter_date) + "'"

            df_parameter_condition=pd.read_sql_query(parameter_condition_q,connection)
            if df_parameter_condition.value1[0] and df_parameter_condition.value2[0]:
                weightage_q = "SELECT IF("+parameter+df_parameter_condition.condition1[0] + \
                                        str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                        + "," + " IF("+parameter+ df_parameter_condition.condition2[0] +\
                                        str(df_parameter_condition.value2[0]) + "," + str(df_parameter_condition.result2[0])\
                                        + ",0)) AS "+parameter+"_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"

                df_weightage=pd.read_sql_query(weightage_q,connection)
                st.session_state['weightage_result_df'][parameter]=df_weightage
                df_weightage_table[parameter] = df_weightage

            elif df_parameter_condition.value1[0]:
                weightage_q = "SELECT IF(" + parameter + df_parameter_condition.condition1[0] + \
                                          str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                          + ",0) AS " + parameter + "_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"
                df_weightage = pd.read_sql_query(weightage_q, connection)
                st.session_state['weightage_result_df'][parameter] = df_weightage
                df_weightage_table[parameter] = df_weightage
                continue

        st.session_state['weightage_result_df'].insert(4, "Positives", df_weightage_table[df_weightage_table == 1].count(axis=1))
        st.session_state['weightage_result_df'].insert(5, "Negetives", df_weightage_table[df_weightage_table == -1].count(axis=1))
        st.session_state['weightage_result_df'].insert(6, "Neutrals", df_weightage_table[df_weightage_table == 0].count(axis=1))
        st.session_state['weightage_result_df'].insert(7, "Resultants", st.session_state['weightage_result_df'].apply(lambda x: x['Positives']+x['Neutrals']-x['Negetives'], axis=1))
       
        return st.session_state['weightage_result_df']

def admin_wightage_dataframe(user,connection,master_sheet_table_name,master_table,master_table_date,selected_filter_name,filter_table,filter_date):

        parameters_list = list(filter_table['parameter_name'])                            # fetched parameters list
        # creating temporary dataframe from master sheetr
        st.session_state['weightage_result_df'] = master_table[['Name','BSE_Code','NSE_Code','Industry']]
        # creating temporary weightage table to attach to above dataframe for each iteration
        df_weightage_table = pd.DataFrame()

        for parameter in parameters_list:
            parameter_condition_q = "SELECT * FROM `"+selected_filter_name+"` WHERE parameter_name='"+parameter+"' and" \
                                            " user='"+user+"' and date_time='" + str(filter_date) + "'"

            df_parameter_condition=pd.read_sql_query(parameter_condition_q,connection)
            if df_parameter_condition.value1[0] and df_parameter_condition.value2[0]:
                weightage_q = "SELECT IF("+parameter+df_parameter_condition.condition1[0] + \
                                        str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                        + "," + " IF("+parameter+ df_parameter_condition.condition2[0] +\
                                        str(df_parameter_condition.value2[0]) + "," + str(df_parameter_condition.result2[0])\
                                        + ",0)) AS "+parameter+"_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"

                df_weightage=pd.read_sql_query(weightage_q,connection)
                st.session_state['weightage_result_df'][parameter]=df_weightage
                df_weightage_table[parameter] = df_weightage

            elif df_parameter_condition.value1[0]:
                weightage_q = "SELECT IF(" + parameter + df_parameter_condition.condition1[0] + \
                                          str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                          + ",0) AS " + parameter + "_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"
                df_weightage = pd.read_sql_query(weightage_q, connection)
                st.session_state['weightage_result_df'][parameter] = df_weightage
                df_weightage_table[parameter] = df_weightage
                continue

        st.session_state['weightage_result_df'].insert(4, "Positives", df_weightage_table[df_weightage_table == 1].count(axis=1))
        st.session_state['weightage_result_df'].insert(5, "Negetives", df_weightage_table[df_weightage_table == -1].count(axis=1))
        st.session_state['weightage_result_df'].insert(6, "Neutrals", df_weightage_table[df_weightage_table == 0].count(axis=1))
        st.session_state['weightage_result_df'].insert(7, "Resultants", st.session_state['weightage_result_df'].apply(lambda x: x['Positives']+x['Neutrals']-x['Negetives'], axis=1))
       
        return st.session_state['weightage_result_df']

#@st.cache(suppress_st_warning=True,show_spinner=False)
def user_sheets_names(username,connection):
    n = "SELECT sheet_name FROM " + master_table + " WHERE username='" + username + "'"
    st.session_state['users_sheets_names'] = pd.read_sql_query(n, connection)
    st.session_state['users_sheets_names'] = st.session_state['users_sheets_names'].drop_duplicates()
    return st.session_state['users_sheets_names']

def sheet_names(_username,table_name,_connection):
    try:
        file_names_q='SELECT sheet_name FROM ' + table_name +' Where username="'+_username+'"'
        file_names_df=pd.read_sql_query(file_names_q,_connection).drop_duplicates()
        return file_names_df
    except:
        st.error("Something were wrong......")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.warning("Please check connection.....")
            st.experimental_rerun()

def filter_names(_username,table_name,_connection):
    try:
        file_names_q='SELECT name FROM ' + table_name +' Where username="'+_username+'"'
        file_names_df=pd.read_sql_query(file_names_q,_connection).drop_duplicates()
        return file_names_df
    except:
        st.error("Something were wrong......")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.warning("Please check connection.....")
            st.experimental_rerun()

def fetch_filter(table_name,_connection,name,_username):

    try:
        selected_file_q='SELECT * FROM '+ table_name+' WHERE username="'+_username+'" and name="'+name+'"'
        selected_file_df=pd.read_sql(selected_file_q,_connection).drop_duplicates().dropna(axis=1,how='all')
        selected_file_datetime=selected_file_df['date_time'][0]
        return selected_file_df.drop(columns=['username','name','lable','date_time'],axis=1),selected_file_datetime
    except:
        st.error("Server lost.....")
        st.error("Please check connection.....")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.experimental_rerun()

# function to fetch table
#@st.experimental_memo(show_spinner=True)
def fetch_table(table_name,_connection,sheet_name=None,_username=None):
    if sheet_name and _username:
        try:
            selected_file_q='SELECT * FROM '+ table_name+' WHERE username="'+_username+'" and sheet_name="'+sheet_name+'"'
            selected_file_df=pd.read_sql(selected_file_q,_connection).drop_duplicates().dropna(axis=1,how='all')
            selected_file_datetime=selected_file_df['date_time'][0]
            return selected_file_df.drop(columns=['username','sheet_name','lable','date_time'],axis=1),selected_file_datetime
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
            selected_file_df=pd.read_sql(selected_file_q,_connection).dropna(axis=1,how='all')
            lenght_of_selected_file_df=len(selected_file_df)
            return selected_file_df,lenght_of_selected_file_df
        except:
            st.error("Server lost.....")
            st.error("Please check connection.....")
            st.session_state["sq_cur_obj"]=0
            time.sleep(2)
            st.experimental_rerun()

styles = [
        dict(selector="tr:hover",
                    props=[("background", "#f4f4f4")]),
        #rgb(86, 86, 243)
        ##00cccc
        dict(selector="th", props=[("color", "#ffff"),
                                ("border", "1px solid #eee"),
                                ("padding", "7px 5px"),
                                ("border-collapse", "collapse"),
                                ("background", "rgb(86, 86, 243)"),
                                ("text-transform", "uppercase"),
                                ("font-size", "14px")
                                ]),
        dict(selector="td", props=[("color", "#99"),
                                ("border", "1px solid #eee"),
                                ("padding", "7px 15px"),
                                ("border-collapse", "collapse"),
                                ("font-size", "15px")
                                ]),
        dict(selector="table", props=[
                                        ("font-family" , 'Arial'),
                                        ("margin" , "25px auto"),
                                        ("border-collapse" , "collapse"),
                                        ("border" , "1px solid #eee"),
                                        ("border-bottom" , "2px solid #00cccc"),                                    
                                        ]),
        dict(selector="caption", props=[("caption-side", "bottom")]),
        dict(selector="cheakbox",props=[("caption-side", "bottom")])]

styles2 = [
                    dict(selector="tr:hover",
                                props=[("background", "#f4f4f4")]),
                    #rgb(86, 86, 243)
                    ##00cccc
                    dict(selector="th", props=[("color", "#ffff"),
                                            ("border", "1px solid #eee"),
                                            ("padding", "7px 5px"),
                                            ("border-collapse", "collapse"),
                                            ("background", "rgb(86, 86, 243)"),
                                            ("text-transform", "uppercase"),
                                            ("font-size", "14px")
                                            ]),
                    dict(selector="td", props=[("color", "#99"),
                                            ("border", "1px solid #eee"),
                                            ("padding", "7px 20px"),
                                            ("border-collapse", "collapse"),
                                            ("font-size", "15px")
                                            ]),
                    dict(selector="table", props=[
                                                    ("font-family" , 'Arial'),
                                                    ("margin" , "25px auto"),
                                                    ("border-collapse" , "collapse"),
                                                    ("border" , "1px solid #eee"),
                                                    ("border-bottom" , "2px solid #00cccc"),                                    
                                                    ]),
                    dict(selector="caption", props=[("caption-side", "bottom")])]


def button(selected_sheet_name,table,username,connection):
    with st.spinner("Loading...."):
        time.sleep(1)
        st.session_state['selected_sheet_name']=selected_sheet_name
        st.session_state['selected_sheet_df'],st.session_state['selected_sheet_datetime']= fetch_table(table_name=table,_connection=connection,sheet_name=st.session_state['selected_sheet_name'],_username=username)
        st.session_state['parameters_list']=st.session_state['selected_sheet_df'].columns.to_list()[4:]
        st.experimental_rerun()


def button_selected_filter_name(selected_filter_name,table,username,connection):
    with st.spinner("Loading...."):
        time.sleep(1)
        st.session_state['selected_filter_name']=selected_filter_name
        st.session_state['selected_filter_df'],st.session_state['selected_filter_date'] = fetch_filter(table_name=table,_connection=connection,name=selected_filter_name,_username=username)
        st.experimental_rerun()


def button_EF_Apply_but(username,connection,eq_sheet_table,selected_sheet_df,selected_sheet_date,eq_filter_table,selected_filter_df,selected_filter_date,sort_by):                
    with st.spinner("Applying...."):
        time.sleep(1)
        st.session_state['weightage_result_df'] = wightage_dataframe(username, connection, eq_sheet_table, selected_sheet_df,selected_sheet_date,eq_filter_table,selected_filter_df, selected_filter_date)
        st.session_state['sorted_weightage_result_df'] = st.session_state['weightage_result_df'].sort_values(by=sort_by, ascending=False,ignore_index=True)
        st.session_state["pos_min"]=st.session_state['sorted_weightage_result_df']['Positives'].max()/2
        st.session_state["pos_max"]=st.session_state['sorted_weightage_result_df']['Positives'].max()         
        #st.experimental_rerun()
        st.success("Filter Applied Succesfully...")
        time.sleep(1)

def dashboard_charts_data(df_t,posi_min,posi_max,top_sel):
    with st.spinner("Preparing Dashboard...."):
        time.sleep(1)
        pos_minq=posi_min
        pos_maxq=posi_max
        se_top_1=top_sel

        #   Positive ranging chart
        st.session_state['positive_range_df']=positive_range_df(df=df_t,parameter_name='Positives',posi_min=pos_minq,posi_max=pos_maxq)                         
        st.session_state['positive_ranging_chart']=df_to_chart(form='Bar',df=st.session_state['positive_range_df'],title=" POSITIVE RANGING FROM {}-{} (TOTAL {} NOS)".format(pos_minq,pos_maxq,len(st.session_state['positive_range_df'])),x_axis='NSE_Code',y_axis=['Positives', 'Negetives'],barmode=barmode,template=template,orientation='v',hover_data=[],slider=True)
        
        #   Resultant chart
        st.session_state['Resultants_head_df']=df_t.head(se_top_1)
        st.session_state['Resultants_bar_chart']=df_to_chart(form='Bar',df=st.session_state['Resultants_head_df'],title=" Top {} Companies by Resultants".format(se_top_1),x_axis='NSE_Code',y_axis=['Positives', 'Negetives','Neutrals', 'Resultants'],barmode=barmode,template=template,orientation='v',hover_data=[],slider=False)
        st.session_state['Resultants_pie_chart']=df_to_chart(form='Pie',df=st.session_state['Resultants_head_df'],title=" Top {} Companies by Resultants".format(se_top_1),x_axis='NSE_Code',y_axis='Resultants',barmode=barmode,template=template,hover_data='Industry',slider=False,orientation='h')
        st.session_state['Resultants_table_styled']=df_to_chart(form='Table',df=st.session_state['Resultants_head_df'],x_axis=None,y_axis=None,barmode=None,template=None,hover_data=None,orientation=None,title=None,slider=None)

        #   Positive chart
        st.session_state['positive_head'],st.session_state['positives_head_df']=charts_df(df=df_t,sort_by='Positives',top=se_top_1)
        st.session_state['positive_bar']=df_to_chart(form='Bar',df=st.session_state['positive_head'],title="Top {} Companies by Positives".format(se_top_1),x_axis='Positives',y_axis='NSE_Code',barmode=barmode,template=template,orientation='h',hover_data=[],slider=False)
        st.session_state['positive_pie']=df_to_chart(form='Pie',df=st.session_state['positives_head_df'],title="Top {} Companies by Positives".format(se_top_1),x_axis='NSE_Code',y_axis='Positives',barmode=barmode,template=template,hover_data='Industry',slider=False,orientation='h')
        st.session_state['positive_table_styled']=df_to_chart(form='Table',df=st.session_state['positives_head_df'],x_axis=None,y_axis=None,barmode=None,template=None,hover_data=None,orientation=None,title=None,slider=None)

        #   Negetive chart
        st.session_state['negetive_head'],st.session_state['negetive_head_df']=charts_df(df=df_t,sort_by='Negetives',top=se_top_1)
        st.session_state['negetive_bar']=df_to_chart(form='Bar',df=st.session_state['negetive_head'],title="Top {} Companies by Negetives".format(se_top_1),x_axis='Negetives',y_axis='NSE_Code',barmode=barmode,template=template,orientation='h',hover_data=[],slider=False)
        st.session_state['negetive_pie']=df_to_chart(form='Pie',df=st.session_state['negetive_head_df'],title="Top {} Companies by Negetives".format(se_top_1),x_axis='NSE_Code',y_axis='Negetives',barmode=barmode,template=template,hover_data='Industry',slider=False,orientation='h')
        st.session_state['negetive_table_styled']=df_to_chart(form='Table',df=st.session_state['negetive_head_df'],x_axis=None,y_axis=None,barmode=None,template=None,hover_data=None,orientation=None,title=None,slider=None)

        #   Neutrals chart
        st.session_state['neutrals_head'],st.session_state['neutrals_head_df']=charts_df(df=df_t,sort_by='Neutrals',top=se_top_1)
        st.session_state['neutral_bar']=df_to_chart(form='Bar',df=st.session_state['neutrals_head'],title="Top {} Companies by Neutrals".format(se_top_1),x_axis='Neutrals',y_axis='NSE_Code',barmode=barmode,template=template,orientation='h',hover_data=[],slider=False)
        st.session_state['neutral_pie']=df_to_chart(form='Pie',df=st.session_state['neutrals_head_df'],title="Top {} Companies by Neutrals".format(se_top_1),x_axis='NSE_Code',y_axis='Neutrals',barmode=barmode,template=template,hover_data='Industry',slider=False,orientation='h')
        st.session_state['neutral_table_styled']=df_to_chart(form='Table',df=st.session_state['neutrals_head_df'],x_axis=None,y_axis=None,barmode=None,template=None,hover_data=None,orientation=None,title=None,slider=None)
        
        st.success("Dashboard generated....")
        time.sleep(1)
    #st.experimental_rerun()

def report_data(df_t,sentiment_table,connection):
    with st.spinner("Preparing Report...."):
        st.session_state['report_chart']=df_to_chart(form='Bar',df=df_t,title='Weightage Graph',x_axis='NSE_Code',y_axis=['Positives', 'Negetives','Neutrals', 'Resultants'],barmode=barmode,template=template,orientation='v',hover_data=[],slider=True)
        st.session_state['analysis_report']=analysis_report(df=df_t,transposed_col='Name',color_func=cell_color,drop_list=["Name","BSE_Code","NSE_Code","Industry","Resultants","Positives","Negetives","Neutrals"])
        st.session_state['total_weightage_report']=styled_total_weightage(df=df_t,drop_list=["Name","Industry","Resultants","Positives","Negetives","Neutrals"])
        st.session_state['sentiment_report']=sentiment_report(df=df_t,names_col='Name',sentiment_table=sentiment_table,connection=connection)
        st.success("Report generated....")
        time.sleep(1)


#function for charts style
def charts_style(form,df,slider):
    if form=='Bar':
        df.update_layout(legend=dict(orientation="h"))
        df.update_xaxes(title_text='')
        df.update_yaxes(title_text='')
        df.update_xaxes(title_font=dict(size=3))
        df.update_xaxes(tickfont=dict(size=12))
        df.update_yaxes(tickfont=dict(size=12))
        df.update_layout(coloraxis_showscale=False)
        df.update_xaxes(ticklen=0)
        df.update_yaxes(ticklen=0)
        df.update_layout(plot_bgcolor='rgba(0, 0, 0, 0)',paper_bgcolor='rgba(0,0,0,0)')
        df.update_xaxes(rangeslider_visible=slider)
        df.update_xaxes(showgrid=False)
        df.update_yaxes(showgrid=False)
        #df.update_yaxes(ticklabelposition="inside top")
        df.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
        return df
    
    if form=='Pie':
        df.update_traces(showlegend=False,pull = [0.25])
        df.update_traces(hovertemplate=None,textinfo='percent+label', textposition='outside', rotation=50)
        #df.add_annotation(dict(x=0.5, y=0.5,  align='center',xref = "paper", yref = "paper",showarrow = False, font_size=22))
        df.update_layout(legend=dict(orientation="h"))
        return df
    
    if form=='Table':
        return df.style.applymap(cell_color).set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['Name'])\
                .set_properties(**{'background-color': '#d67d09','color': 'White','font-weight':'bold'}, subset=['Resultants'])\
                .set_properties(**{'background-color': '#229106','color': 'White','font-weight':'bold'}, subset=['Positives'])\
                .set_properties(**{'background-color': '#c70404','color': 'White','font-weight':'bold'}, subset=['Negetives'])\
                .set_properties(**{'background-color': '#b0ad04','color': 'White','font-weight':'bold'}, subset=['Neutrals'])
        
    
#functions for charts
def df_to_chart(form,df,x_axis,y_axis,barmode,template,hover_data,orientation,title,slider):
    width=700
    height=700
    text_auto='.2s'
    if form=='Bar':    
        bar_chart = px.bar(df, y=y_axis, x=x_axis,barmode=barmode,template=template,text_auto='.2s',hover_data=hover_data,orientation=orientation,title=title,height=500,width=1000)
        styled_bar_chart=charts_style(form,bar_chart,slider)
        return styled_bar_chart

    elif form=='Pie':
        pie_chart=px.pie(df,names=x_axis,values=y_axis,hover_name=hover_data,template=template,hole=0.4,height=450,width=600,title=title)
        styled_pie_chart=charts_style(form,pie_chart,slider)
        return styled_pie_chart
    
    elif form=='Table':
        styled_table=charts_style(form,df,slider)
        return styled_table

def analysis_report(df,transposed_col,color_func,drop_list=[]):
    df_dupli=df.copy(deep=True)
    df_dupli=df_dupli.drop(drop_list,axis=1)
    df2_transposed = df_dupli.T 
    df2_transposed.columns=df[transposed_col]
    excl_styl_df=df2_transposed.style.applymap(color_func).highlight_null(null_color="black")
    return excl_styl_df


def styled_total_weightage(df,drop_list=[]):

    w_df=df[drop_list]
    w_df_ind=list(range(1, len(st.session_state['sorted_weightage_result_df'])+1))
    w_df.set_index([pd.Index(w_df_ind)],inplace = True)

    
    #st.write(w_df)
    cell_hover = {  # for row hover use <tr> instead of <td>
            'selector': 'td:hover',
            'props': [('background-color', '#07918f')]
    }
    index_names = {
            'selector': '.index_name',
            'props': 'font-style: italic; color: darkgrey; font-weight:normal;'
    }
    headers = {
            'selector': 'th:not(.index_name)',
            'props': 'background-color: #000066; color: white;'
    }
    w_df_styled=w_df.style.set_table_styles([cell_hover, index_names, headers])\
                .set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['Name'])\
                .set_properties(**{'background-color': '#d67d09','color': 'White','font-weight':'bold'}, subset=['Resultants'])\
                .set_properties(**{'background-color': '#229106','color': 'White','font-weight':'bold'}, subset=['Positives'])\
                .set_properties(**{'background-color': '#c70404','color': 'White','font-weight':'bold'}, subset=['Negetives'])\
                .set_properties(**{'background-color': '#b0ad04','color': 'White','font-weight':'bold'}, subset=['Neutrals'])
    
    return  w_df_styled


def sentiment_report(df,names_col,sentiment_table,connection):
                                    
    sentiment_df=pd.DataFrame({'stock_name': [], 'date': [],
                        'polarity': [], 'polarity_sentiment': [], 'subjectivity': [], 'positive_score': [],
                        'negative_score': [],'net_score': [], 'news_senti_3_M': [], 'news_senti_1_M': [], 'news_senti_7_d': [],
                        'news_senti_2_d': []})

    stocks_names=df[names_col].values.tolist()
    
    for name in stocks_names:
        sentiment_q='SELECT * FROM '+sentiment_table+' Where stock_name="'+name+'"'
        sentiment_row=pd.read_sql_query(sentiment_q,connection)
        sentiment_df=sentiment_df.append(sentiment_row,ignore_index=True)

    styled_sentiment_df=sentiment_df[['stock_name','date','news_senti_3_M', 'news_senti_1_M', 'news_senti_7_d','news_senti_2_d','polarity', 'polarity_sentiment', 'subjectivity', 'positive_score','negative_score','net_score']]\
                        .style.applymap(cell_color2,subset=['news_senti_3_M', 'news_senti_1_M', 'news_senti_7_d','news_senti_2_d','polarity_sentiment'])\
                        .set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['stock_name'])\
                        .bar(subset=['positive_score'], color='green')\
                        .bar(subset=['negative_score'], color='red')\
                        .bar(subset=['net_score'], color='blue')
    
    return styled_sentiment_df

def positive_range_df(df,parameter_name,posi_min,posi_max):
    sh_chart=df[df[parameter_name] >= posi_min ]
    sh2_chart=sh_chart[sh_chart[parameter_name] <= posi_max ]
    positive_range_df=sh2_chart.sort_values(by=[parameter_name], ascending=False,ignore_index=True)
    return positive_range_df


def charts_df(df,sort_by,top):
    chart_head=df.sort_values(by=sort_by, ascending=True).tail(top)
    chart_head_df=df.sort_values(by=sort_by, ascending=False).head(top)
    chart_head_df_index=list(range(1, len(chart_head_df)+1))
    chart_head_df.set_index([pd.Index(chart_head_df_index)],inplace = True)
    return chart_head,chart_head_df

def new_to_excel(df_1,df_2,df_3):
    output = BytesIO()
    #writer = pd.ExcelWriter(output, engine='xlsxwriter')
    with pd.ExcelWriter(output,engine='xlsxwriter') as writer:  
        df_1.to_excel(writer,sheet_name='ANALYSIS',engine='openpyxl')
        df_2.to_excel(writer, sheet_name='SUMMARY',startrow=2,engine='openpyxl')
        df_3.to_excel(writer, sheet_name='SENTIMENT',startrow=2,engine='openpyxl')
    #df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['ANALYSIS']
    format1 = workbook.add_format({'num_format': '0.00'}) 
    worksheet.set_column('A:A', None, format1)  
    worksheet2 = writer.sheets['SUMMARY']
    format2 = workbook.add_format({'num_format': '0.00'}) 
    worksheet2.set_column('A:A', None, format2) 
    worksheet3 = writer.sheets['SENTIMENT']
    format3 = workbook.add_format({'num_format': '0.00'}) 
    worksheet3.set_column('A:A', None, format3) 
    writer.save()
    processed_data = output.getvalue()
    return processed_data

_="""

    Sessio state variables

"""

_=""" Variables"""
# 0.Sheets names
#if 'users_sheets_names' not in st.session_state:
#    st.session_state['users_sheets_names']=sheet_names(st.session_state["username"],master_table,sq_conn)
if not len(st.session_state['users_sheets_names']):
    st.error("Sorry, you didn't have uploaded any sheets...")
    st.warning("Please,upload your sheets first!!!!")
    st.stop()

# 1.selected sheet
if 'selected_sheet_name' not in st.session_state:
    st.session_state['selected_sheet_name']=st.session_state['users_sheets_names'].iloc[0]['sheet_name']

# 2.selected sheet df
if 'selected_sheet_df' and 'selected_sheet_datetime' not in st.session_state:
    st.session_state['selected_sheet_df'],st.session_state['selected_sheet_datetime']= fetch_table(table_name=master_table,_connection=sq_conn,sheet_name=st.session_state['selected_sheet_name'],_username=st.session_state["username"])

# 3.Parameters list
if 'parameters_list' not in st.session_state:
    st.session_state['parameters_list']=st.session_state['selected_sheet_df'].columns.to_list()[4:]
#st.write(st.session_state['parameters_list'])

# 4.filters names
if 'users_filter_names' not in st.session_state:
    st.session_state['users_filter_names']=filter_names(_username=st.session_state['username'],table_name=filter_table,_connection=sq_conn)

if len(st.session_state['users_filter_names']):
    # 5.selected filter name
    if 'selected_filter_name' not in st.session_state:
        st.session_state['selected_filter_name']=st.session_state['users_filter_names'].sort_values(by="name",ascending=False).iloc[0]['name']


    # 6.selected filter df
    if 'selected_filter_df' and 'selected_filter_date' not in st.session_state:
        st.session_state['selected_filter_df'],st.session_state['selected_filter_date']=fetch_filter(table_name=filter_table,_connection=sq_conn,name=st.session_state['selected_filter_name'],_username=st.session_state['username']) 

# 7.top se
if "top_se" not in st.session_state:
    st.session_state["top_se"]=20

# 8.Weightage Result dataframe
if 'weightage_result_df' not in st.session_state:
    st.session_state['weightage_result_df']=0

# 9.sorted Weightage Result dataframe
if 'sorted_weightage_result_df' not in st.session_state:
    st.session_state['sorted_weightage_result_df']=pd.DataFrame()

# 10.Pos_min
if "pos_min" not in st.session_state:
    st.session_state["pos_min"]=0

# 11.pos_max
if "pos_max" not in st.session_state:
    st.session_state["pos_max"]=0  

# 12.Charts data
if 'positive_range_df' and 'positive_ranging_chart' not in st.session_state:
    st.session_state['positive_range_df'],st.session_state['positive_ranging_chart']=0,0

if 'Resultants_head_df' and 'Resultants_bar_chart' and 'Resultants_pie_chart' and 'Resultants_table_styled' not in st.session_state:
    st.session_state['Resultants_head_df'],st.session_state['Resultants_bar_chart'],st.session_state['Resultants_pie_chart'],st.session_state['Resultants_table_styled']=0,0,0,0

if 'positive_head' and 'positives_head_df' and 'positive_bar' and 'positive_pie' and 'positive_table_styled' not in st.session_state:
    st.session_state['positive_head'],st.session_state['positives_head_df'],st.session_state['positive_bar'],st.session_state['positive_pie'],st.session_state['positive_table_styled']=0,0,0,0,0

if 'negetive_head' and 'negetive_head_df' and 'negetive_bar' and 'negetive_pie' and 'negetive_table_styled' not in st.session_state:
    st.session_state['negetive_head'],st.session_state['negetive_head_df'],st.session_state['negetive_bar'],st.session_state['negetive_pie'],st.session_state['negetive_table_styled']=0,0,0,0,0

if 'neutrals_head' and 'neutrals_head_df' and 'neutral_bar' and 'neutral_pie' and 'neutral_table_styled' not in st.session_state:
    st.session_state['neutrals_head'],st.session_state['neutrals_head_df'],st.session_state['neutral_bar'],st.session_state['neutral_pie'],st.session_state['neutral_table_styled']=0,0,0,0,0

# 13.Report data
if 'report_chart' and 'analysis_report' and 'total_weightage_report' and 'sentiment_report' not in st.session_state:
    st.session_state['report_chart'],st.session_state['analysis_report'],st.session_state['total_weightage_report'],st.session_state['sentiment_report']=0,0,0,0
                    

_=""" Buttons"""
if 'delete_clicked2' not in st.session_state:
    st.session_state.delete_clicked2 = False

def del_callback2():
    st.session_state.delete_clicked2 = True

if 'save_as' not in st.session_state:
    st.session_state.save_as = False

def save_as_callback():
    st.session_state.save_as = True

#Filter apply button
if "EF_Apply" not in st.session_state:
    st.session_state['EF_Apply']=False
def EF_Apply_callback():
    st.session_state['EF_Apply']=True

_="""
    1.Master Sheet Selection
"""
# 1.2) Cheaks sheets are available or not
if len(st.session_state['users_sheets_names'])!=0:

    mf_col1[0].header('📁 My Sheets')       
    mf_col1[2].write('')    
    mf_col1[2].write('')    

    selected_sheet_name= mf_col1[1].selectbox("", options=st.session_state['users_sheets_names'])

    if mf_col1[2].button("🔍"):
        button(selected_sheet_name=selected_sheet_name,table=master_table,username=st.session_state['username'],connection=sq_conn)    

    rem_us_parameter_list = rem_under_scr_col_list(st.session_state['parameters_list'])
    
    mf_col1[0].subheader('📄 {}'.format(st.session_state['selected_sheet_name']))
    mf_col1[1].write('📅 Created at:'+str(st.session_state['selected_sheet_datetime']))

    with st.expander("👀 Show my sheet"):
        st.write("{} results found:".format(len(st.session_state['selected_sheet_df'])))
        show_df=st.session_state['selected_sheet_df'].copy()
        st._arrow_dataframe(pagination(show_df,key_name='sheet',index_param='Name').style.highlight_null(null_color="red"))


    #st.markdown("---")
    #with tab3:
        #st.info("Filter removed")
    tab1,tab2,tab3=st.tabs(["🧮 My Filter","👨‍🔧 Create Filter","🔨 Update Filter"])
  
    with tab2:
        fil_col=st.columns((8,10,4))
        fil_col[0].header('👨‍🔧 Create Filter')

        dem_f_df=st.session_state['users_filter_names']
        #st.write(list(dem_f_df.drop_duplicates()['name']))
        if fil_col[2].button("📤Upload Demo Filter"):
            
            fil_name="Small-Case-Parameters(Demo filter)"
            if fil_name in dem_f_df['name'].to_list():
                st.error("Demo Filter already uploaded!")
                time.sleep(1)
                st.experimental_rerun()
            else:
                with st.spinner("Uploading..."):

                    df = pd.read_csv('scratch/testcsv/master_filter.csv',index_col=None)
                    time.sleep(2)
                    df['parameter_name'] = [x.replace(" ", "_") for x in df['parameter_name']]
                    df.insert(0, "date_time", str(current_date))
                    df.insert(1, "lable", st.session_state["lable"])
                    df.insert(2, "username", st.session_state["username"])
                    df.insert(3, "name", fil_name)

                    df.to_sql(filter_table, sq_conn, if_exists='append', index=False)
                    time.sleep(1)
                    st.success("Done")
                    time.sleep(1)
                    refresh_dashboard(username)
                    st.experimental_rerun()
            
        if 'df_filter' not in st.session_state:
            st.session_state.df_filter = pd.DataFrame({'parameter_name': [], 'condition1': [],
                                                    'value1': [], 'result1': [], 'condition2': [], 'value2': [],
                                                    'result2': []})

        #fil_col[0].subheader("Create Filter:")
        name=st.text_input("Name this filter")
        select_param = st.selectbox("Select parameters:", options=rem_us_parameter_list)
        col1, col2, col3 = st.columns(3)
        with col1:
            operator_1 = st.selectbox("Select 1st operator:", options=['>', '<', '='])
            operator_2 = st.selectbox("Select 2nd operator:", options=['>', '<', '='])
        with col2:
            value_1 = st.number_input("Enter 1st value:",step=1)
            value_2 = st.number_input("Enter 2nd value:",step=1)
        with col3:
            result_1 = st.selectbox("Select 1st result:", options=[-1, 1])
            result_2 = st.selectbox("Select 2nd result:", options=[-1, 1])
        bu_col=st.columns((1,10,1))
        if bu_col[0].button('AND'):
            selection = {'parameter_name': select_param, 'condition1': operator_1,
                            'value1': value_1, 'result1': result_1, 'condition2': operator_2, 'value2': value_2,
                            'result2': result_2}

            st.session_state.df_filter = st.session_state.df_filter.append(selection, ignore_index=True)
            st.write("Selected Conditions List:")
            st.table(st.session_state.df_filter.style.set_table_styles(styles))
            st.experimental_rerun()
        else:
            if len(st.session_state.df_filter):
                st.write("Selected Conditions List:")
                st.table(st.session_state.df_filter.style.set_table_styles(styles))

        if len(st.session_state.df_filter):
            if bu_col[2].button("Clear"):
                st.session_state.df_filter = st.session_state.df_filter.drop(len(st.session_state.df_filter) - 1)
                st.experimental_rerun()

            save_col=st.columns((5,1,5))
            if save_col[1].button("Save"):
                if name=='':
                    st.error("Please,give a name to this filter!")
                    st.stop()
              
                if name in st.session_state['users_filter_names']['name'].to_list() :
                    st.error("Sorry,Filter name already used!!")
                else:
                    with st.spinner('Uploding...'):
                        time.sleep(1)
                        st.session_state.df_filter['parameter_name'] = [x.replace(" ", "_") for x in
                                                                            st.session_state.df_filter['parameter_name']]
                        st.session_state.df_filter.insert(0, "date_time", str(current_date))
                        st.session_state.df_filter.insert(1, "lable", st.session_state["lable"])
                        st.session_state.df_filter.insert(2, "username", st.session_state["username"])
                        st.session_state.df_filter.insert(3, "name", name)

                        try:
                            st.session_state.df_filter.to_sql(filter_table, sq_conn, if_exists='append', index=False)
                            st.success("Uploaded...")
                            time.sleep(1)
                            st.session_state.df_filter = st.session_state.df_filter.drop(x for x in range(len(st.session_state.df_filter)))
                            st.session_state.df_filter.drop(st.session_state.df_filter.columns[[0,1,2,3]], axis=1, inplace=True)
                            refresh_dashboard(username)
                        except:
                            st.error("Error to save this filter...")
                            st.warning("please try again....")
                            time.sleep(2)
                            st.experimental_rerun()
                        st.experimental_rerun()



    with tab3:


        updf=st.columns((5,5))
        updf[0].header("🔨 Update Filter")
            
        if len(st.session_state['users_filter_names'])>0:
            
            
            selected_filter_name = updf[1].selectbox("Select your saved filter", options=st.session_state['users_filter_names'].sort_values(by="name",ascending=False),key="sfgh")

            selected_filter_df,selected_filter_date = fetch_filter(table_name=filter_table,_connection=sq_conn,name=selected_filter_name,_username=username)
                    
            show_df3 = selected_filter_df.copy()

            # Update form
           
            test_df=show_df3.copy()    
            # Data collection from Dataframe for update from
            Options=show_df3['parameter_name']
            conditions=['>', '<', '=']
            results=[-1, 1]

            #Values
            #show_df3.set_index('parameter_name',inplace=True)
 
            sho_col=st.columns((1))


            with st.expander("Update Parameter"):
                new_col=st.columns((15,8,5))
                select_param2 = st.selectbox("Select parameter:", options=Options,key="erserstdvdvyfd")

                # to get index value of the parameter
                ind=np.where(show_df3["parameter_name"] == select_param2)
                #st.write(ind[0][0])

                # to get values of columns of selected parameter
                value1=show_df3.at[ind[0][0], 'value1']
                value2=show_df3.at[ind[0][0], 'value2']
                condition1=show_df3.at[ind[0][0], 'condition1']
                condition2=show_df3.at[ind[0][0], 'condition2']

                if condition1=='>':
                    index1=0
                elif condition1=='<':
                    index1=1
                else :
                    index1=2

                if condition2=='>':
                    index2=0
                elif condition2=='<':
                    index2=1
                else :
                    index2=2

                result1=show_df3.at[ind[0][0], 'result1']
                result2=show_df3.at[ind[0][0], 'result2']
            

                if result1==-1:
                    Result1=0
                else:
                    Result1=1

                if result2==-1:
                    Result2=0
                else:
                    Result2=1

                col1, col2, col3 = st.columns(3)
                with col1:
                    operator_1 = st.selectbox("Select 1st operator:", options=conditions,key="ererfgds",index=index1)
                    operator_2 = st.selectbox("Select 2nd operator:", options=conditions,key="ererdfgs",index=index2)
                with col2:
                    value_1 = st.number_input("Enter 1st value:",key="ersdfges",value=value1)
                    value_2 = st.number_input("Enter 2nd value:",key="erseergr",value=value2 )
                with col3:
                    result_1 = st.selectbox("Select 1st result:", options=results,key="ersegrferggrs",index=Result1)
                    result_2 = st.selectbox("Select 2nd result:", options=results,key="ersdfgeergrs",index=Result2)
            
                
                if 'temp_df' not in st.session_state:
                    st.session_state.temp_df=pd.DataFrame({"parameter_name":[],"condition1":[],"value1":[],"result1":[],"condition2":[],"value2":[],"result2":[]})

                updf_bcol=st.columns((1,10,1))

                test_df.at[ind[0][0],'value1'] = value_1
                test_df.at[ind[0][0],'value2'] = value_2

                test_df.at[ind[0][0],'condition1'] = operator_1
                test_df.at[ind[0][0],'condition2'] = operator_2

                test_df.at[ind[0][0],'result1'] = result_1
                test_df.at[ind[0][0],'result2'] = result_2
                            
                
                if updf_bcol[0].button("Update",key="zdfzv"):
                    st.session_state.temp_df=st.session_state.temp_df.append({"parameter_name":select_param2,"condition1":operator_1,"value1":value_1,"result1":result_1,"condition2":operator_2,"value2":value_2,"result2":result_2}, ignore_index=True)
                    test_df.index = np.arange(1, len(test_df) + 1)
                    st.session_state.temp_df.index = np.arange(1, len(st.session_state.temp_df) + 1)

                    with st.spinner("Updating..."):
                        time.sleep(1)
                        try:
                            for i in range(len(st.session_state.temp_df)):
                                upd_df=st.session_state.temp_df.iloc[[i]]
                                operator__1=str(upd_df['condition1'][i+1])
                                operator__2=str(upd_df['condition2'][i+1])
                                value__1=float(upd_df['value1'][i+1])
                                value__2=float(upd_df['value2'][i+1])
                                result__1=int(upd_df['result1'][i+1])
                                result__2=int(upd_df['result2'][i+1])
                                
                                update_q="UPDATE "+filter_table+" SET condition1 = '"+str(operator__1)+"', value1 = "+str(value__1)+", result1 = "+str(result__1)+", condition2 = '"+str(operator__2)+"', value2 = "+str(value__2)+", result2 = "+str(result__2)+" WHERE username='"+st.session_state["username"]+"' and name='"+str(selected_filter_name)+"' and date_time='"+str(selected_filter_date)+"' and parameter_name='"+select_param2+"'"
                                
                                sq_cur.execute(text(update_q))                                
                                st.success("Filter updated...")
                                st.session_state.temp_df.drop(st.session_state.temp_df.index, inplace=True)
                                refresh_dashboard(username)    
                        except:
                            st.error("Error to update...")
                            st.warning("please try again....")
                            time.sleep(1)
                            st.experimental_rerun()
                        st.experimental_rerun()


                if updf_bcol[2].button("Delete",key="opacdsi"):
                    #  Delete query:DELETE FROM master_filter WHERE date_time = "2022-09-05 06:46:14" and `user`="chetan" and `name`="new" and `parameter_name`="EPS"
                    del_row = "DELETE FROM " + filter_table +" WHERE date_time='"+str(selected_filter_date)+"' and username='"+st.session_state["username"]+"' and name='"+str(selected_filter_name)+"' and parameter_name='"+select_param2+"'"
                    with st.spinner("Deleting...."):
                        time.sleep(1)
                        try:
                            sq_cur.execute(text(del_row))
                            st.success("Deleted...")
                            time.sleep(2)
                            refresh_dashboard(username)    
                            #st.experimental_rerun()
                        except:
                            st.error("Error to delete...")
                            st.warning("please try again....")
                            time.sleep(2)
                        st.experimental_rerun()

                        



            with st.expander("Merge Filter"):
                fi_info=st.columns((1,1))
                selected_filter_name_m=fi_info[1].selectbox("Select filter to merge",options=st.session_state['users_filter_names'].sort_values(by="name",ascending=False))


                selected_filter_df_m,selected_filter_date_m=fetch_filter(table_name=filter_table,_connection=sq_conn,name=selected_filter_name_m,_username=username,)

                fi_info2=st.columns((1,2,1))
                fi_info[0].markdown("##### {}".format(selected_filter_name_m))
                fi_info2[2].write("Created: {}".format(selected_filter_date_m))     

                show_df4 =selected_filter_df_m.copy()

                show_df4_ind=list(range(1, len(show_df4)+1))
                show_df4.set_index([pd.Index(show_df4_ind)],inplace = True)
                fi_info2[0].write("{} Condition's".format(len(show_df4)))

                st.table(pagination(show_df4,key_name='s_filter',index_param='parameter_name').style.set_table_styles(styles))

                test_df1=selected_filter_df_m.copy().copy()

                same_param=set(test_df['parameter_name']).intersection(test_df1['parameter_name'])

                if st.button("Merge"):

                    if same_param:
                        st.warning("Conditions already present:{}".format(same_param))
                        time.sleep(2)
                        st.experimental_rerun()
                    else:
                        same_param=set(test_df['parameter_name']).difference(test_df1['parameter_name'])
                        test_df1.insert(0, "date_time", str(selected_filter_date))
                        test_df1.insert(1, "lable", st.session_state["lable"])
                        test_df1.insert(2, "username", st.session_state["username"])
                        test_df1.insert(3, "name", selected_filter_name)
                        with st.spinner("Merging..."):
                            time.sleep(1)
                            try:
                                test_df1.to_sql(filter_table, sq_conn, if_exists='append', index=False)
                                st.succcess("Filters are merged..")
                                time.sleep(1)
                                refresh_dashboard(username)    
                            except:
                                st.error("Error to merge...")
                                st.warning("please try again....")
                                time.sleep(2)
                            st.experimental_rerun()
            
            test_df.index = np.arange(1, len(test_df) + 1)
            #show_df3 = selected_filter_df_m.copy()

            fi_info=st.columns((10,10,7))
            fi_info[0].markdown("##### {}".format(selected_filter_name))
            fi_info[2].write("Created: {}".format(selected_filter_date))  
            fi_info[0].write("{} Condition's".format(len(test_df)))

            st.table(pagination(test_df,key_name='m_filter',index_param='parameter_name').style.set_table_styles(styles).apply(lambda x: ['background: yellow' if x.name==ind[0][0]+1 else'' for i in x],axis=1))   

            
            _="""if (st.button("Save As",on_click=save_as_callback) or st.session_state.save_as):
                with st.form("Save As"):
                    name= st.text_input("Name your Filter")
                    sv_col=st.columns((1,1,10))
                    if sv_col[0].form_submit_button("Save") and len(name)>0:
                        st.write("del")
                        
                    if sv_col[1].form_submit_button("No"):
                        pass"""

        else:
            st.error("Sorry, you didn't have created any filters!")
            st.warning("Please create your filters..")
      
       
    with tab1:

            #st.subheader("My Filters:") 
            fil_col=st.columns((5,4,1))
            fil_col[0].header('🧰 My Filters')   
            fil_col[2].write('')    
            fil_col[2].write('')    

            #st.write(users_filter_names)
            if len(st.session_state['users_filter_names'])>0:
                
                selected_filter_name = fil_col[1].selectbox("", options=st.session_state['users_filter_names'].sort_values(by="name",ascending=False))
                if fil_col[2].button("🔍",key='selected_filter_name_but'):
                    button_selected_filter_name(selected_filter_name=selected_filter_name,table=filter_table,username=st.session_state["username"],connection=sq_conn)
                
                fil_col[0].subheader('🧮 {}'.format(st.session_state['selected_filter_name']))
                fil_col[1].write('📅 Created at:'+str(st.session_state['selected_filter_date']))

            else:
                st.error("Sorry, you didn't have created any filters!")
                st.stop()            

            with st.expander("👀 Show Filter"):
                st.write("{} results found:".format(len(st.session_state['selected_filter_df'])))
                filter_lc=st.columns((10,10,2))

                if (filter_lc[2].button("Delete",on_click=del_callback2) or st.session_state.delete_clicked2):
                    with st.form(" Are you Sure?"):
                        st.write("Are you Sure ?")
                        del_o_col=st.columns((1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1))
                        if del_o_col[0].form_submit_button("Yes"):
                            with st.spinner('Deleting...'):
                                time.sleep(2)
                                l = "DELETE FROM " + filter_table + " WHERE user='" + st.session_state["username"] + "' and date_time='" + str(selected_filter_date) + "'"
                                sq_cur.execute(text(l))
                                st.success("Filter Deleted")
                                time.sleep(2)
                                st.session_state.delete_clicked2=False
                                st.experimental_rerun()
                        elif del_o_col[1].form_submit_button("No"):
                            st.session_state.delete_clicked2=False
                            st.experimental_rerun()
            
                    #st.experimental_rerun()
                
                show_selected_filter_df = st.session_state['selected_filter_df'].copy()
                #st.table(test_df.style.set_table_styles(styles).apply(lambda x: ['background: yellow' if x.name==ind[0][0]+1 else'' for i in x],axis=1))   

                st.table(pagination(show_selected_filter_df,key_name='filter',index_param='parameter_name').style.set_table_styles(styles).apply(lambda x: ['background: yellow' if x.name==ind[0][0]+1 else'' for i in x],axis=1))   


            apply_col=st.columns((5,1,4,2))
            if apply_col[1].button("Apply",key='EF_Apply_but'):

                button_EF_Apply_but(username= st.session_state["username"],connection= sq_conn,eq_sheet_table= master_table,selected_sheet_df= st.session_state['selected_sheet_df'],selected_sheet_date= st.session_state['selected_sheet_datetime'],
                                                                    eq_filter_table= filter_table,selected_filter_df= st.session_state['selected_filter_df'],selected_filter_date= st.session_state['selected_filter_date'],sort_by='Resultants')            
                
                dashboard_charts_data(df_t=st.session_state['sorted_weightage_result_df'],posi_max=st.session_state["pos_max"],posi_min=st.session_state["pos_min"],top_sel=st.session_state["top_se"])
                
                report_data(df_t=st.session_state['sorted_weightage_result_df'],sentiment_table=sentiment_table,connection=sq_conn)

                st.experimental_rerun()


            if not len(st.session_state['sorted_weightage_result_df'])==0:
                  
                if apply_col[3].checkbox("Create Dashboard"):
                    with st.expander("Create Dashboard"):

                        st.write("Positive Ranging")
                        pos_col=st.columns((5,5))
                        pos_min=pos_col[0].number_input("minimum",step=1,min_value=st.session_state['sorted_weightage_result_df']['Positives'].min(),value=st.session_state['sorted_weightage_result_df']['Positives'].min())
                        
                        pos_max=pos_col[1].number_input("maximum",step=1,max_value=st.session_state['sorted_weightage_result_df']['Positives'].max(),value=st.session_state['sorted_weightage_result_df']['Positives'].max())

                        se_top = st.number_input("Select range", step=1,max_value=len(st.session_state['sorted_weightage_result_df']),value=5)
                        but_col=st.columns((2,10,3))
                        chart_show=but_col[0].button("apply")
                        if chart_show:
                            if pos_max>pos_min:
                                with st.spinner("loading filter"):
                                    time.sleep(2)
                                    st.session_state["top_se"]=se_top
                                    st.session_state["pos_min"]=pos_min
                                    st.session_state["pos_max"]=pos_max
                                    dashboard_charts_data(df_t=st.session_state['sorted_weightage_result_df'],posi_max=st.session_state["pos_max"],posi_min=st.session_state["pos_min"],top_sel=st.session_state["top_se"])
            
                                    #report_data(df_t=st.session_state['sorted_weightage_result_df'],sentiment_table=sentiment_table,connection=sq_conn)

                                    st.experimental_rerun()
                                    st.expander(collapse)
                            else:
                                st.error("Maximum value should be greater than minimum!")

                        elif but_col[2].button("Reset Dashboard"):
                            st.session_state["pos_min"]=st.session_state['sorted_weightage_result_df']['Positives'].max()/2
                            st.session_state["pos_max"]=st.session_state['sorted_weightage_result_df']['Positives'].max() 
                            st.session_state["top_se"]=20
                            dashboard_charts_data(df_t=st.session_state['sorted_weightage_result_df'],posi_max=st.session_state["pos_max"],posi_min=st.session_state["pos_min"],top_sel=st.session_state["top_se"])
                            report_data(df_t=st.session_state['sorted_weightage_result_df'],sentiment_table=sentiment_table,connection=sq_conn)
                            st.success("Dashboard has been reset...")
                            st.experimental_rerun()
                        else:
                            st.session_state["pos_min"]=st.session_state['sorted_weightage_result_df']['Positives'].max()/2
                            st.session_state["pos_max"]=st.session_state['sorted_weightage_result_df']['Positives'].max() 
                
                sv_tab1,sv_tab2=st.tabs(['Dashboard','Report'])
                
                with sv_tab1:
                    ex_col=st.columns((5,7))

                    ex_col[0].subheader("Dashboard")
                    ex_col[1].info("Sheet:{}|Filter:{}".format(st.session_state['selected_sheet_name'],st.session_state['selected_filter_name']))

                    #   Positives_ranging chart     
                    st.plotly_chart(st.session_state['positive_ranging_chart'], use_container_width=True)

                    #   Resultant charts
                    c_col=st.columns((8,3,9,6,3))

                    r_chart=c_col[3].radio( " ",options=('Bar', 'Pie','Table'),horizontal=True)
                    if r_chart=='Bar':
                        st.plotly_chart(st.session_state['Resultants_bar_chart'], use_container_width=True)

                    elif r_chart=='Pie':
                        st.plotly_chart(st.session_state['Resultants_pie_chart'], use_container_width=True)

                    elif r_chart=='Table':
                        c_col[0].write('')
                        c_col[0].write('')
                        c_col[4].write('')
                        c_col[4].write('')

                        c_col[0].write(" Top {} Companies by Resultants".format(st.session_state["top_se"]))
                        st._legacy_dataframe(st.session_state['Resultants_table_styled'])
                        c_col[4].download_button("Export 📥",data=to_excel(st.session_state['Resultants_table_styled']) ,file_name=selected_sheet_name+'Top {} Resultants.xlsx'.format(st.session_state["top_se"]))
                                #st.markdown("""---""")

                    
                    positives_col=st.columns((8,3,9,6,3))

                    p_chart=positives_col[3].radio( " ",options=('Bar', 'Pie','Table'), horizontal=True,key="Positives_charts")
                    if p_chart=='Bar':
                        st.plotly_chart(st.session_state['positive_bar'], use_container_width=True)

                    elif p_chart=='Pie':
                        st.plotly_chart(st.session_state['positive_pie'], use_container_width=True)

                    elif p_chart=='Table':
                        positives_col[0].write('')
                        positives_col[0].write('')
                        positives_col[4].write('')
                        positives_col[4].write('')

                        positives_col[0].write("Top {} Companies by Positives".format(st.session_state["top_se"]))
                        st._legacy_dataframe(st.session_state['positive_table_styled'])
                        positives_col[4].download_button("Export 📥",data=to_excel(st.session_state['positive_table_styled']) ,file_name=selected_sheet_name+'Top {} Positives.xlsx'.format(st.session_state["top_se"]))
                                
                        
                    nege_neu_col=st.columns((1,1))

                    n_chart=nege_neu_col[0].radio( " ",options=('Bar', 'Pie','Table'), horizontal=True,key="Negetives_charts")
                    if n_chart=='Bar':
                        nege_neu_col[0].plotly_chart(st.session_state['negetive_bar'], use_container_width=True)

                    elif n_chart=='Pie':
                        nege_neu_col[0].plotly_chart(st.session_state['negetive_pie'], use_container_width=True)                                    
                        
                    elif n_chart=='Table':
                        nege_neu_col[0]._legacy_dataframe(st.session_state['negetive_table_styled'])
                        nege_neu_col[0].download_button("Export📥",data=to_excel(st.session_state['negetive_table_styled']) ,file_name=selected_sheet_name+'Top {} Negetives.xlsx'.format(st.session_state["top_se"]))


                    nu_chart=nege_neu_col[1].radio( " ",options=('Bar', 'Pie','Table'), horizontal=True,key="Neutrals_charts")
                    if nu_chart=='Bar':
                        nege_neu_col[1].plotly_chart(st.session_state['neutral_bar'], use_container_width=True)

                    elif nu_chart=='Pie':
                        nege_neu_col[1].plotly_chart(st.session_state['neutral_pie'], use_container_width=True)   
                        
                    elif nu_chart=='Table':
                        nege_neu_col[1]._legacy_dataframe(st.session_state['neutral_table_styled'])
                        nege_neu_col[1].download_button("Export📥",data=to_excel(st.session_state['neutral_table_styled']) ,file_name=selected_sheet_name+'Top {} Neutrals.xlsx'.format(st.session_state["top_se"]))


                with sv_tab2:
                    rp_col=st.columns((3,9,3))
                    rp_col[0].subheader("Report")
                    #st.plotly_chart(st.session_state['report_chart'], use_container_width=True)

                    exp_col=st.columns((1,1,1,1,1))
                    exp_col[2].markdown("##### Analysis Report")
                    st._legacy_dataframe(st.session_state['analysis_report'],height=500)

                    st.write(" ")
                    st.write(" ")
                    exp_col2=st.columns((1,1,1,1,1))

                    exp_col2[2].markdown("##### Total weightage")
                    st._legacy_dataframe(st.session_state['total_weightage_report'],height=500)
                    
                    #sentiment
                    exp_col3=st.columns((1,1,1,1,1))
                    exp_col3[2].markdown("##### Sentiment Analysis")
                    st._legacy_dataframe(st.session_state['sentiment_report'])
                    

                    with rp_col[2].expander("Export"):
                        st.download_button("CSV", data= st.session_state['sorted_weightage_result_df'].to_csv(), file_name=selected_sheet_name+'weightage_report.csv')  
                        st.download_button("Excel", data=new_to_excel(st.session_state['analysis_report'],st.session_state['total_weightage_report'],st.session_state['sentiment_report']), file_name=selected_sheet_name+'Analysis_report.xlsx')
    
else:
    st.error("You did'nt have uploaded any sheet.")
    st.info("Please upload sheet!!")

