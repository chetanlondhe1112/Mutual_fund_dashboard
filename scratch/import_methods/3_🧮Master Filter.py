import pandas as pd 
import numpy as np     
import matplotlib.pyplot as plt                                         # to read the uploaded csv
import streamlit as st
import plotly.express as px
import time
from datetime import datetime
from Home import filter_table
from Home import master_table
from Home import sentiment_table

from Home import init_msconnection
from Home import init_sqconnection
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
import plotly.graph_objects as go
import seaborn as sns
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
#st.set_page_config(
#        page_title="Dashboard",
#        page_icon="chart",
#        layout="wide",
#        initial_sidebar_state="collapsed",
#    )


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

def pagination(df):
    
    _="""
        ## 📑 Pagination
        
        Too much data to display? Now you can paginate through items (e.g. a table), 
        storing the current page number in `st.session_state`. 
    """
    
    if "page" not in st.session_state:
        st.session_state.page = 0

    def next_page():
        st.session_state.page += 1

    def prev_page():
        st.session_state.page -= 1


    col0,col1, col2, col3, _ = st.columns((0.7,0.1, 0.17, 0.1, 0.63))

    max_page=len(df)/10


    if st.session_state.page < max_page:
        col3.button(">", on_click=next_page)
    else:
        col3.write("")  # this makes the empty column show up on mobile

    if st.session_state.page > 0:
        col1.button("<", on_click=prev_page)
    else:
        col1.write("")  # this makes the empty column show up on mobile

    #col2.write(f"Page {1+st.session_state.page} of {max_page+0.1}")
    col2.write(f"Page {1+st.session_state.page}")
    start = 10 * st.session_state.page
    end = start + 10
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
    color = 'background-color: red;'
  elif value > 0:
    color = 'background-color: green;'
  else:
    color = 'white'

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
    color = 'background-color: white;'

  #return 'color: %s' % color
  return color

def cell_color2(value):
  """
  Colors elements in a dateframe
  green if positive and red if
  negative. Does not color NaN
  values.
  """

  if value == 'Positive':
    color = 'background-color: #6bf556;'
  elif value =='Negative':
    color = 'background-color: #f75631;'
  else:
    color = 'background-color: white;'

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
   
def wightage_dataframe(user,connection,master_sheet_table_name,master_table,master_table_date,filter_table_name,filter_table,filter_date):

        parameters_list = list(filter_table['parameter_name'])                            # fetched parameters list
        # creating temporary dataframe from master sheetr
        df_master_weightage_table = master_table[['Name','BSE_Code','NSE_Code','Industry']]
        # creating temporary weightage table to attach to above dataframe for each iteration
        df_weightage_table = pd.DataFrame()

        for parameter in parameters_list:
            parameter_condition_q = "SELECT * FROM `"+filter_table_name+"` WHERE parameter_name='"+parameter+"' and" \
                                            " user='"+user+"' and date_time='" + str(filter_date) + "'"

            df_parameter_condition=pd.read_sql_query(parameter_condition_q,connection)
            if df_parameter_condition.value1[0] and df_parameter_condition.value2[0]:
                weightage_q = "SELECT IF("+parameter+df_parameter_condition.condition1[0] + \
                                        str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                        + "," + " IF("+parameter+ df_parameter_condition.condition2[0] +\
                                        str(df_parameter_condition.value2[0]) + "," + str(df_parameter_condition.result2[0])\
                                        + ",0)) AS "+parameter+"_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"

                df_weightage=pd.read_sql_query(weightage_q,connection)
                df_master_weightage_table[parameter]=df_weightage
                df_weightage_table[parameter] = df_weightage

            elif df_parameter_condition.value1[0]:
                weightage_q = "SELECT IF(" + parameter + df_parameter_condition.condition1[0] + \
                                          str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                          + ",0) AS " + parameter + "_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"
                df_weightage = pd.read_sql_query(weightage_q, connection)
                df_master_weightage_table[parameter] = df_weightage
                df_weightage_table[parameter] = df_weightage
                continue

        df_master_weightage_table.insert(4, "Positives", df_weightage_table[df_weightage_table == 1].count(axis=1))
        df_master_weightage_table.insert(5, "Negetives", df_weightage_table[df_weightage_table == -1].count(axis=1))
        df_master_weightage_table.insert(6, "Neutrals", df_weightage_table[df_weightage_table == 0].count(axis=1))
        df_master_weightage_table.insert(7, "Resultants", df_master_weightage_table.apply(lambda x: x['Positives']+x['Neutrals']-x['Negetives'], axis=1))
       
        return df_master_weightage_table


def admin_wightage_dataframe(user,connection,master_sheet_table_name,master_table,master_table_date,filter_table_name,filter_table,filter_date):

        parameters_list = list(filter_table['parameter_name'])                            # fetched parameters list
        # creating temporary dataframe from master sheetr
        df_master_weightage_table = master_table[['Name','BSE_Code','NSE_Code','Industry']]
        # creating temporary weightage table to attach to above dataframe for each iteration
        df_weightage_table = pd.DataFrame()

        for parameter in parameters_list:
            parameter_condition_q = "SELECT * FROM `"+filter_table_name+"` WHERE parameter_name='"+parameter+"' and" \
                                            " user='"+user+"' and date_time='" + str(filter_date) + "'"

            df_parameter_condition=pd.read_sql_query(parameter_condition_q,connection)
            if df_parameter_condition.value1[0] and df_parameter_condition.value2[0]:
                weightage_q = "SELECT IF("+parameter+df_parameter_condition.condition1[0] + \
                                        str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                        + "," + " IF("+parameter+ df_parameter_condition.condition2[0] +\
                                        str(df_parameter_condition.value2[0]) + "," + str(df_parameter_condition.result2[0])\
                                        + ",0)) AS "+parameter+"_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"

                df_weightage=pd.read_sql_query(weightage_q,connection)
                df_master_weightage_table[parameter]=df_weightage
                df_weightage_table[parameter] = df_weightage

            elif df_parameter_condition.value1[0]:
                weightage_q = "SELECT IF(" + parameter + df_parameter_condition.condition1[0] + \
                                          str(df_parameter_condition.value1[0]) + "," + str(df_parameter_condition.result1[0]) \
                                          + ",0) AS " + parameter + "_w FROM "+master_sheet_table_name+" WHERE username='"+user+"' and date_time='" + str(master_table_date) + "'"
                df_weightage = pd.read_sql_query(weightage_q, connection)
                df_master_weightage_table[parameter] = df_weightage
                df_weightage_table[parameter] = df_weightage
                continue

        df_master_weightage_table.insert(4, "Positives", df_weightage_table[df_weightage_table == 1].count(axis=1))
        df_master_weightage_table.insert(5, "Negetives", df_weightage_table[df_weightage_table == -1].count(axis=1))
        df_master_weightage_table.insert(6, "Neutrals", df_weightage_table[df_weightage_table == 0].count(axis=1))
        df_master_weightage_table.insert(7, "Resultants", df_master_weightage_table.apply(lambda x: x['Positives']+x['Neutrals']-x['Negetives'], axis=1))
       
        return df_master_weightage_table
#@st.cache(suppress_st_warning=True,show_spinner=False)
def user_sheets_names(username):
    n = "SELECT sheet_name FROM " + master_table + " WHERE username='" + username + "'"
    df_user_master_table_names = pd.read_sql_query(n, conn)
    user_master_table_names = df_user_master_table_names.drop_duplicates()
    return user_master_table_names

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

with open('CSS/master_filter.css') as f:
    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)

if len(st.session_state)==0:
    st.warning("Please! Don't Refresh your browser.")
    with st.expander('Do This:'):
        video_file = open('video/dashboard_refresh.mp4', 'rb')
        video_bytes = video_file.read()
        st.video(video_bytes)
    st.info("Always, use dashboard Default Refresh!-Press 'R'")
    st.stop()

try:
    conn_1 = init_msconnection()
    conn_1.close()
except:
    st.warning("Connection lost!,Please your database connection.")
    st.stop()

conn = st.session_state["conn"]

if st.session_state["authentication_status"]:
    current_date=datetime.now()
    private_columns=['date_time','lable','username']
    ignore_columns_parameter_list=['Name','BSE_Code','NSE_Code','Industry']
    sq_conn = init_sqconnection()
    ## Instantiating cursor
    #conn=connect()
    cur = conn.cursor()

    
    user=st.session_state["username"]
    label=st.session_state["lable"]
   
    _="""
        Master Sheet Selection
    """
    # Date selection for sheet
    ##        SELECT date_time FROM master_sheet_table WHERE username='chetan';
    #q = "SELECT date_time FROM "+master_table+" WHERE username='"+st.session_state["username"]+"'"
    #df_master_sheets_dates= pd.read_sql_query(q, conn)
    #master_sheets_dates=df_master_sheets_dates.drop_duplicates()

    #n = "SELECT sheet_name FROM " + master_table + " WHERE username='" + st.session_state["username"] + "'"
    #df_user_master_table_names = pd.read_sql_query(n, conn)
    #user_master_table_names = df_user_master_table_names.drop_duplicates()

    user_master_table_names=user_sheets_names(st.session_state["username"])

    if len(user_master_table_names)!=0:
        mf_col1,mf_col2=st.columns(2)
        user_master_table_name = mf_col2.selectbox("Select your saved sheets", options=user_master_table_names)

        v = "SELECT * FROM " + master_table + " WHERE username='" + st.session_state["username"] + "' and sheet_name='" +user_master_table_name+ "'"
        
        df_user_master_table = pd.read_sql_query(v, conn)
        df_user_master_table=df_user_master_table.dropna(axis=1,how='all')
        master_table_date=df_user_master_table['date_time'].iloc[0]

        #master_table_date=mf_col2.selectbox("Select your sheet",options=master_sheets_dates)

        # Slecting all data from Master table
        ##        SELECT * FROM master_sheet_table WHERE username='chetan' and date_time='2022-07-26 16:34:56'
        q = "SELECT * FROM "+master_table+" WHERE username='"+st.session_state["username"]+"' and date_time='"+str(master_table_date)+"'"
        df_master_table = pd.read_sql_query(q, conn)
        df_master_table=df_master_table.dropna(axis=1,how='all')


        # user and selected date query
        priority_q = "username='"+st.session_state["username"]+"' and date_time='"+str(master_table_date)+"' and sheet_name='"+user_master_table_name+"'"  

        # columns extraction
        columns=[]
        for col in df_master_table.columns:
            columns.append(col)
            #print(columns)
        df_col=pd.DataFrame(columns)

        parameters_list = columns[8:]
        rem_us_parameter_list = rem_under_scr_col_list(parameters_list)
        
        if 'delete_clicked2' not in st.session_state:
            st.session_state.delete_clicked2 = False
        
        def del_callback2():
            st.session_state.delete_clicked2 = True

        if 'save_as' not in st.session_state:
            st.session_state.save_as = False
        
        def save_as_callback():
            st.session_state.save_as = True

        #---Dashboard

        mf_col1.title("🧮Master Filter")
        
        st.subheader(user_master_table_name)
        with st.expander("Show my sheet"):
            
            st.write("{} results found:".format(len(df_master_table)))
            show_df=df_master_table.drop(["date_time","lable","username",'sheet_name'],axis=1)
            #show_df.index=show_df['Name']

            x=list(range(1, len(show_df)+1))
            show_df.set_index([pd.Index(x), 'Name'],inplace = True)
            list_c=show_df.columns.to_list()
            st.table(pagination(show_df).style.set_table_styles(styles2).applymap(color_negative_red,subset=list_c[3:]).highlight_null(null_color="black"))   
            #st.dataframe(show_df.style.apply(styles2))   


        #st.markdown("---")
        tab1,tab2,tab3=st.tabs(["Saved Filter","Create Filter","Update Filter"])
        #with tab3:
            #st.info("Filter removed")
        
        with tab2:
            fil_col=st.columns((4,10,4))
   
            dmf_q="SELECT name FROM "+filter_table+" WHERE user='"+st.session_state["username"]+"'"
            cur.execute(dmf_q)
            dem_f=cur.fetchall()
            #st.write(dem_f)
            dem_f_df=pd.read_sql_query(dmf_q,conn)
            #st.write(list(dem_f_df.drop_duplicates()['name']))
            if fil_col[2].button("📤Upload Demo Filter"):
                
                fil_name="Small-Case-Parameters(Demo filter)"
                if fil_name in list(dem_f_df.drop_duplicates()['name']):
                    st.error("Demo Filter already uploaded!")
                    time.sleep(2)
                    st.experimental_rerun()
                else:
                    with st.spinner("Uploading..."):

                        df = pd.read_csv('scratch/testcsv/master_filter.csv',index_col=None)
                        time.sleep(2)
                        df['parameter_name'] = [x.replace(" ", "_") for x in df['parameter_name']]
                        df.insert(0, "date_time", str(current_date))
                        df.insert(1, "lable", st.session_state["lable"])
                        df.insert(2, "user", st.session_state["username"])
                        df.insert(3, "name", fil_name)

                        df.to_sql(filter_table, sq_conn, if_exists='append', index=False)
                        conn.commit()
                        time.sleep(2)
                        st.success("Done")
                        time.sleep(2)
                        st.experimental_rerun()
                
            if 'df_filter' not in st.session_state:
                st.session_state.df_filter = pd.DataFrame({'parameter_name': [], 'condition1': [],
                                                        'value1': [], 'result1': [], 'condition2': [], 'value2': [],
                                                        'result2': []})

            fil_col[0].subheader("Create Filter:")
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
            bu_col=st.columns((1,1,10))
            if bu_col[0].button('AND'):
                selection = {'parameter_name': select_param, 'condition1': operator_1,
                                'value1': value_1, 'result1': result_1, 'condition2': operator_2, 'value2': value_2,
                                'result2': result_2}

                st.session_state.df_filter = st.session_state.df_filter.append(selection, ignore_index=True)
                st.write("Selected Conditions List:")
                st.table(st.session_state.df_filter)
                st.experimental_rerun()
            else:
                if len(st.session_state.df_filter):
                    st.write("Selected Conditions List:")
                    st.table(st.session_state.df_filter)

            if len(st.session_state.df_filter):
                if bu_col[1].button("Clear"):
                    st.session_state.df_filter = st.session_state.df_filter.drop(len(st.session_state.df_filter) - 1)
                    st.experimental_rerun()

                if st.button("Save"):
                    if name=='':
                        st.error("Please,give a name to this filter!")
                        st.stop()
                    fchk_query = "SELECT * FROM "+filter_table+" where name='" + name + "' AND user='" + st.session_state["username"] + "'"
                    cur.execute(fchk_query)
                    values = cur.fetchall()
                    if values:
                        st.error("Sorry,Filter name already used!!")
                    else:
                        with st.spinner('Saving...'):
                            time.sleep(2)
                            st.session_state.df_filter['parameter_name'] = [x.replace(" ", "_") for x in
                                                                                st.session_state.df_filter['parameter_name']]
                            st.session_state.df_filter.insert(0, "date_time", str(current_date))
                            st.session_state.df_filter.insert(1, "lable", st.session_state["lable"])
                            st.session_state.df_filter.insert(2, "user", st.session_state["username"])
                            st.session_state.df_filter.insert(3, "name", name)

                            st.session_state.df_filter.to_sql(filter_table, sq_conn, if_exists='append', index=False)
                            conn.commit()
                            time.sleep(2)
                            st.success("Done")
                            st.session_state.df_filter = st.session_state.df_filter.drop(x for x in range(len(st.session_state.df_filter)))
                            st.session_state.df_filter.drop(st.session_state.df_filter.columns[[0,1,2,3]], axis=1, inplace=True)
                            time.sleep(2)
                            st.experimental_rerun()
        with tab3:
            updf=st.columns((5,5))
            updf[0].subheader("Update Filters:")

            #upf_tab_1,udf_tab_2
            # Show filter selection dropdown
 
            n = "SELECT name FROM " + filter_table + " WHERE user='" + st.session_state["username"] + "'"
            df_user_table_names = pd.read_sql_query(n, conn)
            user_table_names = df_user_table_names.drop_duplicates()
                
            if len(user_table_names)>0:
                #user_table_date = st.selectbox("Select your saved filter", options=user_table_dates.sort_values(by="date_time",ascending=False))
                #v = "SELECT * FROM " + filter_table + " WHERE user='"+st.session_state["username"]+"' and date_time='" + str(user_table_date) + "'"
                #df_user_table = pd.read_sql_query(v, conn)

                user_table_name = updf[1].selectbox("Select your saved filter", options=user_table_names.sort_values(by="name",ascending=False),key="sfgh")
                nv = "SELECT * FROM " + filter_table + " WHERE user='"+st.session_state["username"]+"' and name='" + user_table_name + "'"
                df_user_table = pd.read_sql_query(nv, conn)

                #st._legacy_dataframe(df_user_table.drop([ "lable", "user",'parameter_name','condition1','value1', 'result1','condition2', 'value2',
                #                'result2'], axis=1).drop_duplicates())
                #st.write(df_user_table[['name','date_time']].drop_duplicates())
                filter_table_name=df_user_table[['date_time','name']].iloc[0,1]
                #st.write(filter_table_name)
                filter_table_date=df_user_table[['date_time','name']].iloc[0,0]
                user_table_date=filter_table_date
                      
                show_df3 = df_user_table.drop(["date_time", "lable", "user","name"], axis=1)

                # Update form
                #with st.form("Update Form"):
                test_df=show_df3    
                # Data collection from Dataframe for update from
                Options=show_df3['parameter_name']
                conditions=['>', '<', '=']
                results=[-1, 1]

                #Values
                #show_df3.set_index('parameter_name',inplace=True)

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
                        #ignore_index=True    
                        st.session_state.temp_df=st.session_state.temp_df.append({"parameter_name":select_param2,"condition1":operator_1,"value1":value_1,"result1":result_1,"condition2":operator_2,"value2":value_2,"result2":result_2}, ignore_index=True)
                        test_df.index = np.arange(1, len(test_df) + 1)
                        #st.table(test_df.style.set_table_styles(styles).apply(lambda x: ['background: yellow' if x.name==ind[0][0]+1 else'' for i in x],axis=1))   
                        #st.experimental_rerun()
                        st.session_state.temp_df.index = np.arange(1, len(st.session_state.temp_df) + 1)

                        for i in range(len(st.session_state.temp_df)):
                            #st.write(st.session_state.temp_df.iloc[[i]])
                            upd_df=st.session_state.temp_df.iloc[[i]]

                            operator__1=str(upd_df['condition1'][i+1])
                            operator__2=str(upd_df['condition2'][i+1])
                            value__1=float(upd_df['value1'][i+1])
                            value__2=float(upd_df['value2'][i+1])
                            result__1=int(upd_df['result1'][i+1])
                            result__2=int(upd_df['result2'][i+1])
                            
                            update_q="UPDATE "+filter_table+" SET condition1 = '"+str(operator__1)+"', value1 = "+str(value__1)+", result1 = "+str(result__1)+", condition2 = '"+str(operator__2)+"', value2 = "+str(value__2)+", result2 = "+str(result__2)+" WHERE user='"+st.session_state["username"]+"' and name='"+str(filter_table_name)+"' and date_time='"+str(user_table_date)+"' and parameter_name='"+select_param2+"'"
                            
                            #'fmlxgnx,;v.b x[lmkn kpZ<G
                            cur.execute(update_q)
                            conn.commit()
                            st.success("Filter Update")
                            st.session_state.temp_df.drop(st.session_state.temp_df.index, inplace=True)
                            time.sleep(2)
                            st.experimental_rerun()

                    if updf_bcol[2].button("Delete",key="opacdsi"):
                        #DELETE FROM master_filter WHERE date_time = "2022-09-05 06:46:14" and `user`="chetan" and `name`="new" and `parameter_name`="EPS"
                        del_row = "DELETE FROM " + filter_table +" WHERE date_time='"+str(user_table_date)+"' and user='"+st.session_state["username"]+"' and name='"+str(filter_table_name)+"' and parameter_name='"+select_param2+"'"
                        cur.execute(del_row)
                        st.experimental_rerun()


                with st.expander("Merge Filter"):
                    fi_info=st.columns((1,1))
                    user_table_name_m=fi_info[1].selectbox("Select filter to merge",options=user_table_names.sort_values(by="name",ascending=False))
                    nv = "SELECT * FROM " + filter_table + " WHERE user='"+st.session_state["username"]+"' and name='" + user_table_name_m + "'"
                    df_user_table = pd.read_sql_query(nv, conn)
                    filter_table_name_m=df_user_table[['date_time','name']].iloc[0,1]
                    filter_table_date_m=df_user_table[['date_time','name']].iloc[0,0]
                    #user_table_date=filter_table_date
                    fi_info2=st.columns((1,2,1))
                    fi_info[0].markdown("##### {}".format(filter_table_name_m))
                    fi_info2[2].write("Created: {}".format(filter_table_date_m))         
                    #show_df3 = df_user_table.drop(["date_time", "lable", "user","name"], axis=1)
                    show_df4 = df_user_table.drop(["date_time", "lable", "user","name"], axis=1)
                    show_df4_ind=list(range(1, len(show_df4)+1))
                    show_df4.set_index([pd.Index(show_df4_ind)],inplace = True)
                    fi_info2[0].write("{} Condition's".format(len(show_df4)))

                    #fi_info2[2].write("Updated: {}".format(filter_table_date_m))
                    #st._legacy_dataframe(show_df4,height=145)
                    st.table(show_df4.style.set_table_styles(styles))

                    test_df1=show_df4

                    #st.write(st.session_state.df_filter)

                    #df_date=st.session_state.df_filter[['date_time','name']].iloc[0,1]
                    same_param=set(test_df['parameter_name']).intersection(test_df1['parameter_name'])
                    #st.write(same_param)
                    if st.button("Merge"):

                        if same_param:
                            st.warning("Conditions already present:{}".format(same_param))
                        else:
                            same_param=set(test_df['parameter_name']).difference(test_df1['parameter_name'])
                            #st.write(same_param)
                            test_df1.insert(0, "date_time", str(filter_table_date))
                            test_df1.insert(1, "lable", st.session_state["lable"])
                            test_df1.insert(2, "user", st.session_state["username"])
                            test_df1.insert(3, "name", filter_table_name)
                            test_df1.to_sql(filter_table, sq_conn, if_exists='append', index=False)
                            conn.commit()
                            # st.write(test_df1)
                            st.experimental_rerun()
                
                test_df.index = np.arange(1, len(test_df) + 1)
                #test_df.insert(loc = 0,column = 'PPA',value = <input type="checkbox" />)
                fi_info=st.columns((10,10,7))
                fi_info[0].markdown("##### {}".format(filter_table_name))
                fi_info[2].write("Created: {}".format(filter_table_date))         
                show_df3 = df_user_table.drop(["date_time", "lable", "user","name"], axis=1)
                fi_info[0].write("{} Condition's".format(len(show_df3)))
                st.table(test_df.style.set_table_styles(styles).apply(lambda x: ['background: yellow' if x.name==ind[0][0]+1 else'' for i in x],axis=1))   
                #if (st.button("Delete", help="Delete your Query",key='query_delete',on_click=del_callback) or st.session_state.delete_clicked):

                _="""
                if (st.button("Save As",on_click=save_as_callback) or st.session_state.save_as):
                    with st.form("Save As"):
                        name= st.text_input("Name your Filter")
                        sv_col=st.columns((1,1,10))
                        if sv_col[0].form_submit_button("Save") and len(name)>0:
                            st.write("del")
                            
                        sv_col[1].form_submit_button("No")
                """

            else:
                st.error("Sorry, you didn't have created any filters!")


        with tab1:

                st.subheader("My Filters:")
                w = "SELECT date_time FROM " + filter_table + " WHERE user='" + st.session_state["username"] + "'"
                df_user_table_dates = pd.read_sql_query(w, conn)
                user_table_dates = df_user_table_dates.drop_duplicates()

                n = "SELECT name FROM " + filter_table + " WHERE user='" + st.session_state["username"] + "'"
                df_user_table_names = pd.read_sql_query(n, conn)
                user_table_names = df_user_table_names.drop_duplicates()
                
                if len(user_table_dates)>0:
                    #user_table_date = st.selectbox("Select your saved filter", options=user_table_dates.sort_values(by="date_time",ascending=False))
                    #v = "SELECT * FROM " + filter_table + " WHERE user='"+st.session_state["username"]+"' and date_time='" + str(user_table_date) + "'"
                    #df_user_table = pd.read_sql_query(v, conn)

                    user_table_name = st.selectbox("Select your saved filter", options=user_table_names.sort_values(by="name",ascending=False))
                    nv = "SELECT * FROM " + filter_table + " WHERE user='"+st.session_state["username"]+"' and name='" + user_table_name + "'"
                    df_user_table = pd.read_sql_query(nv, conn)

                    #st._legacy_dataframe(df_user_table.drop([ "lable", "user",'parameter_name','condition1','value1', 'result1','condition2', 'value2',
                    #                'result2'], axis=1).drop_duplicates())
                    #st.write(df_user_table[['name','date_time']].drop_duplicates())
                    filter_table_name=df_user_table[['date_time','name']].iloc[0,1]
                    filter_table_date=df_user_table[['date_time','name']].iloc[0,0]
                    user_table_date=filter_table_date
                    with st.expander("Show Filter"):
                        del_col=st.columns((10,10,2))
                        if (del_col[2].button("Delete",on_click=del_callback2) or st.session_state.delete_clicked2):
                            with st.form(" Are you Sure?"):
                                st.write("Are you Sure ?")
                                del_o_col=st.columns((1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1))
                                if del_o_col[0].form_submit_button("Yes"):
                                    with st.spinner('Deleting...'):
                                        time.sleep(2)
                                        l = "DELETE FROM " + filter_table + " WHERE user='" + st.session_state["username"] + "' and date_time='" + str(user_table_date) + "'"
                                        cur.execute(l)
                                        conn.commit()
                                        st.success("Filter Deleted")
                                        time.sleep(2)
                                        st.session_state.delete_clicked2=False
                                        st.experimental_rerun()
                                elif del_o_col[1].form_submit_button("No"):
                                    st.session_state.delete_clicked2=False
                                    st.experimental_rerun()
                    
                            #st.experimental_rerun()
                        
                        del_col[0].markdown("#### {}".format(filter_table_name))
                        st.write(filter_table_date)
                        show_df3 = df_user_table.drop(["date_time", "lable", "user","name"], axis=1)
                        show_df3_ind=list(range(1, len(show_df3)+1))
                        show_df3.set_index([pd.Index(show_df3_ind)],inplace = True)
                        #st._legacy_dataframe(show_df3)
                        st.table(show_df3.style.set_table_styles(styles))   

                        #gd=GridOptionsBuilder.from_dataframe(show_df3)
                        #gd.configure_pagination(enabled=True,paginationPageSize=10)
                        #gd.configure_default_column(editable=False,groupable=True)
                        #gridoptions=gd.build()
                        #grid_table=AgGrid(show_df3,gridOptions=gridoptions,theme='material') 
                    if "top_se" not in st.session_state:
                        st.session_state["top_se"]=20
                    
                    if st.checkbox("Apply", key='user_apply'):
                        #f_v=filter_validate(df_user_table,df_master_table)
                        #if f_v==False:
                        #    st.error("Sorry, invalid application!")
                        #    st.error("Sheet must have to include all the parameters of your filter!")
                        #elif f_v==True:
                            #with st.spinner('applying...'):
                            #time.sleep(1)                 
                            df_master_weightage_table = wightage_dataframe(st.session_state["username"], conn, master_table, df_master_table, master_table_date,
                                                                                filter_table, df_user_table, user_table_date)
                            df_master_weightage_table_sorted = df_master_weightage_table.sort_values(by='Resultants', ascending=False,
                                                                                                            ignore_index=True)
                            st.session_state["we_table"]=df_master_weightage_table_sorted
                            # st.write(df_master_weightage_table_sorted)
                            csv = df_master_weightage_table_sorted.to_csv()
                            #time.sleep(1)
                            #st.success("Done")
                            #st.experimental_rerun()
                            if "pos_min" not in st.session_state:
                                st.session_state["pos_min"]=df_master_weightage_table_sorted['Positives'].min()
                            if "pos_max" not in st.session_state:
                                st.session_state["pos_max"]=df_master_weightage_table_sorted['Positives'].max()    
                            if  len(st.session_state["we_table"]):
                                sv_tab1,sv_tab2=st.tabs(['Dashboard','Report'])
                                
                                with sv_tab1:
                                    ex_col=st.columns((3,10,3))

                                    ex_col[0].subheader("Dashboard")
                                    
                                    if ex_col[2].checkbox("Create Dashboard"):
                                        with st.expander("Create Dashboard"):
                                            #st.info("Features under development")
                                            _="""              
                                            ind_sel_col=st.columns((1,35))
                                            ind_sel_col[0].checkbox("")
                                            industry_n = ind_sel_col[1].selectbox("Select sector", options=df_master_weightage_table_sorted[
                                                    'Industry'].drop_duplicates())
                                            f_col=st.columns((1,2))
                                            re_list=df_master_weightage_table_sorted['Resultants'].drop_duplicates()
                                            sl_resultant_max=f_col[1].select_slider("select", options=re_list, valu)

                                            s_resultant = f_col[0].number_input("enter number", step=1,
                                                                        max_value=df_master_weightage_table_sorted[
                                                                            'Resultants'].max(),
                                                                        min_value=df_master_weightage_table_sorted[
                                                                            'Resultants'].min(), value=sl_resultant_max
                                                                        )
                                            """
                                            st.write("Positive Ranging")
                                            pos_col=st.columns((5,5))
                                            pos_min=pos_col[0].number_input("minimum",step=1,min_value=df_master_weightage_table_sorted['Positives'].min(),value=df_master_weightage_table_sorted['Positives'].min())
                                            
                                            pos_max=pos_col[1].number_input("maximum",step=1,max_value=df_master_weightage_table_sorted['Positives'].max(),value=df_master_weightage_table_sorted['Positives'].max())

                                            se_top = st.number_input("Select range", step=1,
                                                                    max_value=len(df_master_weightage_table_sorted),value=5)
                                            chart_show=st.button("apply")
                                            if chart_show:
                                                if pos_max>pos_min:
                                                    with st.spinner("loading filter"):
                                                        time.sleep(2)
                                                        st.session_state["top_se"]=se_top
                                                        st.session_state["pos_min"]=pos_min
                                                        st.session_state["pos_max"]=pos_max
                                                        st.experimental_rerun()
                                                        st.expander(collapse)
                                                else:
                                                    st.error("Maximum value should be greater than minimum!")
                                    else:
                                        
                                        st.session_state["top_se"]=20
                                        st.session_state["pos_max"]=df_master_weightage_table_sorted['Positives'].max()    
                                        st.session_state["pos_min"]=df_master_weightage_table_sorted['Positives'].max()/2    

                                    #with st.form("Charts"):
                                    pos_minq=st.session_state["pos_min"]
                                    pos_maxq=st.session_state["pos_max"]
                                    sh_chart=df_master_weightage_table_sorted[df_master_weightage_table_sorted.Positives >= pos_minq ]
                                    sh2_chart=sh_chart[sh_chart.Positives <= pos_maxq ]
                                    #st.write(sh2_chart.sort_values(by='Positives', ascending=False,ignore_index=True))

                                    #Positives_ranging
                                    post_col=st.columns((2,5,1))
                                    post_col[1].markdown("##### POSITIVE RANGING FROM {}-{} (TOTAL {} NOS)".format(pos_minq,pos_maxq,len(sh2_chart)))

                                    Positives_ranging=px.bar(sh2_chart.sort_values(by='Positives', ascending=False,ignore_index=True), x='NSE_Code', y=['Positives', 'Negetives'],
                                                    text_auto='.2s',barmode='group',template='none')
                                    Positives_ranging.update_layout(legend=dict(orientation="h"))
                                    Positives_ranging.update_xaxes(rangeslider_visible=True,)
                                    Positives_ranging.update_xaxes(title_text='')
                                    Positives_ranging.update_yaxes(title_text='')
                                    Positives_ranging.update_xaxes(title_font=dict(size=2, family='Courier', color='crimson'))
                                    Positives_ranging.update_xaxes( tickfont=dict(family='Rockwell', color='black', size=13))
                                    Positives_ranging.update_yaxes( tickfont=dict(family='Rockwell', color='black', size=11))
                                    Positives_ranging.update_layout(coloraxis_showscale=False)
                                    Positives_ranging.update_xaxes(ticklen=0)
                                    Positives_ranging.update_yaxes(ticklen=0)
                                    st.plotly_chart(Positives_ranging, use_container_width=True)


                                    # Resultant charts
                                    se_top_1=st.session_state["top_se"]

                                    Resultants_head=pd.DataFrame(df_master_weightage_table_sorted.head(se_top_1))
                                    Resultants_csv=Resultants_head.to_csv(index=False)

                                    c_col=st.columns((8,3,9,7))

                                    c_col[0].markdown("##### Top {} Companies by Resultants".format(se_top_1))

                                    r_chart=c_col[3].radio( " ",options=('Bar', 'Pie','Table'), horizontal=True)
                                    #resl_fig_col=st.columns((1,1))
                                    #BAR CHART
                                    if r_chart=='Bar':
                                        Resultants_bar = px.bar(Resultants_head, x='NSE_Code', y=['Positives', 'Negetives','Neutrals', 'Resultants'],
                                                        barmode='group',template='simple_white')
                                        #fig2.update_traces( textangle=0, textposition="outside", cliponaxis=False)
                                        Resultants_bar.update_layout(legend=dict(orientation="v"))
                                        Resultants_bar.update_xaxes(title_text='')
                                        Resultants_bar.update_yaxes(title_text='')
                                        Resultants_bar.update_xaxes(title_font=dict(size=2, family='Courier', color='crimson'))
                                        Resultants_bar.update_xaxes( tickfont=dict(family='Rockwell', color='black', size=13))
                                        Resultants_bar.update_yaxes( tickfont=dict(family='Rockwell', color='black', size=11))
                                        Resultants_bar.update_layout(coloraxis_showscale=False)
                                        Resultants_bar.update_xaxes(ticklen=0)
                                        Resultants_bar.update_yaxes(ticklen=0)
                                        #fig2.update_xaxes(rangeslider_visible=True)
                                        st.plotly_chart(Resultants_bar, use_container_width=True)

                                    elif r_chart=='Pie':

                                        Resultants_pie=px.pie(Resultants_head,names='NSE_Code',values='Resultants',hover_name='Industry',template='none',hole=0.4,height=450,width=600)
                                        Resultants_pie.update_traces(showlegend=False,pull = [0.25])
                                        #fig3 = px.pie(df_master_weightage_table_sorted.head(se_top_1), names='NSE_Code', hole=0.7, template='none',height=400,width=600)
                                        Resultants_pie.update_traces(hovertemplate=None,textinfo='percent+label', textposition='outside', rotation=50)
                                        #fig3.update_layout(margin=dict(t=50, b=35, l=0, r=0), showlegend=True,
                                        #                        plot_bgcolor='#fafafa', paper_bgcolor='#fafafa',
                                        #                       font=dict(size=17, color='#8a8d93'),
                                        #                        hoverlabel=dict(bgcolor="#444", font_size=13, font_family="Lato, sans-serif"))
                                        Resultants_pie.add_annotation(dict(x=0.5, y=0.5,  align='center',
                                                                xref = "paper", yref = "paper",
                                                                showarrow = False, font_size=22,
                                                                text="Resultants"))
                                        Resultants_pie.update_layout(legend=dict(
                                                                        orientation="h"))
                                        st.plotly_chart(Resultants_pie, use_container_width=True)

                                    elif r_chart=='Table':
                                        Resultants_head_styled=Resultants_head.style.applymap(cell_color).set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['Name'])\
                                                .set_properties(**{'background-color': '#d67d09','color': 'White','font-weight':'bold'}, subset=['Resultants'])\
                                                .set_properties(**{'background-color': '#229106','color': 'White','font-weight':'bold'}, subset=['Positives'])\
                                                .set_properties(**{'background-color': '#c70404','color': 'White','font-weight':'bold'}, subset=['Negetives'])\
                                                .set_properties(**{'background-color': '#b0ad04','color': 'White','font-weight':'bold'}, subset=['Neutrals'])
                                        st._legacy_dataframe(Resultants_head_styled)
                                        c_col[1].download_button("Export📥",data=to_excel(Resultants_head_styled) ,file_name='Top Resultants.xlsx')
                                                #st.markdown("""---""")

                                    
                                    #Positives Charts:[['NSE_Code','Positives']]
                                    positive_head=df_master_weightage_table_sorted.sort_values(by='Positives', ascending=True).tail(se_top_1)
                                    #positive_head=df_master_weightage_table_sorted.sort_values(by='Positives', ascending=True).nlargest(se_top_1, 'Positives', keep='all')
                                    positives_head_df=df_master_weightage_table_sorted.sort_values(by='Positives', ascending=False).head(se_top_1)
                                    positives_head_df_index=list(range(1, len(positives_head_df)+1))
                                    positives_head_df.set_index([pd.Index(positives_head_df_index)],inplace = True)
                                    positives_col=st.columns((8,3,9,7))
                                    positives_col[0].markdown("##### Top {} Companies by Positives".format(se_top_1))
                                    #positives_col[1].download_button("📥",data=Positives_csv ,file_name='Top Positives.csv')
                                    p_chart=positives_col[3].radio( " ",options=('Bar', 'Pie','Table'), horizontal=True,key="Positives_charts")
                                    
                                    if p_chart=='Bar':
                                        positive_bar = px.bar(positive_head, y='NSE_Code', x='Positives',orientation='h',template='simple_white',color='Positives')
                                        positive_bar.update_xaxes(title_text='')
                                        positive_bar.update_yaxes(title_text='')
                                        positive_bar.update_xaxes(title_font=dict(size=2, family='Courier', color='crimson'))
                                        positive_bar.update_xaxes( tickfont=dict(family='Rockwell', color='black', size=13))
                                        positive_bar.update_yaxes( tickfont=dict(family='Rockwell', color='black', size=11))
                                        positive_bar.update_layout(coloraxis_showscale=False)
                                        positive_bar.update_xaxes(ticklen=0)
                                        positive_bar.update_yaxes(ticklen=0)
                                        #fig2.update_layout(legend=dict(orientation="h"))
                                        #fig2.update_xaxes(rangeslider_visible=True,)
                                        #Positives_csv=positive_df.tail(se_top_1).to_csv()
                                        #dc_col[0].markdown("##### Top {} Companies by Positives".format(se_top_1))
                                        #dc_col[0].plotly_chart(fig1, use_container_width=True)
                                        st.plotly_chart(positive_bar, use_container_width=True)

                                    elif p_chart=='Pie':
                                        Positives_pie=px.pie(positives_head_df,names='NSE_Code',values='Positives',hover_name='Industry',template='none',hole=0.4,height=450,width=600)
                                        Positives_pie.update_traces(showlegend=False,pull = [0.25])
                                        #fig3 = px.pie(df_master_weightage_table_sorted.head(se_top_1), names='NSE_Code', hole=0.7, template='none',height=400,width=600)
                                        Positives_pie.update_traces(hovertemplate=None,textinfo='percent+label', textposition='outside', rotation=50)
                                        #fig3.update_layout(margin=dict(t=50, b=35, l=0, r=0), showlegend=True,
                                        #                        plot_bgcolor='#fafafa', paper_bgcolor='#fafafa',
                                        #                       font=dict(size=17, color='#8a8d93'),
                                        #                        hoverlabel=dict(bgcolor="#444", font_size=13, font_family="Lato, sans-serif"))
                                        Positives_pie.add_annotation(dict(x=0.5, y=0.5,  align='center',
                                                                xref = "paper", yref = "paper",
                                                                showarrow = False, font_size=22,
                                                                text="Positives"))
                                        Positives_pie.update_layout(legend=dict(
                                                                        orientation="h"))
                                        st.plotly_chart(Positives_pie, use_container_width=True)
                                        #st.plotly_chart(fig3, use_container_width=True)
                                    elif p_chart=='Table':
                                        positive_df_styled=positives_head_df.style.applymap(cell_color).set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['Name'])\
                                                .set_properties(**{'background-color': '#d67d09','color': 'White','font-weight':'bold'}, subset=['Resultants'])\
                                                .set_properties(**{'background-color': '#229106','color': 'White','font-weight':'bold'}, subset=['Positives'])\
                                                .set_properties(**{'background-color': '#c70404','color': 'White','font-weight':'bold'}, subset=['Negetives'])\
                                                .set_properties(**{'background-color': '#b0ad04','color': 'White','font-weight':'bold'}, subset=['Neutrals'])
                                        st._legacy_dataframe(positive_df_styled)
                                        positives_col[1].download_button("Export📥",data=to_excel(positive_df_styled) ,file_name='Top Positives.xlsx')
                                              
                                    #st.plotly_chart(fig1, use_container_width=True)
                                    #fig1 = go.Figure(data=[go.Pie(labels=df_master_weightage_table_sorted['NSE_Code'].head(10), values=df_master_weightage_table_sorted['Positives'].head(10), hole=.8,showlegend=False ),])
                                    #fig1.add_trace(go.Pie(name="positives"))
                                    #fig2 = px.bar(df_master_weightage_table_sorted.head(se_top), x='NSE_Code', y=['Positives', 'Negetives','Neutrals', 'Resultants'],
                                    
                                    nege_neu_col=st.columns((1,1))

                                    #Negetives Charts:[['NSE_Code','Positives']]
                                    negetive_head=df_master_weightage_table_sorted.sort_values(by='Negetives', ascending=True).tail(se_top_1)
                                    #positive_head=df_master_weightage_table_sorted.sort_values(by='Positives', ascending=True).nlargest(se_top_1, 'Positives', keep='all')
                                    negetive_head_df=df_master_weightage_table_sorted.sort_values(by='Negetives', ascending=False).head(se_top_1)
                                    negetive_head_df_index=list(range(1, len(negetive_head_df)+1))
                                    negetive_head_df.set_index([pd.Index(negetive_head_df_index)],inplace = True)
                                    #negetive_col=st.columns((8,3,9,7))
                                    nege_neu_col[0].markdown("##### Top {} Companies by Negetives".format(se_top_1))
                                    #positives_col[1].download_button("📥",data=Positives_csv ,file_name='Top Positives.csv')
                                    n_chart=nege_neu_col[0].radio( " ",options=('Bar', 'Pie','Table'), horizontal=True,key="Negetives_charts")
                                    
                                    if n_chart=='Bar':
                                        negetives_bar = px.bar(negetive_head, y='NSE_Code', x='Negetives',orientation='h',template='simple_white',color='Negetives')
                                        negetives_bar.update_xaxes(title_text='')
                                        negetives_bar.update_yaxes(title_text='')
                                        negetives_bar.update_xaxes(title_font=dict(size=2, family='Courier', color='crimson'))
                                        negetives_bar.update_xaxes( tickfont=dict(family='Rockwell', color='black', size=13))
                                        negetives_bar.update_yaxes( tickfont=dict(family='Rockwell', color='black', size=11))
                                        negetives_bar.update_layout(coloraxis_showscale=False)
                                        negetives_bar.update_xaxes(ticklen=0)
                                        negetives_bar.update_yaxes(ticklen=0)
                                        #fig2.update_layout(legend=dict(orientation="h"))
                                        #fig2.update_xaxes(rangeslider_visible=True,)
                                        #Positives_csv=positive_df.tail(se_top_1).to_csv()
                                        #dc_col[0].markdown("##### Top {} Companies by Positives".format(se_top_1))
                                        #dc_col[0].plotly_chart(fig1, use_container_width=True)
                                        nege_neu_col[0].plotly_chart(negetives_bar, use_container_width=True)

                                    elif n_chart=='Pie':
                                        negetives_pie=px.pie(negetive_head_df,names='NSE_Code',values='Negetives',hover_name='Industry',template='none',hole=0.4,height=450,width=600)
                                        negetives_pie.update_traces(showlegend=False,pull = [0.25])
                                        #fig3 = px.pie(df_master_weightage_table_sorted.head(se_top_1), names='NSE_Code', hole=0.7, template='none',height=400,width=600)
                                        negetives_pie.update_traces(hovertemplate=None,textinfo='percent+label', textposition='outside', rotation=50)
                                        #fig3.update_layout(margin=dict(t=50, b=35, l=0, r=0), showlegend=True,
                                        #                        plot_bgcolor='#fafafa', paper_bgcolor='#fafafa',
                                        #                       font=dict(size=17, color='#8a8d93'),
                                        #                        hoverlabel=dict(bgcolor="#444", font_size=13, font_family="Lato, sans-serif"))
                                        negetives_pie.add_annotation(dict(x=0.5, y=0.5,  align='center',
                                                                xref = "paper", yref = "paper",
                                                                showarrow = False, font_size=22,
                                                                text="Negetives"))
                                        negetives_pie.update_layout(legend=dict(
                                                                        orientation="h"))
                                        nege_neu_col[0].plotly_chart(negetives_pie, use_container_width=True)
                                        #st.plotly_chart(fig3, use_container_width=True)
                                    elif n_chart=='Table':
                                        negetive_df_styled=negetive_head_df.style.applymap(cell_color).set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['Name'])\
                                                .set_properties(**{'background-color': '#d67d09','color': 'White','font-weight':'bold'}, subset=['Resultants'])\
                                                .set_properties(**{'background-color': '#229106','color': 'White','font-weight':'bold'}, subset=['Positives'])\
                                                .set_properties(**{'background-color': '#c70404','color': 'White','font-weight':'bold'}, subset=['Negetives'])\
                                                .set_properties(**{'background-color': '#b0ad04','color': 'White','font-weight':'bold'}, subset=['Neutrals'])
                                        nege_neu_col[0]._legacy_dataframe(negetive_df_styled)
                                        nege_neu_col[0].download_button("Export📥",data=to_excel(negetive_df_styled) ,file_name='Top Negetives.xlsx')



                                    #Neutrals Charts:[['NSE_Code','Positives']]
                                    neutrals_head=df_master_weightage_table_sorted.sort_values(by='Neutrals', ascending=True).tail(se_top_1)
                                    #positive_head=df_master_weightage_table_sorted.sort_values(by='Positives', ascending=True).nlargest(se_top_1, 'Positives', keep='all')
                                    neutrals_head_df=df_master_weightage_table_sorted.sort_values(by='Neutrals', ascending=False).head(se_top_1)
                                    neutrals_head_df_index=list(range(1, len(neutrals_head_df)+1))
                                    neutrals_head_df.set_index([pd.Index(neutrals_head_df_index)],inplace = True)
                                    #negetive_col=st.columns((8,3,9,7))
                                    nege_neu_col[1].markdown("##### Top {} Companies by Neutrals".format(se_top_1))
                                    #positives_col[1].download_button("📥",data=Positives_csv ,file_name='Top Positives.csv')
                                    nu_chart=nege_neu_col[1].radio( " ",options=('Bar', 'Pie','Table'), horizontal=True,key="Neutrals_charts")
                                    
                                    if nu_chart=='Bar':
                                        neutrals_bar = px.bar(neutrals_head, y='NSE_Code', x='Neutrals',orientation='h',template='simple_white',color='Neutrals')
                                        neutrals_bar.update_xaxes(title_text='')
                                        neutrals_bar.update_yaxes(title_text='')
                                        neutrals_bar.update_xaxes(title_font=dict(size=2, family='Courier', color='crimson'))
                                        neutrals_bar.update_xaxes( tickfont=dict(family='Rockwell', color='black', size=13))
                                        neutrals_bar.update_yaxes( tickfont=dict(family='Rockwell', color='black', size=11))
                                        neutrals_bar.update_layout(coloraxis_showscale=False)
                                        neutrals_bar.update_xaxes(ticklen=0)
                                        neutrals_bar.update_yaxes(ticklen=0)
                                        #fig2.update_layout(legend=dict(orientation="h"))
                                        #fig2.update_xaxes(rangeslider_visible=True,)
                                        #Positives_csv=positive_df.tail(se_top_1).to_csv()
                                        #dc_col[0].markdown("##### Top {} Companies by Positives".format(se_top_1))
                                        #dc_col[0].plotly_chart(fig1, use_container_width=True)
                                        nege_neu_col[1].plotly_chart(neutrals_bar, use_container_width=True)

                                    elif nu_chart=='Pie':
                                        neutrals_pie=px.pie(neutrals_head_df,names='NSE_Code',values='Neutrals',hover_name='Industry',template='none',hole=0.4,height=450,width=600)
                                        neutrals_pie.update_traces(showlegend=False,pull = [0.25])
                                        #fig3 = px.pie(df_master_weightage_table_sorted.head(se_top_1), names='NSE_Code', hole=0.7, template='none',height=400,width=600)
                                        neutrals_pie.update_traces(hovertemplate=None,textinfo='percent+label', textposition='outside', rotation=50)
                                        #fig3.update_layout(margin=dict(t=50, b=35, l=0, r=0), showlegend=True,
                                        #                        plot_bgcolor='#fafafa', paper_bgcolor='#fafafa',
                                        #                       font=dict(size=17, color='#8a8d93'),
                                        #                        hoverlabel=dict(bgcolor="#444", font_size=13, font_family="Lato, sans-serif"))
                                        neutrals_pie.add_annotation(dict(x=0.5, y=0.5,  align='center',
                                                                xref = "paper", yref = "paper",
                                                                showarrow = False, font_size=22,
                                                                text="Neutrals"))
                                        neutrals_pie.update_layout(legend=dict(
                                                                        orientation="h"))
                                        nege_neu_col[1].plotly_chart(neutrals_pie, use_container_width=True)
                                        #st.plotly_chart(fig3, use_container_width=True)
                                    elif nu_chart=='Table':
                                        neutrals_df_styled=neutrals_head_df.style.applymap(cell_color).set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['Name'])\
                                                .set_properties(**{'background-color': '#d67d09','color': 'White','font-weight':'bold'}, subset=['Resultants'])\
                                                .set_properties(**{'background-color': '#229106','color': 'White','font-weight':'bold'}, subset=['Positives'])\
                                                .set_properties(**{'background-color': '#c70404','color': 'White','font-weight':'bold'}, subset=['Negetives'])\
                                                .set_properties(**{'background-color': '#b0ad04','color': 'White','font-weight':'bold'}, subset=['Neutrals'])
                                        nege_neu_col[1]._legacy_dataframe(neutrals_df_styled)
                                        nege_neu_col[1].download_button("Export📥",data=to_excel(neutrals_df_styled) ,file_name='Top Neutrals.xlsx')



                                    

                                with sv_tab2:
                                   
                                    st.subheader("Report")

                                    report_bar = px.bar(df_master_weightage_table_sorted, x='NSE_Code',
                                                        y=['Positives', 'Negetives','Neutrals', 'Resultants'],
                                                        text_auto='.2s',barmode='group',height=650,template='none')
                                    report_bar.update_xaxes(rangeslider_visible=True)
                                    report_bar.update_xaxes(title_text='')
                                    report_bar.update_yaxes(title_text='')
                                    report_bar.update_xaxes(title_font=dict(size=2, family='Courier', color='crimson'))
                                    report_bar.update_xaxes( tickfont=dict(family='Rockwell', color='black', size=13))
                                    report_bar.update_yaxes( tickfont=dict(family='Rockwell', color='black', size=11))
                                    report_bar.update_layout(coloraxis_showscale=False)
                                    report_bar.update_xaxes(ticklen=0)
                                    report_bar.update_yaxes(ticklen=0)
                                    st.plotly_chart(report_bar, use_container_width=True)
                                    mf_rcol=st.columns((5,10,2))
                                    mf_rcol[0].markdown("#### Weightage Table")
                                    x=list(range(1, len(df_master_weightage_table_sorted)+1))

                                    df_dupli=df_master_weightage_table_sorted.copy(deep=True)
                                    df_dupli=df_dupli.drop(["Name","BSE_Code","NSE_Code","Industry","Resultants","Positives","Negetives","Neutrals"],axis=1)
                                    #df_dupli.set_index([pd.Index(x), 'Name'],inplace = True)
                                    #st.write(df_dupli)
                                    #df2=df[df_l].drop(["Name","BSE_Code","NSE_Code","Industry","Positives","Negetives","Resultants","Neutrals"],axis=1)
                                    #st.write(df2.columns.to_list())
                                    #st.write(df2)
                                    #df2_transposed = df2.T 
                                    #df2_transposed.columns=df['Name']

                                    list_c=df_dupli.columns.to_list()
                                    #st.write(list_c[3:])
                                    #df.style.applymap(color_negative_red, subset=['total_amt_usd_diff','total_amt_usd_pct_diff'])
                                    cm = sns.light_palette("green", as_cmap=True)
                                    df2_transposed = df_dupli.T 
                                    df2_transposed.columns=df_master_weightage_table_sorted['Name']
                                    
                                    #df2_transposed.set_index([pd.Index(y), 'Name'],inplace = True)
                                    #st.write(df2_transposed)
                                    excl_styl_df=df2_transposed.style.applymap(cell_color).highlight_null(null_color="black")
                                   

                                    exp_col=st.columns((1,1,1,1,1))
                                    exp_col[2].markdown("##### Analysis Report")
                                    st._legacy_dataframe(excl_styl_df,height=500)
                                    #df_master_weightage_table_sorted.to_excel('styled.xlsx', engine='openpyxl')
                                    #st.write(xl)
                                    #.to_excel('styled.xlsx', engine='openpyxl')
                                    #st._legacy_dataframe(df_master_weightage_table_sorted)
                                    #background_gradient(cmap=cm, subset=['Positives', 'Negetives','Neutrals', 'Resultants']).
                                    # st.sidebar.multiselect("select companies for portfolio:", options=df_master_weightage_table_sorted['Name'])
                                    w_df=df_master_weightage_table_sorted[["Name","Industry","Resultants","Positives","Negetives","Neutrals"]]
                                    w_df_ind=list(range(1, len(df_master_weightage_table_sorted)+1))
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
                                                
                                    st.write(" ")
                                    st.write(" ")
                                    exp_col2=st.columns((1,1,1,1,1))

                                    exp_col2[2].markdown("##### Total weightage")
                                    st._legacy_dataframe(w_df_styled,height=500)
                                    
                                    #sentiment
                                    #st.write(df_master_weightage_table_sorted['Name'])
                                    exp_col3=st.columns((1,1,1,1,1))
                                    exp_col3[2].markdown("##### Sentiment Analysis")

                                    sentiment_df=pd.DataFrame({'stock_name': [], 'date': [],
                                                        'polarity': [], 'polarity_sentiment': [], 'subjectivity': [], 'positive_score': [],
                                                        'negative_score': [],'net_score': [], 'news_senti_3_M': [], 'news_senti_1_M': [], 'news_senti_7_d': [],
                                                        'news_senti_2_d': []})

                                    stocks_names=df_master_weightage_table_sorted['Name'].values.tolist()
                                    #not_available_sent_li=[]
                                    sentiment_q='SELECT stock_name FROM '+sentiment_table
                                    available_sent_li=pd.read_sql_query(sentiment_q,sq_conn)['stock_name']
                                    #if len(available_sent_li)>0:
                                    for name in stocks_names:
                                        #if name in available_sent_li:
                                        sentiment_q='SELECT * FROM '+sentiment_table+' Where stock_name="'+name+'"'
                                        sentiment_row=pd.read_sql_query(sentiment_q,sq_conn)
                                        sentiment_df=sentiment_df.append(sentiment_row,ignore_index=True)
                                    styled_sentiment_df=sentiment_df[['stock_name','date','news_senti_3_M', 'news_senti_1_M', 'news_senti_7_d','news_senti_2_d','polarity', 'polarity_sentiment', 'subjectivity', 'positive_score','negative_score','net_score']]\
                                        .style.applymap(cell_color2,subset=['news_senti_3_M', 'news_senti_1_M', 'news_senti_7_d','news_senti_2_d','polarity_sentiment'])\
                                        .set_properties(**{'background-color': '#2798e3','color': 'White','font-weight':'bold'}, subset=['stock_name'])\
                                        .bar(subset=['positive_score'], color='#6bf556')\
                                        .bar(subset=['negative_score'], color='#f75631')\
                                        .bar(subset=['net_score'], color='#5289e3')
                                    st._legacy_dataframe(styled_sentiment_df)
                                                #.drop(['polarity', 'polarity_sentiment', 'subjectivity', 'positive_score','negative_score','net_score'],axis=1)
                                            #else:
                                            #    not_available_sent_li.append(name)

                                    #else:
                                    #    st.error("Sorry Sentiment data not found")
                                    
                                    
                                    #if len(not_available_sent_li)>0:
                                    #    st.error("Sorry ,Sentiment for these stocks is not available")
                                    #    with st.expander("Unavailable sentiment list"):
                                    #        st.write(not_available_sent_li)
                                    with exp_col[4].expander("Export"):
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
                                        st.download_button("CSV", data=csv, file_name='weightage_report.csv')  
                                        #excel=
                                        st.download_button("Excel", data=new_to_excel(excl_styl_df,w_df_styled,styled_sentiment_df), file_name=user_master_table_name+'Analysis_report.xlsx')
                                        #   df_master_weightage_table_sorted.style.applymap(cell_color,subset=list_c[6:]).highlight_null(null_color="black").to_excel('styled.xlsx', engine='openpyxl')


                else:
                    st.error("Sorry, you didn't have created any filters!")
    else:
        st.error("You did'nt have uploaded any sheet.")
        st.info("Please upload sheet!!")
else:
    st.warning("Please login first.")
