from sqlalchemy import create_engine  # to setup mysql connection
import pandas as pd
#from database_conn import connect
import streamlit as st
import time
from datetime import datetime
from sqlalchemy import create_engine,text
from Home import sqlalchemy_connection,refresh_data
from Home import mf_sheet_table,mf_rolling_return_table,master_table


with open('CSS/upload_sheet.css') as f:
    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)


st.title("📤My Uploads")
MF_tab1,MF_tab2=st.tabs(('Equity','Mutual Fund'))

_=""" Refresh warning process"""
if len(st.session_state) == 0:
    st.warning("Please! Don't Refresh your browser.")
    st.info("Always, use dashboard Default Refresh!")
    with st.expander('Do This:'):
        video_file = open('video/dashboard_refresh.mp4', 'rb')
        video_bytes = video_file.read()
        st.video(video_bytes)
    st.stop()


_=""" Authentication Cheak"""
if not st.session_state["authentication_status"]:
    st.error("Please Login...")
    st.stop()

_=""" Defaults"""
username=st.session_state["username"]
current_date = datetime.now()

_="""Ttile layout"""




# credentials declaration
_="""

    Database credentials 

"""


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

    Function definitions
 
"""
#@st.cache(allow_output_mutation=False)
def file_read(file):
    df=pd.read_csv(file,thousands=',',index_col=None)
    return df



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

def eq_fetch_table(table_name,_connection,sheet_name=None,_username=None):
    if sheet_name and _username:
        try:
            selected_file_q='SELECT * FROM '+ table_name+' WHERE username="'+_username+'" and sheet_name="'+sheet_name+'"'
            selected_file_df=pd.read_sql(selected_file_q,_connection).dropna(axis=1,how='all')
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
            
# function to fetch table
#@st.experimental_memo(show_spinner=True)
def fetch_table(table_name,_connection,sheet_name=None,_username=None):
    if sheet_name and _username:
        try:
            selected_file_q='SELECT * FROM '+ table_name+' WHERE username="'+_username+'" and sheet_name="'+sheet_name+'"'
            selected_file_df=pd.read_sql(selected_file_q,_connection).dropna(axis=1,how='all').dropna(axis=0,how='any')
            selected_file_date=selected_file_df['date_time'][0]
            return selected_file_df.drop(columns=['username','sheet_name','date_time'],axis=1),selected_file_date
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
            selected_file_df=pd.read_sql(selected_file_q,_connection).dropna(axis=1,how='all').dropna(axis=0,how='any')
            lenght_of_selected_file_df=len(selected_file_df)
            return selected_file_df,lenght_of_selected_file_df
        except:
            st.error("Server lost.....")
            st.error("Please check connection.....")
            st.session_state["sq_cur_obj"]=0
            time.sleep(2)
            st.experimental_rerun()


# CSV validations    
def ready_for_work(df):
    chk_list={'Name':{'typec':'text','loc':1},'BSE Code':{'typec':'int','loc':2},'NSE Code':{'typec':'text','loc':3},'Industry':{'typec':'text','loc':4}}
    ck=[]
    for i in chk_list:
        if i not in df.columns:
            ck.append(i)
    if ck:
        return False,ck
    else:
        return True,0

def string_chk(i,loc=None):  

    symbols = {'~', ':', "'", '+', '[', '\\', '@', '^', '{', '%', '(', '"', 
            '*', '|', ',', '<', '`', '}', '=', ']', '!', '>', ';', '?', 
            '#', '$', ')'}
    symbol_with_space={'~', ':', "'", '+', '[', '\\', '@', '^', '{', '%', '(',".","-",
                    '"', '*', '|', ',', '&', '<', '`', '}', '=', ']', '!', '>', 
                    ';', '?', '#', '$', ')', '/'}
    
    errors=[]
    count=0
    for char in str(i):
        if count==0:
            if char in symbol_with_space:
                if loc:
                    if char==' ':
                        e="error:at column='"+str(loc.get('column_no'))+"' row='"+str(loc.get('row_no'))+"' element='"+str(i)+"' (includes space at first):Strings should'nt have '_space_' at first"
                        errors.append(e)
                    else:
                        e="error:at column='"+str(loc.get('column_no'))+"' row='"+str(loc.get('row_no'))+"' element='"+str(i)+"' (includes ' "+char+" ' at location "+str(count+1)+":Strings should'nt have special characters"
                        errors.append(e)
                else:
                    e="error:at '"+str(i)+" '(includes ' "+char+" ' at location "+str(count)+":Strings should'nt have special characters"
                    errors.append(e)
    count+=1
    if char in symbols:
        if loc:
            c="error:at column='"+str(loc.get('column_no'))+"' row='"+str(loc.get('row_no'))+"' element='"+str(i)+"' (includes ' "+char+" ' at location "+str(count)+":Strings should'nt have special characters"
            errors.append(c)
        else:
            c="error:at '"+str(i)+"' (includes '"+char+"' at location "+str(count)+":Strings should'nt have special characters"
            errors.append(c)
    return errors

def values_chek(i,loc=None):
    errors=[]
    symbols_for_int={' ','~', ':', "'", '[', '\\', '@', '^', '{', '%', '(',
                    '"', '*', '|', ',', '&', '<', '`', '}', '=', ']', '!', '>', 
                    ';', '?', '#', '$', ')', '/'}
    Null_list={'nun','none','NUN','NONE','None','NULL','Null','nan'}
    if str(i) not in Null_list:
        if str(i).isalpha():
            if loc:
                c="error:at column='"+str(loc.get('column_no'))+"' row='"+str(loc.get('row_no'))+"' element='"+str(i)+"'(Values should'nt have characters/special characters)"
                errors.append(c)
            else:
                c="error:at element='"+str(i)+"'(Values should'nt have characters/special characters)"
                errors.append(c)
    else:
        count=0
        for v in str(i):
            count+=1
            if loc:
                if v in symbols_for_int:
                    c="error:at column='"+str(loc.get('column_no'))+"' row='"+str(loc.get('row_no'))+"' element='"+str(i)+"' (includes ' "+v+" ' at location "+str(count)+":Values should'nt have characters/special characters)"                
                    errors.append(c)
            else:
                if v in symbols_for_int:
                    c="error:at element='"+str(i)+"' (includes ' "+v+" ' at location "+str(count)+":Values should'nt have characters/special characters)"
                    errors.append(c)
    return errors

def dataframe_validate(df):
    chk_list={'Name':{'type':'text','loc':1},'BSE Code':{'type':'int','loc':2},'NSE Code':{'type':'text','loc':3},'Industry':{'type':'text','loc':4}}
    columns_list=[]
    for column in df:
        columns_list.append(column)
    columns_err=[]
    col_no=0
    for i in columns_list:
        col_no+=1
        loc={'column_no':col_no,'row_no':0}
        x=string_chk(i,loc)
        if x:
            columns_err.append(x)
    values_errors=[]
    col_count=0
    for col in df.columns:
        col_count+=1
        if col in chk_list:
            if chk_list.get(col).get('type')=='text':
                row_count=0
                for ele in df[col]:
                    row_count+=1
                    if str(ele).isalpha():
                        continue
                    else:
                        loc={'column_no':col_count,'row_no':row_count}
                        er=string_chk(ele,loc)
                        if er:
                            values_errors.append(er)
        else:
            row_count=0
            for ele in df[col]:
                row_count+=1
                if str(ele).isalpha():
                    continue
                else:
                    loc={'column_no':col_count,'row_no':row_count}
                    er=values_chek(ele,loc)
                    if er:
                        values_errors.append(er)      
    return columns_err,values_errors




def query_run(query,connection):
    try:
        df=pd.read_sql_query(query,connection)
        return df
    except:
        st.error("Something went wrong!!!")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            st.error("Connection lost")
            time.sleep(2)
            st.experimental_rerun()

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

#
def new_column_list(df,db_table_name,_connection):
    master_sheet_column_set = set(df.columns)
    # uploading new column
    q = "SHOW COLUMNS FROM "+db_table_name
    database_column = pd.read_sql_query(q, _connection)        #type=pandas.core.series.Series
    db_col_list = database_column['Field']
    rem_us_col = rem_under_scr_col_list(db_col_list[1:])
    database_column_set = set(rem_us_col)
    new_column_set = master_sheet_column_set.difference(database_column_set)
    return new_column_set,rem_us_col

def cheak_sheet_name(db_table_name,sheet_name,_connection,username):
    if sheet_name=='':
        st.error("Please,give a name to this sheet!")
        st.stop()
    else:    
        n = "SELECT sheet_name FROM " + db_table_name + " WHERE username='" +username + "' and sheet_name='"+sheet_name+"'"
        try:
            catlog_names=query_run(n,_connection)['sheet_name'].to_list()
        except:
            catlog_names=""
        if len(catlog_names):
            e="Sorry,Sheet name already used!!"
            return 0,e
        else:
            e=0
            return 1,e

    _="""#username='" +username + "' and
    n = "SELECT sheet_name FROM " + db_table_name + " WHERE username='" +username + "' and sheet_name='"+sheet_name+"'"
    st.write(cursor.execute(n).fetchall(),n)
    username
    n = "SELECT sheet_name FROM " + db_table_name + " WHERE username='" +username + "' and sheet_name='"+sheet_name+"'"
    if cursor.execute(n).fetchall():
        e="Sorry,Sheet name already used!!"
        return 0,e
    else:
        e=0
        return 1,e"""



def upload_file_db(df,_current_date,_username,_sheet_name,db_table_name,_connection):
    try:
        with st.spinner('Uploading...'):
                df.insert(0, "date_time", _current_date)
                df.insert(1, "username", _username)
                df.insert(2, "sheet_name", _sheet_name)
                df.columns = [x.replace(" ", "_") for x in df.columns]                         # replacing space with underscore
                df.to_sql(db_table_name, _connection, if_exists='append', index=False)           # uploading file to database
                #conn.commit()
                st.success("Sheet has uploaded!")
                time.sleep(1)
    except:
        if not st.session_state["sq_cur_obj"]:
            st.error("Connection lost")
            st.session_state["sq_cur_obj"]=0   
            time.sleep(1)
            st.experimental_rerun()
        st.error("Something went wrong!!!")
        time.sleep(1)
        st.experimental_rerun()
    st.experimental_rerun()



#upload columns
def upload_columns(temp_df,db_table_name,_cursor):
    try:
        with st.spinner('Uploading...'):
            time.sleep(1)
            add_col_db(temp_df, db_table_name, _cursor)
            st.success("Columns Has successfully added to database!")
            time.sleep(2)
            temp_df.drop(temp_df.index, inplace=True)
    except:
        st.error("Something went wrong!!!")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            st.error("Connection lost")
            time.sleep(1)
            st.experimental_rerun()
    st.experimental_rerun() 




_=""" 

Session state

"""

_=""" ss variables """

# 0.Sheets names
if 'u_users_sheets_names' not in st.session_state:
    st.session_state['u_users_sheets_names']=sheet_names(st.session_state["username"],master_table,sq_conn)

# 1.selected sheet
if 'u_selected_sheet_name' not in st.session_state:
    st.session_state['u_selected_sheet_name']=st.session_state['u_users_sheets_names'].iloc[0]['sheet_name']

# 2.selected sheet df
if 'u_selected_sheet_df' and 'u_selected_sheet_datetime' not in st.session_state:
    st.session_state['u_selected_sheet_df'],st.session_state['u_selected_sheet_datetime']= eq_fetch_table(table_name=master_table,_connection=sq_conn,sheet_name=st.session_state['u_selected_sheet_name'],_username=st.session_state["username"])




if 'u_main_file_names' not in st.session_state:
    st.session_state['u_main_file_names']=sheet_names(username,mf_sheet_table,sq_conn)

if 'u_selected_main_file_name' not in st.session_state:
    st.session_state['u_selected_main_file_name']=st.session_state['u_main_file_names'].iloc[0]['sheet_name']

if 'u_selected_main_file_df' and 'u_selected_main_file_date' not in st.session_state:
    st.session_state['u_selected_main_file_df'],st.session_state['u_selected_main_file_date']=fetch_table(mf_sheet_table,sheet_name=st.session_state['u_selected_main_file_name'],_connection=sq_conn,_username=username)

# 2.Rolling return name & dataframe

if 'u_rr_file_names' not in st.session_state:
    st.session_state['u_rr_file_names']=sheet_names(username,mf_rolling_return_table,sq_conn)

if 'u_selected_rr_file_name' not in st.session_state:
    st.session_state['u_selected_rr_file_name']=st.session_state['u_rr_file_names'].iloc[0]['sheet_name']

if 'u_selected_rr_file_df' and 'u_selected_rr_file_date' not in st.session_state:
    st.session_state['u_selected_rr_file_df'],st.session_state['u_selected_rr_file_date']=fetch_table(mf_rolling_return_table,sheet_name=st.session_state['u_selected_rr_file_name'],_connection=sq_conn,_username=username)




# temporary dataframe to hold selected list
if 'df_temp' not in st.session_state:
    st.session_state.df_temp = pd.DataFrame({'Parameter Name': [], 'Datatype': [], 'Range': []})

if 'df_temp2' not in st.session_state:
    st.session_state.df_temp2 = pd.DataFrame({'Parameter Name': [], 'Datatype': [], 'Range': []})


if 'delete_clicked_ueq' not in st.session_state:
        st.session_state.delete_clicked_ueq = False

def del_callback_ueq():
        st.session_state.delete_clicked_ueq = True


if 'delete_clicked_um' not in st.session_state:
        st.session_state.delete_clicked_um = False

def del_callback_um():
        st.session_state.delete_clicked_um = True

if 'delete_clicked_urr' not in st.session_state:
        st.session_state.delete_clicked_urr = False

def del_callback_urr():
        st.session_state.delete_clicked_urr = True




with MF_tab1:
    eq_files_tab,eq_upload_tab=st.tabs(["My Files","Upload File"])

    with eq_upload_tab:
        eq_file = st.file_uploader("Upload your file:",key='eq_upload')
        if st.checkbox("Upload to Database"):
            if 'eq_df_temp' not in st.session_state:
                st.session_state.eq_df_temp = pd.DataFrame({'Parameter Name': [], 'Datatype': [], 'Range': []})
            if eq_file:
                
                eq_df = pd.read_csv(eq_file,index_col=None)
                with st.expander(" Your selected sheet:"):         
                    st._legacy_dataframe(eq_df)
                
                re,er=ready_for_work(eq_df)
                if re==False:
                    st.error("Please,don't upload sheet it will damage your storage. ")                                
                    with st.expander("Error log:"):
                        st.write("Your sheet doesn't have neccessory columns:")
                        st.write(er)
                else:
                    ce,ve=dataframe_validate(eq_df)
                    if ce and ve:
                        #st.error
                        st.error("Your sheet is invalid!")
                        with st.expander("Error log:"):  
                            st.write(ce)
                            st.write(ve)

                #ce,ve=dataframe_validate(df)
                #if ce and ve:
                #    st.warning("Your sheet is invalid:")
                #    with st.form("sheet validator"):
                #        re,er=ready_for_work(df)
                #        if st.form_submit_button("Validate"):
                #            if re==False:
                #                with st.expander("Error log:"):
                #                    st.write("Your sheet doesn't have neccessory columns:")
                #                    st.write(er)
                #            else:
                #                ce,ve=dataframe_validate(df)
                #                if ce and ve:
                                    #st.error
                #                    with st.expander("Error log:"):
                #                        st.write("Your sheet is invalid:")
                #                        st.write(ce)
                #                        st.write(ve)
                        
                master_sheet_column_set = set(eq_df.columns)
                # uploading new column
                q = "SHOW COLUMNS FROM "+master_table
                database_column = pd.read_sql_query(q, sq_conn)        #type=pandas.core.series.Series
                db_col_list = database_column['Field']
                rem_us_col = rem_under_scr_col_list(db_col_list[4:])
                database_column_set = set(rem_us_col)
                new_column_set = master_sheet_column_set.difference(database_column_set)

                if new_column_set:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.subheader("Your New sheets columns:{}".format(len(eq_df.columns)))
                        st._legacy_dataframe(list(eq_df.columns), height=200)
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
                        st.session_state.eq_df_temp=st.session_state.eq_df_temp.append({'Parameter Name': column, 'Datatype': datatype, 'Range': length}, ignore_index=True)
                        st.write("Selected Columns List:")
                        st.write(st.session_state.eq_df_temp)
                        st.experimental_rerun()
                    if len (st.session_state.eq_df_temp)>0:
                        if st.button("clear"):
                            st.session_state.eq_df_temp = st.session_state.eq_df_temp.drop(len(st.session_state.eq_df_temp) - 1)
                            st.experimental_rerun()
                                
                        else:
                            st.write("Selected Columns List:")
                            st.write(st.session_state.eq_df_temp)

                        if st.button("Add Columns"):
                            with st.spinner('Uploading...'):
                                time.sleep(1)
                                upload_columns(st.session_state.eq_df_temp, master_table, sq_cur)
                                st.success("Columns Has successfully added to database!")
                                time.sleep(2)
                                st.session_state.eq_df_temp.drop(st.session_state.eq_df_temp.index, inplace=True)
                                st.experimental_rerun()
                            _="""    
                            if st.button("Submit"):
                                df.insert(0, "date_time", str(current_date))
                                df.insert(1, "lable", st.session_state["lable"])
                                df.insert(2, "username", st.session_state["username"])
                                df.columns = [x.replace(" ", "_") for x in df.columns]
                                df.to_sql(master_table, sq_conn, if_exists='append', index=False)
                                conn.commit()
                                st.session_state.df_temp.drop(st.session_state.df_temp.index, inplace=True)
                                time.sleep(2)
                                st.success("Sheet has uploaded!")
                                time.sleep(2)
                                st.experimental_rerun()
                            """
                else:
                    eq_sheet_name=st.text_input("Name your sheet.",max_chars=100)

                    if st.button("Submit"):
                        cheaked,error=cheak_sheet_name(master_table,eq_sheet_name,sq_conn,username)
                        if cheaked:
                        # Uploading process
                            upload_file_db(eq_df,current_date,username,eq_sheet_name,master_table,sq_conn)
                        elif error:
                            st.error(error)
                    else:
                        st.error("Please give your sheet a name")

                    _="""
                        if sheet_name=='':
                            st.error("Please,give a name to this sheet!")
                            st.stop()
                        n = "SELECT sheet_name FROM " + master_table + " WHERE username='" + st.session_state["username"] + "' and sheet_name='"+sheet_name+"'"
                        cur.execute(n)
                        values = cur.fetchall()
                        if values:
                            st.error("Sorry,Sheet name already used!!")
                        
                        else:
                            with st.spinner('Uploading...'):
                                time.sleep(3)
                                df.insert(0, "date_time", str(current_date))
                                df.insert(1, "lable", st.session_state["lable"])
                                df.insert(2, "username", st.session_state["username"])
                                df.insert(3, "sheet_name", sheet_name)
                                #st.write(df)
                                df.columns = [x.replace(" ", "_") for x in df.columns]
                                df.to_sql(master_table, sq_conn, if_exists='append', index=False)
                                conn.commit()
                                time.sleep(2)
                                st.success("Sheet has uploaded!")
                                time.sleep(2)
                                st.experimental_rerun()
                    """
            else:
                st.warning("file has not uploaded")

    with eq_files_tab:
        _=""" 
        table of rr file
        """
        eq_myf_col=st.columns((8,12,1))

        #   subheader for main files
        #   Select box for main files selection
        eq_myf_col[0].subheader("Equity Files")
        eq_myf_col[0].write("Total Sheets : {}".format(len(st.session_state['u_users_sheets_names'])))
        eq_myf_col[0]._legacy_dataframe(st.session_state['u_users_sheets_names'])

        u_eq__selected_file_name_1=eq_myf_col[1].selectbox("",options=st.session_state['u_users_sheets_names'],key='ueq')
        #   buttons
        eq_myf_col[2].write('')
        eq_myf_col[2].write('')
        if eq_myf_col[2].button("🔍",key='ueq'):
            st.session_state['u_users_sheets_names']=sheet_names(st.session_state["username"],master_table,sq_conn)
            st.session_state['users_sheets_names']=st.session_state['u_users_sheets_names']
            st.session_state['u_selected_sheet_name']=u_eq__selected_file_name_1
            st.session_state['u_selected_sheet_df'],st.session_state['u_selected_sheet_datetime']= eq_fetch_table(table_name=master_table,_connection=sq_conn,sheet_name=st.session_state['u_selected_sheet_name'],_username=st.session_state["username"])
                #update sessionsate main file df
            st.experimental_rerun()

        eq_myf_col[2].write('')
        eq_rem_but=eq_myf_col[2].button("🗑️",help="Delete your sheet",key='U_eq',on_click=del_callback_ueq)
        if (eq_rem_but or st.session_state.delete_clicked_ueq):
            with eq_myf_col[1].form("Are you sure?"):
                st.write("Are you sure?")
                if st.form_submit_button("Yes"):
                    with st.spinner('Deleting...'):
                        time.sleep(1)
                        o = "DELETE FROM " + master_table + " WHERE username='" + st.session_state["username"] + "' and sheet_name='" +st.session_state['u_selected_sheet_name'] + "'"
                        sq_cur.execute(text(o))
                        time.sleep(1)
                        st.success("Sheet Deleted")
                        time.sleep(1)
                        st.session_state.delete_clicked_ueq=False
                        st.session_state['u_users_sheets_names']=sheet_names(st.session_state["username"],master_table,sq_conn)
                        st.experimental_rerun()
                elif st.form_submit_button("No"):
                    st.session_state.delete_clicked_ueq=False
                    st.experimental_rerun()

        #   show table
        if len(st.session_state['u_selected_sheet_df']):
            eq_myf_col[1].subheader(st.session_state['u_selected_sheet_name'])
            eq_myf_col[1].write('Created at:'+str(st.session_state['u_selected_sheet_datetime']))
            with eq_myf_col[1].expander("Show my sheet",expanded=True):
                st._legacy_dataframe(st.session_state['u_selected_sheet_df'])
        else:
            st.info("Empty.....")


with MF_tab2:

    # layout columns
    MF_tab21,MF_tab22=st.tabs(["My Files","Upload File"])

    with MF_tab22:

        # expanding container for Upload file
        with st.expander("+ Main File"):
        
            # upload file feature
            file = st.file_uploader("Upload your file:")

            # cheak file upload or not
            upload=st.checkbox("Upload")

            if upload:
  

                if file:
                    # Take a name for sheet 
                    m_file_df = file_read(file)

                    st.write(" Your selected sheet:") 

                    st._legacy_dataframe(m_file_df)

                    m_sheet_name=st.text_input("Name")

                    new_column_set,rem_us_col =new_column_list(m_file_df,mf_sheet_table,sq_conn)


                    if new_column_set:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.subheader("Your New sheets columns:{}".format(len(m_file_df.columns)))
                            st._legacy_dataframe(list(m_file_df.columns), height=200)
                        with col2:
                            st.subheader("your database columns:{}".format(len(rem_us_col)))
                            st._legacy_dataframe(rem_us_col, height=200)
                        with col3:
                            st.subheader("Extra new columns:{}".format(len(new_column_set)))
                            st._legacy_dataframe(new_column_set, height=200)


                        copy_new_column_set = new_column_set.copy()

                        #st.experimental_rerun()
                        column_list = copy_new_column_set.difference(set(st.session_state.df_temp['Parameter Name']))

                        col1, col2, col3, col4 = st.columns(4)
                        with col2:
                            datatype = st.selectbox("Select Datatype for column :",options=('FLOAT','VARCHAR','DOUBLE'))
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

                            if st.button("Add Columns",key='main_file'):
                                upload_columns(st.session_state.df_temp,mf_sheet_table,sq_cur)

                    else:
                    
                        submit=st.button("Submit",key='main_file_s')

                        if submit :
                               
                            cheaked,error=cheak_sheet_name(mf_sheet_table,m_sheet_name,sq_conn,username)
                                                        # Uploading process
                            if cheaked:
                                upload_file_db(m_file_df,current_date,username,m_sheet_name,mf_sheet_table,sq_conn)
                            elif error:
                                st.error(error)
                                st.stop() 
                            st.session_state['u_main_file_names']=sheet_names(username,mf_sheet_table,sq_conn)
                            st.session_state['main_file_names']=sheet_names(username,mf_sheet_table,sq_conn)
                            st.experimental_rerun()
                            
                       
                else:
                    st.warning("Please upload file.....")


        with st.expander("+ Rolling Return File"):

                # upload file feature
                rr_file = st.file_uploader("Upload your file:",key=2)

                # cheak file upload or not
                rr_uploaded=st.checkbox("Upload",key=2)

                if rr_uploaded:

                    if rr_file:

                        # read file in dataframe
                        rr_file_df = file_read(rr_file)

                        # show the uploaded file
                        st.write(" Your selected sheet:")    
                        st._legacy_dataframe(rr_file_df)

                        # Take a name for sheet    
                        rr_sheet_name=st.text_input("")

                        rr_new_column_set,rr_rem_us_col = new_column_list(rr_file_df,mf_rolling_return_table,sq_conn)

                        if rr_new_column_set:
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.subheader("Your New sheets columns:{}".format(len(rr_file_df.columns)))
                                st._legacy_dataframe(list(rr_file_df.columns), height=200)
                            with col2:
                                st.subheader("your database columns:{}".format(len(rr_rem_us_col)))
                                st._legacy_dataframe(rr_rem_us_col, height=200)
                            with col3:
                                st.subheader("Extra new columns:{}".format(len(rr_new_column_set)))
                                st._legacy_dataframe(rr_new_column_set, height=200)


                            copy_rr_new_column_set = rr_new_column_set.copy()

                            column_list = copy_rr_new_column_set.difference(set(st.session_state.df_temp2['Parameter Name']))
                            col1, col2, col3, col4 = st.columns(4)
                            with col2:
                                datatype = st.selectbox("Select Datatype for column :",options=('FLOAT','VARCHAR','DOUBLE'))
                            with col3:
                                length = st.number_input("Select Range for column:",min_value=10, max_value=50, value=20, step=1)
                            with col1:
                                column = st.selectbox("Choose new column:", options=column_list)

                            if col4.button("AND",key='rr_file'):
                                selection = {'Parameter Name': column, 'Datatype': datatype, 'Range':length}
                                st.session_state.df_temp=st.session_state.df_temp.append({'Parameter Name': column, 'Datatype': datatype, 'Range': length}, ignore_index=True)
                                st.write("Selected Columns List:")
                                st.write(st.session_state.df_temp2)
                                st.experimental_rerun()

                            if len (st.session_state.df_temp2)>0:
                                if st.button("clear",key='rr_file'):
                                    st.session_state.df_temp2 = st.session_state.df_temp2.drop(len(st.session_state.df_temp2) - 1)
                                    st.experimental_rerun()
                                        
                                else:
                                    st.write("Selected Columns List:")
                                    st.write(st.session_state.df_temp2)

                                if st.button("Add Columns",key='rr_file'):
                                    upload_columns(st.session_state.df_temp2, mf_rolling_return_table, sq_cur)
                        else:
                            # start of uploading process
                            submit=st.button("Submit",key='rr_file')

                            if submit:

                                cheaked,error=cheak_sheet_name(mf_rolling_return_table,rr_sheet_name,sq_conn,username)
                                if cheaked:
                                # Uploading process
                                    upload_file_db(rr_file_df,current_date,username,rr_sheet_name,mf_rolling_return_table,sq_conn)
                                
                                elif error:
                                    st.error(error)

                                st.session_state['u_rr_file_names']=sheet_names(username,mf_rolling_return_table,sq_conn)
                                st.session_state['rr_file_names']=sheet_names(username,mf_rolling_return_table,sq_conn)
                                st.experimental_rerun()
                            
                    else:
                        st.warning("Please upload file.....")

    with MF_tab21:
        st.header("My Files")
        myf_col=st.columns((4,12,1))

        _=""" 
        table of main file
        """

        #   subheader for main files
        #   Select box for main files selection
        myf_col[0].subheader("Main Files:")
        myf_col[0].write("Total Sheets : {}".format(len(st.session_state['u_main_file_names'])))

        myf_col[0]._legacy_dataframe(st.session_state['u_main_file_names'])


        u_selected_file_name_1=myf_col[1].selectbox("",options=st.session_state['u_main_file_names'],key=1)
        #   buttons
        myf_col[2].write('')
        myf_col[2].write('')
        if myf_col[2].button("🔍",key=1):

            st.session_state['u_selected_main_file_name']=u_selected_file_name_1
            st.session_state['u_selected_main_file_df'],st.session_state['u_selected_main_file_date']=fetch_table(mf_sheet_table,sheet_name=st.session_state['u_selected_main_file_name'],_connection=sq_conn,_username=username)
                #update sessionsate main file df
            st.experimental_rerun()

        m_rem_but=myf_col[2].button("🗑️",help="Delete your sheet",key='U_me',on_click=del_callback_um)
        if (m_rem_but or st.session_state.delete_clicked_um):
            with myf_col[1].form("Are you sure?"):
                st.write("Are you sure?")
                if st.form_submit_button("Yes"):
                    with st.spinner('Deleting...'):
                        time.sleep(1)
                        o = "DELETE FROM " + mf_sheet_table + " WHERE username='" + st.session_state["username"] + "' and sheet_name='" +st.session_state['u_selected_main_file_name'] + "'"
                        sq_cur.execute(text(o))
                        time.sleep(1)
                        st.success("Sheet Deleted")
                        time.sleep(1)
                        st.session_state.delete_clicked_um=False
                        st.session_state['u_main_file_names']=sheet_names(username,mf_sheet_table,sq_conn)
                        st.experimental_rerun()
                elif st.form_submit_button("No"):
                    st.session_state.delete_clicked_um=False
                    st.experimental_rerun()   
        #   show table
        if len(st.session_state['u_selected_main_file_df']):
            myf_col[1].subheader(st.session_state['u_selected_main_file_name'])
            myf_col[1].write('Created at:'+str(st.session_state['u_selected_main_file_date']))
            with myf_col[1].expander("Show my sheet",expanded=True):
                st._legacy_dataframe(st.session_state['u_selected_main_file_df'])
        else:
            st.info("Empty.....")
        # Show all file's register
      

        _=""" 
        table of rr file
        """
        rr_myf_col=st.columns((4,12,1))

        #   subheader for main files
        #   Select box for main files selection
        rr_myf_col[0].subheader("Rolling Returns Files:")
        rr_myf_col[0].write("Total Sheets : {}".format(len(st.session_state['u_rr_file_names'])))
        rr_myf_col[0]._legacy_dataframe(st.session_state['u_rr_file_names'])

        u_rr_selected_file_name_1=rr_myf_col[1].selectbox("",options=st.session_state['u_rr_file_names'],key='urr')
        #   buttons
        rr_myf_col[2].write('')
        rr_myf_col[2].write('')
        if rr_myf_col[2].button("🔍",key='urr'):
            st.session_state['u_rr_file_names']=sheet_names(username,mf_rolling_return_table,sq_conn)
            st.session_state['u_selected_rr_file_name']=u_rr_selected_file_name_1
            st.session_state['u_selected_rr_file_df'],st.session_state['u_selected_rr_file_date']=fetch_table(mf_rolling_return_table,sheet_name=st.session_state['u_selected_rr_file_name'],_connection=sq_conn,_username=username)
                #update sessionsate main file df
            st.experimental_rerun()

        rr_rem_but=rr_myf_col[2].button("🗑️",help="Delete your sheet",key='U_rre',on_click=del_callback_urr)
        if (rr_rem_but or st.session_state.delete_clicked_urr):
            with rr_myf_col[1].form("Are you sure?"):
                st.write("Are you sure?")
                if st.form_submit_button("Yes"):
                    with st.spinner('Deleting...'):
                        time.sleep(1)
                        o = "DELETE FROM " + mf_rolling_return_table + " WHERE username='" + st.session_state["username"] + "' and sheet_name='" +st.session_state['u_selected_rr_file_name'] + "'"
                        sq_cur.execute(text(o))
                        time.sleep(1)
                        st.success("Sheet Deleted")
                        time.sleep(1)
                        st.session_state.delete_clicked_urr=False
                        st.session_state['u_rr_file_names']=sheet_names(username,mf_rolling_return_table,sq_conn)
                        st.experimental_rerun()
                elif st.form_submit_button("No"):
                    st.session_state.delete_clicked_urr=False
                    st.experimental_rerun()

        #   show table
        if len(st.session_state['u_selected_rr_file_df']):
            rr_myf_col[1].subheader(st.session_state['u_selected_rr_file_name'])
            rr_myf_col[1].write('Created at:'+str(st.session_state['u_selected_rr_file_date']))
            with rr_myf_col[1].expander("Show my sheet",expanded=True):
                st._legacy_dataframe(st.session_state['u_selected_rr_file_df'])
        else:
            st.info("Empty.....")

