# Auto add columns in database through query
# Database:stock_filtering_dashboard_database, Table: trial_master_sheet, Table_names= "Underscored"
import pandas as pd
import streamlit as st
from datetime import datetime  # to read the uploaded csv
import time

_="""
    Required functions
"""
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

# To add extra columns to database

def add_col_db(df,table_name,cursor,connection):
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
            cursor.execute(query)
            connection.commit()

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

if 'delete_clicked_2' not in st.session_state:
        st.session_state.delete_clicked_2 = False

def del_callback_2():
        st.session_state.delete_clicked_2 = True
        
st.title("📤Upload Sheet")
tab1,tab2=st.tabs(["Upload Sheet","Saved Sheet"])
with tab1:
        file = st.file_uploader("Upload your file:")
        if st.checkbox("Upload to Database"):
            if 'df_temp' not in st.session_state:
                st.session_state.df_temp = pd.DataFrame({'Parameter Name': [], 'Datatype': [], 'Range': []})
            if file:
                
                df = pd.read_csv(file,index_col=None)
                with st.expander(" Your selected sheet:"):         
                    st._legacy_dataframe(df)
                
                re,er=ready_for_work(df)
                if re==False:
                    st.error("Please,don't upload sheet it will damage your storage. ")                                
                    with st.expander("Error log:"):
                        st.write("Your sheet doesn't have neccessory columns:")
                        st.write(er)
                else:
                    ce,ve=dataframe_validate(df)
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
                        
                master_sheet_column_set = set(df.columns)
                # uploading new column
                q = "SHOW COLUMNS FROM "+master_table
                database_column = pd.read_sql_query(q, conn)        #type=pandas.core.series.Series
                db_col_list = database_column['Field']
                rem_us_col = rem_under_scr_col_list(db_col_list[4:])
                database_column_set = set(rem_us_col)
                new_column_set = master_sheet_column_set.difference(database_column_set)

                if new_column_set:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.subheader("Your New sheets columns:{}".format(len(df.columns)))
                        st._legacy_dataframe(list(df.columns), height=200)
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
                                add_col_db(st.session_state.df_temp, master_table, cur, conn)
                                st.success("Columns Has successfully added to database!")
                                time.sleep(2)
                                st.session_state.df_temp.drop(st.session_state.df_temp.index, inplace=True)
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
                    sheet_name=st.text_input("Name your sheet.",max_chars=100)

                    if st.button("Submit"):
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

            else:
                st.warning("file has not uploaded")
with tab2:
        w = "SELECT date_time FROM " + master_table + " WHERE username='" + st.session_state["username"] + "'"
        df_user_master_table_dates = pd.read_sql_query(w, conn)
        user_master_table_dates = df_user_master_table_dates.drop_duplicates()
        n = "SELECT sheet_name FROM " + master_table + " WHERE username='" + st.session_state["username"] + "'"
        df_user_master_table_names = pd.read_sql_query(n, conn)
        user_master_table_names = df_user_master_table_names.drop_duplicates()
        if len(user_master_table_dates)!=0:
            user_master_table_name = st.selectbox("Select your saved sheets", options=user_master_table_names)
            v = "SELECT * FROM " + master_table + " WHERE username='" + st.session_state[
                    "username"] + "' and sheet_name='" +user_master_table_name+ "'"
            df_user_master_table = pd.read_sql_query(v, conn)
            df_user_master_table=df_user_master_table.dropna(axis=1,how='all')
            master_table_date=df_user_master_table['date_time'].iloc[0]

            show_df3 = df_user_master_table.drop(["date_time", "lable", "username","sheet_name"], axis=1)
            #st.write(show_df3.style.hide_index())
        
            gd=GridOptionsBuilder.from_dataframe(show_df3)
            gd.configure_pagination(enabled=True,paginationPageSize=10)
            gd.configure_default_column(min_column_width =1,editable=False,groupable=True)
            gridoptions=gd.build()
            grid_table=AgGrid(show_df3,gridOptions=gridoptions,theme='material')
            if (st.button("🗑️Delete",help="Delete your sheet",key='sgbfb',on_click=del_callback_2) or st.session_state.delete_clicked_2):
                with st.form("Are you sure?"):
                    st.write("Are you sure?")
                    if st.form_submit_button("Yes"):
                        with st.spinner('Deleting...'):
                            time.sleep(3)
                            o = "DELETE FROM " + master_table + " WHERE username='" + st.session_state["username"] + "' and date_time='" + str(master_table_date) + "'"
                            cur.execute(o)
                            conn.commit()
                            time.sleep(2)
                            st.success("Sheet Deleted")
                            time.sleep(2)
                            st.session_state.delete_clicked_2=False
                            st.experimental_rerun()
                    elif st.form_submit_button("No"):
                        st.session_state.delete_clicked_2=False
                        st.experimental_rerun()        
        else:
            st.error("You did'nt have uploaded any sheet.")
            st.info("Please upload sheet!!")


