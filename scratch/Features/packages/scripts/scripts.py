import streamlit as st
import pandas as pd


="""
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