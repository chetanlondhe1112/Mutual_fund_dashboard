import time
import streamlit as st
import pandas as pd
import plotly.express as px
from Home import master_table
from Home import sentiment_table
from Home import news_table
from sqlalchemy import create_engine,text
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import pytz
from sqlalchemy import create_engine  # to setup mysql connection
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
from Home import _user,_password,_host,_port,_database
from Home import sqlalchemy_connection,refresh_data,auth_func
from Home import sq_conn,sq_cur,main
from IPython.display import HTML
import extra_streamlit_components as stx



_=""" Layout definition """

#st.set_page_config(layout='wide', page_icon='📈')


_=""" CSS of dashboard """

with open('CSS/upload_sheet.css') as f:
    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)

#@st.cache(allow_output_mutation=True, suppress_st_warning=True


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


_=""" main Layout designing """
#tab_1,tab_2=st.tabs(["News","Data_collector"])

_=""" title declaration """
#with tab_1:
title_col=st.columns((7,5,1))
title_col[0].title("📰Sentiment")
s_col=st.columns((7,5,1))
with s_col[1].expander("💡"):
    st.markdown("\t###### Positive:🟢 Negative:🔴 Neutral:🔵")
s_col4=st.columns((1,1,1,1))
s_col3=st.columns((5,5,5,5))
s_col1=st.columns((5,5,2,2))
#st.title("📰Sentiment")
_=""" Layout designing """

_="""credentials declaration """
# required database table
# Mutual Fund table credentials
#st.secrets["db_table"]["mf_sheet_table"]
mf_sheet_table='mf_sheet_table'
mf_filter_table=st.secrets["db_table"]["mf_filter_table"]
mf_rolling_return_table='mf_rr_sheet_table'



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


#sq_conn = create_engine('mysql://root:@localhost/{}'.format(database))

converted = pytz.timezone('Asia/Kolkata')
    #current_time=datetime.now(converted)



_=""" 

    Functions definition

"""

# 1. color function
def color_negative_red(value):
    
    _="""
        Colors elements in a dateframe
        green if positive and red if
        negative. Does not color NaN
        values
    """

    if value < 0:
        color = 'red'
    elif value > 0:
        color = 'green'
    else:
        color = 'blue'
    return 'color: %s' % color


# function to collect names from table
def names(table,connection):

    _="""
    
    To collect the names of stocks

    """
    try:
        stocks_names_q="SELECT Name From "+table
        stocks_names_df=pd.read_sql_query(stocks_names_q,connection).drop_duplicates()
        return stocks_names_df
    except:
        st.error("Something were wrong......")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.warning("Please check connection.....")
            st.experimental_rerun()



# function for flags
def flag(Flag):
    if Flag=="Positive":
        return '🟢'
    elif Flag=='Negative':
        return '🔴'
    elif Flag=='Neutral':
        return '🔵'
        

# function to get news
def news_df(stock_name):
    try:
        news_q='SELECT * From '+news_table+' Where Name="'+stock_name+'"'
        news_df=pd.read_sql_query(news_q,sq_conn).drop_duplicates().sort_values(by='date', ascending=False)
        news_df.reset_index(inplace = True)
        return news_df
    except:
        st.error("Something were wrong......")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.warning("Please check connection.....")
            st.experimental_rerun()


# funtion to collect senti
def sentiment_df(stock_name):
    try:
        senti_q='SELECT * FROM '+sentiment_table+' Where stock_name="'+stock_name+'"'
        senti_q_df_list=pd.read_sql_query(senti_q,sq_conn).drop_duplicates()
        return senti_q_df_list
    except:
        st.error("Something were wrong......")
        if not st.session_state["sq_cur_obj"]:
            st.session_state["sq_cur_obj"]=0   
            time.sleep(2)
            st.warning("Please check connection.....")
            st.experimental_rerun()

# function hold selected stockname
def button(comp_name):
    with st.spinner("Loading...."):
        st.session_state['comp_name']=comp_name
        st.session_state['news_df']=news_df(st.session_state['comp_name'])
        st.session_state['sentiment_df']=sentiment_df(st.session_state['comp_name'])
        st.session_state['styled_news_df']=st.session_state['news_df'][['title','compound','date','link']].style.applymap(color_negative_red,subset=['compound'])
        st.experimental_rerun()
_="""

    Sessio state variables

"""

_=""" Variables"""

if 'stocks_names' not in st.session_state:
    st.session_state['stocks_names']=names(news_table,sq_conn)

# 1.comp name
if 'comp_name' not in st.session_state:
    st.session_state['comp_name']=st.session_state['stocks_names'].iloc[0]['Name']


if 'news_df' not in st.session_state:
    st.session_state['news_df']=news_df(st.session_state['comp_name'])

if 'sentiment_df' not in st.session_state:
    st.session_state['sentiment_df']=sentiment_df(st.session_state['comp_name'])

if 'styled_news_df' not in st.session_state:
    st.session_state['styled_news_df']=st.session_state['news_df'][['title','compound','date','link']].style.applymap(color_negative_red,subset=['compound'])

_="""  Buttons """

# 1. search button
if 'search_button' not in st.session_state:
    st.session_state['search_button'] = False
        
def search_button_callback():
    st.session_state['search_button'] = True


#with tab_1:
title_col[2].write('')    
title_col[2].write('')  

comp_name=title_col[1].selectbox("",options=st.session_state['stocks_names'])
#if (s_col[2].button("@",on_click=search_button_callback) or st.session_state['search_button']):
if title_col[2].button("🔍"):
    button(comp_name)



ind=np.where(st.session_state['stocks_names']['Name']==st.session_state['comp_name'])
s_col[0].header('🏢 {}'.format(st.session_state['comp_name']))
s_col[0].subheader("📝 Sentiment Analysis")
s_col1[0].subheader("🧾 News")
s_col1[2].download_button("📥Export", data=st.session_state['news_df'].to_csv(), file_name=st.session_state['comp_name']+'.csv')


if s_col1[3].button("Delete"):
    if st.session_state['comp_name'] in st.session_state['stocks_names']['Name'].values.tolist():
        #l = "DELETE FROM " + filter_table + " WHERE user='" + st.session_state["username"] + "' and date_time='" + str(user_table_date) + "'"
        cheak_stocks_name_q='DELETE From '+news_table+' Where Name="'+st.session_state['comp_name']+'"'
        cheak_stocks_sentiment_q='DELETE From '+sentiment_table+' Where stock_name="'+st.session_state['comp_name']+'"'
        sq_cur.execute(text(cheak_stocks_name_q))
        sq_cur.execute(text(cheak_stocks_sentiment_q))
        st.experimental_rerun()
    else:
        st.error("No data found")
        time.sleep(1)
        st.experimental_rerun()


if len(st.session_state['sentiment_df'])>0:
    senti_q_df_list=st.session_state['sentiment_df'].copy()           
    s_col4[0].markdown("##### 3 Months:{}".format(flag(senti_q_df_list.iloc[0]['news_senti_3_M'])))
    s_col4[1].markdown("##### 1 Months:{}".format(flag(senti_q_df_list.iloc[0]['news_senti_1_M'])))
    s_col4[2].markdown("##### 7 Days:{}".format(flag(senti_q_df_list.iloc[0]['news_senti_7_d'])))
    s_col4[3].markdown("##### 2 Days:{}".format(flag(senti_q_df_list.iloc[0]['news_senti_2_d'])))


    s_col3[0].metric("polarity","{:.4f}".format(senti_q_df_list.iloc[0]['polarity']))
    s_col3[1].metric("positive","{:.4f}".format(senti_q_df_list.iloc[0]['positive_score']))
    s_col3[2].metric("negative","{:.4f}".format(senti_q_df_list.iloc[0]['negative_score']))
    s_col3[3].metric("net_score","{:.4f}".format(senti_q_df_list.iloc[0]['net_score']))

    st.write("")
    st.write("")
else:
    st.error("Sorry Sentiment data not found")


st._legacy_dataframe(st.session_state['styled_news_df'])


#with tab_2:
#    st.write('collecting')


