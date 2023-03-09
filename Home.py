import time
import streamlit as st
import pandas as pd
from PIL import Image
from datetime import datetime
import streamlit_authenticator as stauth
from sqlalchemy import create_engine
from validate_email_address import validate_email
from password_validator import PasswordValidator
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re


_=""" Defaults"""

current_date = datetime.now()


_="""
    
    Page layout design
    
"""


# Set page config
st.set_page_config(
        page_title="Stocks Analyser",
        page_icon='📈',
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            #'Report a bug': "https://www.stockanalyser2022@gmail.com/",
            'About': "# Stocks Analyser!"
                    "\n ##### Report a bug on mail:"
                    "\n ##### https://www.stockanalyser2022@gmail.com/"
        }
    )

# CSS file
with open('CSS/homestyle.css') as f:
   st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


#Page columns layout
if "Home_Image" not in st.session_state:
    st.session_state["Home_Image"]=Image.open('stock3.png')


col1=st.columns((2,1))
with col1[0]:
    col1[0].markdown('# 📈Stocks Analyser')
    col1[0].image(st.session_state["Home_Image"], caption='Stocks Analyser',use_column_width=True)



_="""

    Database credentials 

"""   # fetching from .streamlit folder
# connection credentials
_user=st.secrets["mysql4"]["user"]
_password=st.secrets["mysql4"]["password"],
_host=st.secrets["mysql4"]["host"],
_port=st.secrets["mysql4"]["port"],
_database=st.secrets["mysql4"]["database"]
# Equity tables credentials
admin = st.secrets["admin_ch"]["admin_ch"]
master_table = st.secrets["db_table"]["master_table"]
filter_table = st.secrets["db_table"]["filter_table"]
query_table = st.secrets["db_table"]["query_table"]
user_table = st.secrets["db_table"]["user_table"]
portfolio_table = st.secrets["db_table"]["portfolio_table"]
news_table = st.secrets["db_table"]["news_table"]
sentiment_table = st.secrets["db_table"]["sentiment_table"]
# Mutual Fund table credentials
mf_sheet_table=st.secrets["db_table"]["mf_sheet_table"]
mf_filter_table=st.secrets["db_table"]["mf_filter_table"]
mf_rolling_return_table=st.secrets["db_table"]["mf_rolling_return_table"]



_="""credentials declaration """



_=""" 

    Creating Connection 

"""
# Connection defaults
if "serverout_time" not in st.session_state:
    st.session_state["serverout_time"]=0

# SQL alchemy  connection function
def sqlalchemy_connection():
    try:
        connect_string = "mysql://{}:{}@{}:{}/{}".format(st.secrets["mysql4"]["user"],
                                            st.secrets["mysql4"]["password"],
                                            st.secrets["mysql4"]["host"],
                                            st.secrets["mysql4"]["port"],
                                            st.secrets["mysql4"]["database"])
        return create_engine(connect_string)
    except:
        error="No database passed to function!!!"
        return error

# Session state objects of database connection 
if "sq_connection_obj" not in st.session_state:                                 # session state SQL connection object         
    st.session_state["sq_connection_obj"]=0
if "sq_cur_obj" not in st.session_state:                                        # session state SQL cursor object
    st.session_state["sq_cur_obj"]=0

with col1[1]:
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
        #with st.expander("Connection objects:"):
        #    st.write("SQLAlchemy connection:{}".format(sq_conn))
        #    st.write("SQLAlchemy curser:{}".format(sq_cur))
        #    st.write("check [link](http://localhost:8501/upload_test2)")"""
   


_="""

    Function Definations

"""

_=""" Supportive function """

# funtcion to get refress data
def refresh_data():
    video_file = open('video/dashboard_refresh.mp4', 'rb')
    video_bytes = video_file.read()
    return video_bytes    

# Function to get user table data
def user_table_data(user_table,connection):
    try:
        df = pd.read_sql_query("SELECT * FROM " + user_table, connection)
        names = list(df['id'])
        usernames = list(df['username'])
        hashed_passwords = list(df['password'])
        return names,usernames,hashed_passwords
    except:
        st.session_state["sq_cur_obj"]=0
        st.experimental_rerun()

# Function for :Reset Password
@st.experimental_memo(suppress_st_warning=True,show_spinner=False)
def reset_password_mail_verfication(receiver_email):
    import smtplib, ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    import math
    import random
    digits="0123456789"
    OTP=""
    for i in range(6):
        OTP+=digits[math.floor(random.random()*10)]
    otp = OTP + " is your OTP"
    msg= otp
    print(msg)

    sender_email = "stockanalyser2022@gmail.com"
    sender_password = "ehsqeuyvxmphxpkz"


    message = MIMEMultipart("alternative")
    message["Subject"] = "Your OTP for password reset"
    message["From"] = sender_email
    message["To"] = receiver_email

    # Create the plain-text and HTML version of your message
    text = """\
    Welcome,to the Stock_analyser!

    Hello,
    Here is your OTP
    to reset password:
      {OTP}

    Best of luck, 
    for your best strategies for stock market.""".format(OTP=OTP)
    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with st.spinner("sending OTP to your Mail-Id"):
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, message.as_string())
            st.success("Sent")
            time.sleep(2)
    return OTP

# Functio to send user credentials after accounc creation
def password_mail(mail_id,username,password):
        import smtplib, ssl
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        username = username
        password = password
        sender_email = "stockanalyser2022@gmail.com"
        sender_password = "ehsqeuyvxmphxpkz"
        receiver_email = mail_id

        message = MIMEMultipart("alternative")
        message["Subject"] = "Your Stock-analysers Password"
        message["From"] = sender_email
        message["To"] = receiver_email

        # Create the plain-text and HTML version of your message
        text = """\
        Hello,{user}
        Welcome,to the Stock_analyser!
        Here is your password:
        {password}

        Best of luck, 
        for your best strategies for stock market.""".format(user=username, password=password)
        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(part1)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            with st.spinner("your password is sending to your mail..."):
                time.sleep(2)
                server.login(sender_email, sender_password)
                server.sendmail(
                        sender_email, receiver_email, message.as_string()
                    )
                time.sleep(3)
                st.success("sent")
                st.write("please cheak in you'r spam")
                st.experimental_rerun()

# User validation
def user_validate(mail_id=None,username=None,password=None):
    def mail_validation(mail_id=None):
        m="Sorry,mail-id doesn't exist!"
        isexists = validate_email(mail_id, verify=False)
        if isexists == True:
            return True, 0
        else:
            return False,m

    def mail_validation_2(mail_id=None):
        exist_error="Sorry,mail-id doesn't exist!"
        invalid_error="Sorry,mail-id is'nt valid!"
        email_condition="^[a-z]+[\._]?[a-z 0-9]+[@]\w+[.]\w{2,3}$"
        if re.search(email_condition,mail_id):
            return True, 0
        else:
            return False,invalid_error

    def password_validate(password=None):
       
        t='Password must contains-\n Uppercase,lowercase,digits,special characters; Must be 8 to 12 digit long; \n Not allowed-\n Spaces'
        
        # Create a schema
        schema = PasswordValidator()
        # Add properties to it
        schema\
        .min(8)\
        .max(12)\
        .has().uppercase()\
        .has().lowercase()\
        .has().digits()\
        .has().no().spaces()\
        
        return schema.validate(password),t


    def username_validate(username=None):
            t='Username should contain-\n characters, special characters \n; Not allowed-\n alphanmeric'
            ALPHANUM=re.compile('^[a-zA-Z0-9_.-]+$')
            if ALPHANUM.match(username):
                return False,t
            else:
                return True,0
    if mail_id and username and password:
        mail_v,mail_e=mail_validation_2(mail_id)
        user_v,user_e=username_validate(username)
        pass_v,pass_e=password_validate(password)
        return mail_v,mail_e,user_v,user_e,pass_v,pass_e
    if mail_id:
        mail_v,mail_e=mail_validation_2(mail_id)
        return mail_v,mail_e
    if username:
        user_v,user_e=username_validate(username)
        return user_v,user_e
    if password:
        pass_v,pass_e=password_validate(password)
        return pass_v,pass_e

def auth_func(names_,uernames_,passwords_):
    authenticator = stauth.Authenticate(names_,uernames_,passwords_,"sfdb", "abcdef", cookie_expiry_days=30)
    names, authentication_status, username = authenticator.login("Login", "main")
    return names,authentication_status,username,authenticator



_="""

    session state

"""

_="""SS_Variables"""

# 0.Image


# 1. User_table data
if "names" and "usernames" and "hashed_passwords" not in st.session_state:
    st.session_state["names"],st.session_state["usernames"],st.session_state["hashed_password"]=user_table_data(user_table,sq_conn)

# 2.Username
if "username" not in st.session_state:
    st.session_state["username"]=""
username=st.session_state["username"]

# 3.Authentication_status
if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] =0
auth_status=st.session_state['authentication_status']

if 'authenticator' not in st.session_state:
    st.session_state['authenticator']=0

# 4.Lable
if "lable" not in st.session_state:
    st.session_state["lable"] = ""

# 5.User Id
if 'user_id' not in st.session_state:
    st.session_state["user_id"]=''

# 6.OTP
if 'OTP' not in st.session_state:
    st.session_state["OTP"]=0

# 7.Values
if 'values' not in st.session_state:
    st.session_state["values"]='' 

_="""SS_Buttons"""
# 1. Loggin 
if 'login' not in st.session_state:
    st.session_state["login"]=False
def login_callback():
    st.session_state["login"] =True

# 2. Create acc
if 'create_acc' not in st.session_state:
    st.session_state["create_acc"]=False
def create_acc_callback():
    st.session_state["create_acc"] = True

# 3.Forget Password
if 'forget_password' not in st.session_state:
    st.session_state["forget_password"]=False
def forget_password_callback():
    st.session_state["forget_password"] = True

# 4.Send_OTP
if 'send_otp' not in st.session_state:
    st.session_state["send_otp"]=False
def send_otp_callback():
    st.session_state["send_otp"] = True

# 5.OTP_verify
if 'otp_verify' not in st.session_state:
    st.session_state["otp_verify"]=False
def otp_verify_callback():
    st.session_state["otp_verify"] = True

# 6.Reset Password
if 'reset_password' not in st.session_state:
    st.session_state["reset_password"]=False
def reset_password_callback():
    st.session_state["reset_password"] = True




#@st.cache(suppress_st_warning=True,show_spinner=False)
def main():


    with col1[1]:
        #authenticator = stauth.Authenticate(st.session_state["names"],st.session_state["usernames"],st.session_state["hashed_password"], "sfdb", "abcdef", cookie_expiry_days=30)
        if not st.session_state["authentication_status"]:
            if st.session_state["login"]==True or st.session_state["create_acc"]==False and st.session_state["forget_password"]==False and  st.session_state["reset_password"]==False:
                names, st.session_state['authentication_status'], username, st.session_state["authenticator"]=auth_func(st.session_state["names"],st.session_state["usernames"],st.session_state["hashed_password"])
                #names, st.session_state['authentication_status'], username = authenticator.login("Login", "main")
                if st.session_state["authentication_status"]: 
                    st.session_state["username"]=username    
                    st.experimental_rerun()
                elif st.session_state["authentication_status"] == False:
                    st.error('Username/password is incorrect')
                    st.session_state["forget_password"]=st.button("Forget Password",on_click=forget_password_callback)
                    st.session_state["create_acc"]=st.button("Create Account",on_click=create_acc_callback)
                else:
                    st.session_state["forget_password"]=st.button("Forget Password",on_click=forget_password_callback)
                    st.session_state["create_acc"]=st.button("Create Account",on_click=create_acc_callback)
            
            if st.session_state["create_acc"] :
                with col1[1].form("sign up"):
                    values=[]
                    st.subheader("Create New Account")
                    user_id = st.text_input("User ID (mail_id)",max_chars=50)
                    username = st.text_input("Username",help="Eg. username=james@SA",max_chars=50)
                    password = st.text_input("Password", type='password',help="Eg. password=James@333",max_chars=100)
                    hashed_password = stauth.Hasher([str(password)]).generate()
                    acc_query = "SELECT * FROM user_login where id='" + str(user_id) + "' OR username='" + username + "'"
                    cur.execute(acc_query)
                    values = cur.fetchall()
                    if st.form_submit_button("Sign Up"):
                        mail_v, mail_e,user_v,user_e,pass_v,pass_e=user_validate(user_id,username,password)
                        if mail_v==False or user_v==False or pass_v==False:
                            if mail_v==False:
                                st.warning("1.invalid mail-id")
                                st.error(mail_e)
                            if user_v==False:
                                st.warning("2.invalid username")
                                st.error(user_e)
                            if pass_v==False:
                                st.warning("3.invalid password")
                                st.error(pass_e)
                        elif values:
                            st.warning("User already exist!,Please try new username/mail_id")
                        else:    
                            add_query = 'insert into `user_login`(`id`,`username`,`password`)VALUES(%s,%s,%s)'
                            cur.execute(add_query, (str(user_id), username, str(hashed_password[0])))
                            conn.commit()
                            st.success("Successfully created.")
                            password_mail(user_id, username, password)
                            st.balloons()
                    
                col1[1].write("Already registerd,")        
                if col1[1].button("Login"):
                    st.session_state["create_acc"]=False
                    st.experimental_rerun()

            if st.session_state["forget_password"]:
                #st.experimental_memo.clear()
                with col1[1].form("Email Verification"):
                    values=[]
                    st.subheader("Email Verification")
                    st.session_state["user_id"] = st.text_input("Enter your registered mail-id")

                    #st.session_state["send_otp"]=st.form_submit_button("Send OTP",on_click=send_otp_callback)
                    #st.session_state["send_otp"]
                    #st.write(st.session_state)
                    
                    if (st.form_submit_button("Send OTP",on_click=send_otp_callback) or st.session_state["send_otp"]  ) and st.session_state["user_id"] :
                        #st.session_state["OTP"]=False
                        if st.form_submit_button("Resend"):
                            st.session_state["OTP"]=0
                            st.experimental_memo.clear()
                            st.session_state["send_otp"]=True
                            st.experimental_rerun()
                        
                        mail_v, mail_e=user_validate(mail_id=st.session_state["user_id"])
                        
                        if mail_v==False:
                            st.error("Invlid Mail_id")
                            time.sleep(2)
                            st.session_state["forget_password"]=False
                            st.experimental_rerun()
                        elif mail_v==True:    
                            acc_query = "SELECT * FROM user_login where id='" + str(st.session_state["user_id"]) + "'" 
                            cur.execute(acc_query)      
                            st.session_state["values"] = cur.fetchall()
                            #st.experimental_rerun()
                            if st.session_state["values"]:
                                if not st.session_state["OTP"]:
                                    st.session_state["OTP"]=reset_password_mail_verfication(st.session_state["user_id"])
                                    st.experimental_rerun()
                                else:    
                                    check_otp=st.text_input("Enter OTP")
                                    #st.session_state["otp_verify"]=st.form_submit_button("Verify",on_click=otp_verify_callback)
                                    if( st.form_submit_button("Verify",on_click=otp_verify_callback) or st.session_state["otp_verify"] ):  
                                        if check_otp==st.session_state["OTP"]:
                                            with st.spinner():
                                                st.session_state["reset_password"]=True
                                                st.session_state["forget_password"]=False
                                                st.session_state["login"]=False
                                                st.success("Verified...")
                                                time.sleep(2)
                                                st.experimental_rerun()
                                        else:
                                            st.error("Incorrect OTP")                          
                            else:
                                st.error("Invalid Mail-ID") 

            if  st.session_state["reset_password"] :
                with col1[1].form("Email Verification"):
                    st.subheader("Reset Password")
                    new_pass=st.text_input(" New password",type="password")
                    conf_pass=st.text_input(" New password confirmation",type="password")
                    
                    
                    if st.form_submit_button("Save Password"):
                        pass_v,pass_e=user_validate(password=conf_pass)
                        
                        if new_pass==conf_pass:
                            if pass_v==False:
                                st.warning("3.invalid password")
                                st.error(pass_e)
                            elif pass_v==True:
                                hashed_password = stauth.Hasher([str(new_pass)]).generate()
                                reset_pass_q="UPDATE user_login SET password = '"+str(hashed_password[0])+"' WHERE id = '"+st.session_state["user_id"]+"'"
                                cur.execute(reset_pass_q)
                                conn.commit()
                                st.success("Password has reset")
                                time.sleep(2)
                                st.session_state["user_id"]=''
                                st.session_state["reset_password"]=False
                                st.experimental_rerun()
                        else:
                            st.error("The two password fields did'nt match.")       

        else:
            authen=st.session_state['authenticator']
                  
            if st.session_state["username"]==admin:
                st.session_state["lable"]='admin'
            else:
                st.session_state["lable"]='user'

            with st.expander("🙎‍♂️{}".format(st.session_state["username"])):

                if authen.logout('Logout', 'main'):
                    st.session_state=0
                    st.experimental_rerun()  

col1[1].info("Username:chetan|Password:Chetan@3333")

if __name__ == '__main__':
        main()

 