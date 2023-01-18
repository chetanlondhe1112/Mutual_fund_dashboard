# Home page for mutual fund
import time
import streamlit as st
import pandas as pd
from PIL import Image
from datetime import datetime
import streamlit_authenticator as stauth
import mysql.connector
from sqlalchemy import create_engine
from validate_email_address import validate_email
from password_validator import PasswordValidator
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re


current_date = datetime.now()
global conn,sq_conn,cur

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

with open('CSS/homestyle.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

_ = """
    to setup mysql connection Creating connection
"""
if "conn" not in st.session_state:
    st.session_state["conn"] = ""

def init_msconnection():
    return mysql.connector.connect(**st.secrets["mysql3"])

#@st.cache(allow_output_mutation=True,show_spinner=False,suppress_st_warning=True)
def init_sqconnection():
    connect_string = "mysql://{}:{}@{}:{}/{}".format(st.secrets["mysql3"]["user"],
                                                st.secrets["mysql3"]["password"],
                                                st.secrets["mysql3"]["host"],
                                                st.secrets["mysql3"]["port"],
                                                st.secrets["mysql3"]["database"])
    return create_engine(connect_string)

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
    

if len(st.session_state) == 0:
    st.warning("Please! Don't Refresh your browser.")
    st.info("Always, use dashboard Default Refresh!")
    with st.expander('Do This:'):
        video_file = open('video/dashboard_refresh.mp4', 'rb')
        video_bytes = video_file.read()
        st.video(video_bytes)
    st.stop()

try: 
    st.session_state["conn"] = init_msconnection()
    conn=st.session_state["conn"]
except:
    st.warning("Connection lost")
    st.stop()

if "username" not in st.session_state:
    st.session_state["username"]=""
username=st.session_state["username"]

if conn.is_connected():
        sq_conn = init_sqconnection()
        cur = conn.cursor()
        admin = st.secrets["admin_ch"]["admin_ch"]
        master_table = st.secrets["db_table"]["master_table"]
        filter_table = st.secrets["db_table"]["filter_table"]
        query_table = st.secrets["db_table"]["query_table"]
        user_table = st.secrets["db_table"]["user_table"]
        portfolio_table = st.secrets["db_table"]["portfolio_table"]
        news_table = st.secrets["db_table"]["news_table"]
        sentiment_table = st.secrets["db_table"]["sentiment_table"]

        mf_master_table=st.secrets["db_table_2"]["mf_master_table"]
        
        #@st.cache(suppress_st_warning=True,show_spinner=False)
        def main():

                if 'authentication_status' not in st.session_state:
                    st.session_state['authentication_status'] = ""
                #if "username" not in st.session_state:
                #    st.session_state["username"]=""
                if "lable" not in st.session_state:
                    st.session_state["lable"] = ""

                if 'login' not in st.session_state:
                    st.session_state["login"]=False
                def login_callback():
                    st.session_state["login"] =True

                if 'create_acc' not in st.session_state:
                    st.session_state["create_acc"]=False
                def create_acc_callback():
                    st.session_state["create_acc"] = True

                if 'forget_password' not in st.session_state:
                    st.session_state["forget_password"]=False
                def forget_password_callback():
                    st.session_state["forget_password"] = True

                if 'user_id' not in st.session_state:
                    st.session_state["user_id"]=''

                if 'send_otp' not in st.session_state:
                    st.session_state["send_otp"]=False
                def send_otp_callback():
                    st.session_state["send_otp"] = True
                
                if 'OTP' not in st.session_state:
                    st.session_state["OTP"]=0

                if 'otp_verify' not in st.session_state:
                    st.session_state["otp_verify"]=False
                def otp_verify_callback():
                    st.session_state["otp_verify"] = True
                
                if 'values' not in st.session_state:
                    st.session_state["values"]='' 

                if 'reset_password' not in st.session_state:
                    st.session_state["reset_password"]=False
                def reset_password_callback():
                    st.session_state["reset_password"] = True

                col1=st.columns((2,1))
                with col1[0]:
                    col1[0].markdown('# 📈Stocks Analyser')

                    #image = Image.open('stock2.png')
                    image = Image.open('stock3.png')
                    col1[0].image(image, caption='Stocks Analyser',use_column_width=True)


                #and st.session_state["forget_password"]==False
                if st.session_state["login"]==True or st.session_state["create_acc"]==False and st.session_state["forget_password"]==False and  st.session_state["reset_password"]==False:
                    df = pd.read_sql_query("SELECT * FROM " + user_table, conn)
                    names = list(df['id'])
                    usernames = list(df['username'])
                    hashed_passwords = list(df['password'])

                    authenticator = stauth.Authenticate(names, usernames, hashed_passwords, "sfdb", "abcdef", cookie_expiry_days=30)
                    with col1[1]: 
                            names, st.session_state['authentication_status'], username = authenticator.login("Login", "main")
                            if st.session_state["authentication_status"]:     
                                with st.expander("🙎‍♂️{}".format(st.session_state["username"])):
                                #with st.expander("👤"):
                                    authenticator.logout('Logout', 'main')
                                #with col1[1]:
                                #st.markdown(f'### *__Welcome* *{st.session_state["username"]}__*')
                                st.session_state["username"]=username
                                if st.session_state["username"]==admin:
                                    st.session_state["lable"]='admin'
                                else:
                                    st.session_state["lable"]='user'
                                
                            elif st.session_state["authentication_status"] == False:
                                st.error('Username/password is incorrect')
                                st.session_state["forget_password"]=st.button("Forget Password",on_click=forget_password_callback)
                                #st.session_state["forget_password"]=st.checkbox("Forget Password")
                                st.session_state["create_acc"]=st.button("Create Account",on_click=create_acc_callback)
                            #elif st.session_state["authentication_status"] == None:
                            else:
                                #st.warning('Please enter your username and password')

                                st.session_state["forget_password"]=st.button("Forget Password",on_click=forget_password_callback)
                                #st.session_state["forget_password"]=st.checkbox("Forget Password")
                                st.session_state["create_acc"]=st.button("Create Account",on_click=create_acc_callback)

                #elif process == "Sign Up":
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

                                       
        if __name__ == '__main__':
                main()

 