import pandas as pd
from datetime import datetime
current_date = datetime.now()
print(current_date)
print(current_date)

df=pd.read_csv("master_filterV5.1.csv", index_col=0)
#df=df.drop('date_time',axis=1)
#df=df.drop('lable',axis=1)
#df=df.drop('user', axis=1)

df.insert(0,"date_time",current_date)
#df.insert(1,"time",current_date.time())
df.insert(1,"lable",'admin')
df.insert(2,"user",'chetan')
df.to_csv("master_filterV5.2.csv")
print(df)
