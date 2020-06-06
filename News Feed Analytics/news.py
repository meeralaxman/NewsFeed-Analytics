import pandas as pd
import json
import glob
import sys
import boto3
import datetime as dt
#add the code for copying the files from S3 to local 

#read the data from all 3 tsv files and store it in a single file 
data = pd.concat([pd.read_csv(f,delimiter='\t',encoding='utf-8') for f in glob.glob("C:/Users/mlaxman/Desktop/Upday/test/*.tsv")], ignore_index = True)
print("full data is......",len(data))

#dropping the rows which has empty cells.
data.dropna(inplace=True)
data.reset_index(drop=True,inplace=True)

#taking the attributes column values
attr=data.iloc[:,4].to_frame()

js = [eval(item) for item in attr['ATTRIBUTES']]

#converting columns to upper case and removing spaces
new_js=[]
for i in range(len(js)):
    new_js.append({(k.upper()).replace(' ', ''):v for k, v in js[i].items()})
    

df2 = pd.DataFrame.from_dict(new_js)
final= pd.concat([data,df2],axis=1)
final.drop(['ATTRIBUTES'],axis=1,inplace=True)

#dropping the rows for which the articleID is empty
final= final.dropna(subset=['ID'])
final.reset_index(drop=True,inplace=True)

#Exploading subcatgories list values to new columns
subcats = final['SUBCATEGORIES'].apply(pd.Series)

# rename each variable is tags
subcats= subcats.rename(columns = lambda x : 'SUBCATEGORIES_' + str(x))

combo_data=pd.concat([final[:], subcats[:]], axis=1)
#combo_data.to_csv("exploded_data.csv", encoding='utf-8', index=False)

print("length of combo data is:\n\n",len(combo_data))
user_performance=combo_data[['MD5(USER_ID)','EVENT_NAME','MD5(SESSION_ID)','TIMESTAMP']]
#user_performance.to_csv("user_performance.csv", encoding='utf-8', index=False)

article_performance=combo_data[combo_data.columns[~combo_data.columns.isin(['MD5(SESSION_ID)','MD5(USER_ID)'])]]
#article_performance.to_csv("article_performance.csv", encoding='utf-8', index=False)

#converting string to datetime
combo_data['TIMESTAMP'] = combo_data['TIMESTAMP'].apply(lambda x : pd.to_datetime(str(x)))
combo_data['DATE'] = combo_data['TIMESTAMP'].dt.date


daily_clicks= combo_data[combo_data.EVENT_NAME == 'article_viewed'].reset_index(drop=True)

#Functions for calculating aggregations
	
def generic_agg(df,newcol,groupbycol1,groupbycol2):
    df[newcol]=df.groupby([groupbycol1,groupbycol2]).transform("count")
    df.drop_duplicates(inplace=True)
    return df

def generic_ctr(df,newcol,num,denum):
   df[newcol]=df[num]/df[denum]
   df=df.round({ "CTR":2})
   return df 


#Calculating daily clicks for each article id
df1=generic_agg(daily_clicks[['ID','DATE','EVENT_NAME']].copy(),'clicks','ID','DATE')
#df1.to_csv("daily_clicks_article_Id.csv", encoding='utf-8', index=False)


#calculating Cumulative Clicks throughout the day for each articleid 
df2=generic_agg(daily_clicks[['ID','TIMESTAMP','EVENT_NAME']].copy(),'clicks','ID','TIMESTAMP')
df3=df2[['ID','TIMESTAMP','clicks']].copy()
df3=df3.set_index(['ID', 'TIMESTAMP'])
df3["cum_clicks"]=df3.groupby(['ID', 'TIMESTAMP']).sum().fillna(0).groupby(level=0).cumsum()
df3=df3.sort_values(['ID', 'TIMESTAMP']).reset_index()
#df3.to_csv("cumulative_clicks_article_Id.csv", encoding='utf-8', index=False)


#calculating CTR 
daily_displays= combo_data[(combo_data.EVENT_NAME == 'my_news_card_viewed') | (combo_data.EVENT_NAME == 'top_news_card_viewed')].reset_index(drop=True)

#Calculating daily displays for each article id
df4=generic_agg(daily_displays[['ID','DATE','EVENT_NAME']].copy(),'displays','ID','DATE')

#Calculating the click through rate per day per articleid
clicks_displays=pd.merge(df1, df4, on=['ID','DATE'], how='inner')
clicks_displays=generic_ctr(clicks_displays,'CTR','clicks','displays')
#clicks_displays[["ID","DATE","CTR"]].to_csv("ctr_article_Id.csv", encoding='utf-8', index=False)

#BEGIN-Calculating metrics per userid

#calcuating the number of sessions per each userid
daily_sessions= combo_data[['MD5(USER_ID)','DATE','TIMESTAMP','MD5(SESSION_ID)']].copy()
daily_sessions.drop_duplicates(inplace=True)
daily_sessions['sessions']=daily_sessions.groupby(['MD5(USER_ID)','DATE','MD5(SESSION_ID)']).transform("count")

daily_sessions_uniq=daily_sessions[['MD5(USER_ID)','DATE','sessions']].drop_duplicates()
#daily_sessions_uniq.to_csv("daily_sessions_per_userid.csv", encoding='utf-8', index=False)

#Keeping only session which have more than 1 session as to calcuate the session duration
daily_sessions=daily_sessions[daily_sessions.sessions > 1 ]


#calculating the time difference between sessions
daily_sessions= daily_sessions.sort_values(by=['MD5(USER_ID)','TIMESTAMP'])
daily_sessions['time_diff'] = daily_sessions.groupby(['MD5(USER_ID)','DATE','MD5(SESSION_ID)'])['TIMESTAMP'].diff()


#chaning the time difference to seconds
daily_sessions['time_diff'] =daily_sessions.time_diff.dt.total_seconds()
time_duration=daily_sessions.reset_index().groupby(['MD5(USER_ID)','DATE','MD5(SESSION_ID)'])['time_diff'].sum()
daily_sessions.drop(['TIMESTAMP','time_diff'], axis=1, inplace=True)
daily_sessions.drop_duplicates(inplace=True)


daily_sessions["session_duration(Seconds)"]=list(time_duration)
daily_sessions["avg_Session_Length(Seconds)"]= daily_sessions['session_duration(Seconds)'] / daily_sessions['sessions']
#daily_sessions.to_csv("daily_avg_session_length.csv", encoding='utf-8', index=False)


user_clicks=generic_agg(daily_clicks[['MD5(USER_ID)','DATE','EVENT_NAME']].copy(),'clicks','MD5(USER_ID)', 'DATE')
#user_clicks.to_csv("daily_clicks_user_id.csv", encoding='utf-8', index=False)

#calculating CTR for userid

#Calculating daily displays for each userid
user_displays=generic_agg(daily_displays[['MD5(USER_ID)','DATE','EVENT_NAME']].copy(),'displays','MD5(USER_ID)', 'DATE')
user_displays=user_displays[['MD5(USER_ID)','DATE','displays']]
user_displays.drop_duplicates(inplace=True)
print("Number of Displays per userid per day is ...............\n\n",user_displays.head(10))
#user_displays.to_csv("daily_displays_user_id.csv", encoding='utf-8', index=False)


user_clicks_displays=pd.merge(user_clicks,user_displays, on=['MD5(USER_ID)','DATE'], how='inner')
user_clicks_displays=generic_ctr(user_clicks_displays,'CTR','clicks','displays')
#user_clicks_displays[['MD5(USER_ID)',"DATE","CTR"]].to_csv("ctr_user_Id.csv", encoding='utf-8', index=False)



