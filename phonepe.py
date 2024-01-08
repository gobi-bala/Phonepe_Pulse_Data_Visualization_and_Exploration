import streamlit as st
import googleapiclient.discovery
from pprint import pprint
import pandas as pd
import numpy as np
import re
from datetime import datetime
import pymongo
from pymongo.mongo_client import MongoClient
import mysql.connector

API = "AIzaSyDpXLLHfYjBSOp03UUMgAF-vMW9jEFQtFE"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API)
client = MongoClient("mongodb+srv://sivachandana49230:SChand@cluster0.zsco2ek.mongodb.net/?retryWrites=true&w=majority&appName=AtlasApp")
db = client.GUVI_project1
records = db.YOUTUBE
import sqlite3
con = sqlite3.connect("CAPSTONE.db")
mycursor = con.cursor()
st.title('YOUTUBE DATA HARVESTING AND WAREHOUSING')
st.title('USING THE SKILL :rainbow[MONGODB, SQL, STREAMLIT] :balloon:')



def Channel_detais(C):
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id= C
        )
        response = request.execute()
        data = [{"channel_id" :  response['items'][0] ['id'],
                "channel_Title" :response['items'][0]['snippet']['title'],
                "channel_Description" : response['items'][0]['snippet']['description'],
            "publishedAt" :response['items'][0]['snippet']['publishedAt'].replace('Z',''),
                "uplods":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                "url" : response['items'][0]['snippet']['thumbnails']['default']['url'],
                "videocount" :int(response['items'][0]['statistics']['videoCount']),
            "subscriberCount": int(response['items'][0]['statistics']['subscriberCount']),
            "viewcount" : int(response['items'][0]['statistics']['viewCount'])}]
        return data

def get_playlist_details(C):
        playlistid = []
        request = youtube.playlists().list(part="snippet,contentDetails",channelId= C,maxResults=25)
        response = request.execute()
        for i in response['items']:
            D = { 'playlist_id': i['id'],
                'channelid' : i['snippet']["channelId"],
                'playlist_title' : i['snippet']['title'],
                'pubAt':i['snippet']['publishedAt'].replace('Z',''),
                'pl_video_count': int(i['contentDetails']['itemCount'])}
            playlistid.append(D)

        return playlistid

def duration_to_hms(duration_str):
        if not duration_str.startswith("PT"):
            return "Invalid duration format"

        duration_str = duration_str[2:]

        hours, minutes, seconds = 0, 0, 0

        if 'H' in duration_str:
            hours_part, duration_str = duration_str.split('H')
            hours = int(hours_part)
        if 'M' in duration_str:
            minutes_part, duration_str = duration_str.split('M')
            minutes = int(minutes_part)
        if 'S' in duration_str:
            seconds = int(duration_str.split('S')[0])

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def get_Video_Details(playlistid):
        videoids =[]
        l =[]
        nextpage = None
        while True:
         request = youtube.playlistItems().list(part = "snippet ,contentDetails",playlistId = playlistid,maxResults = 50,pageToken = nextpage)
         response = request.execute()
         for i in response['items']:
            videoid = i['contentDetails']["videoId"]
            videoids.append(videoid)
         nextpage = response.get('nextPageToken')
         if nextpage is None:
          break
        for videoid in videoids:
         request = youtube.videos().list(
                part="snippet,contentDetails,statistics",id= videoid)
         response = request.execute()
         l.append({'video_id':response['items'][0]['id'] ,
                   'channelid':response['items'][0]['snippet']['channelId'],
                'video_title': response['items'][0]['snippet']['title'],
                'Des' : response['items'][0]['snippet']['description'],
                'Comment_count': int(response['items'][0]['statistics']['commentCount']),
                'Duration' : duration_to_hms(response['items'][0]['contentDetails']['duration'])if response['items'][0]['contentDetails'].get('duration')is not None else np.nan,
                'LikeCount': int(response['items'][0]['statistics']['likeCount']),
                'ViewCount' : int(response['items'][0]['statistics']['viewCount']),
                'pubAt':response['items'][0]['snippet']['publishedAt'].replace('Z',''),
                'Thmb': response['items'][0]['snippet']['thumbnails']['default']['url']})
        return videoids,l

def commentThreads_Details(videoids):
        Comments =[]
        for i in videoids:
         try:
            request = youtube.commentThreads().list(part="snippet,replies",videoId= i,maxResults = 50)
            response = request.execute()
            if len(response['items'])>0:
             for j in range(len(response['items'])):
              Comments.append({'commentid' :response['items'] [j]['id'],
             'Autname':response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
             'text' :response['items'][j]['snippet']['topLevelComment']['snippet']['textOriginal'],
             'Videoid' : i,
             'pA' :response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'].replace('Z',''),
                'cmt_likes':int(response['items'][j]['snippet']['topLevelComment']['snippet']['likeCount'])})
         except:
            Comments.append({'video_id': i,'comment_author':np.nan,'cmt_text':np.nan,'cmt_like':np.nan})

        return Comments

def main(C):

        ch = Channel_detais(C)
        p = get_playlist_details(C)
        V = get_Video_Details(ch[0]['uplods'])
        ct = commentThreads_Details(V[0])

        Details = {'channel_details':ch,
                    'playlist_details': p,
                    'Video_Details': V[1],
                    'commentThreads_Details': ct}
        return Details


add_selectbox = st.sidebar.selectbox("select_option" ,["EXTRACT DATA", "MIGRATE DATA", "QUERY DATA"])
if add_selectbox == "EXTRACT DATA":
    C = st.text_input('ENTER THE CHANNEL ID')
    if C and st.button('scrape'):    
     D1= main(C)
     
     records.insert_one(D1)
     st.success('successfully stored in MONGODB', icon="✅")
elif add_selectbox == "MIGRATE DATA":
    myclient = pymongo.MongoClient("mongodb+srv://sivachandana49230:SChand@cluster0.zsco2ek.mongodb.net/?retryWrites=true&w=majority&appName=AtlasApp")
    mydb = myclient["GUVI_project1"]
    mycol = mydb["YOUTUBE"]
    channel_names =[]
    for names in mycol.find({},{"channel_details":1}):
        channel_names.append(names['channel_details'][0]['channel_Title'])
    option = st.selectbox("SELECT CHANNEL_NAME",(channel_names))
    
    
    mycursor.execute('''CREATE TABLE IF NOT EXISTS CHANNEL_DETAILS( 
                channel_id VARCHAR(255),
                channel_Title TEXT,
                channel_Description TEXT, 
                publishedAt DATETIME,
                uplods VARCHAR(255),
                url VARCHAR(255),
                videocount INT,
                subscriberCount INT,
                viewcount INT)''')
    con.commit()
    mycursor.execute('''CREATE TABLE IF NOT EXISTS PLAYLIST_DETAILS( 
                    playlist_id VARCHAR(255),
                    channel_id VARCHAR(255),
                    playlist_title VARCHAR(255),
                    pubAt DATETIME,
                    pl_video_count INT)''')
    con.commit()
    mycursor.execute('''CREATE TABLE IF NOT EXISTS VIDEO_DETAILS(
                    video_id VARCHAR(255),
                    channelid VARCHAR(255),
                    video_title VARCHAR(255),
                    Des TEXT,             
                    Comment_count INT, 
                    Duration DATETIME,
                    LikeCount INT,
                    ViewCount INT,
                    pubAt DATETIME,
                    Thmb VARCHAR(255))''')
    con.commit()
    mycursor.execute('''CREATE TABLE IF NOT EXISTS COMMENT_DETAILS( 
                    commentid VARCHAR(255),
                    Autname VARCHAR(255),
                    text TEXT,
                    Videoid VARCHAR(255),
                    pA DATETIME,
                    cmt_likes INT)''') 
    con.commit()
    def SQL_table(youtube_details):
        sql_table = '''INSERT INTO CHANNEL_DETAILS(channel_id,channel_Title,channel_Description,publishedAt,uplods,url,videocount,subscriberCount,viewcount) VALUES(?,?,?,?,?,?,?,?,?)'''
        for i in youtube_details["channel_details"]:
            values = tuple(i.values())
            mycursor.execute(sql_table,values)
            
        sql_table1 = '''INSERT INTO PLAYLIST_DETAILS(playlist_id,channel_id,playlist_title,pubAt,pl_video_count) VALUES(?,?,?,?,?)'''
        for i in youtube_details["playlist_details"]:
            values = tuple(i.values())
            mycursor.execute(sql_table1,values)
        
        sql_table2 = '''INSERT INTO VIDEO_DETAILS(video_id,channelid,video_title,Des,Comment_count,Duration,LikeCount,ViewCount,pubAt,Thmb) VALUES(?,?,?,?,?,?,?,?,?,?)'''
        for i in youtube_details["Video_Details"]:
            values = tuple(i.values())
            mycursor.execute(sql_table2,values)
        
        sql_table3 = '''INSERT INTO COMMENT_DETAILS(commentid,Autname,text,Videoid,pA,cmt_likes) VALUES(?,?,?,?,?,?)'''
        for i in youtube_details["commentThreads_Details"]:
            values = tuple(i.values())
            mycursor.execute(sql_table3,values)   
        con.commit()

    if st.button("MIGRATE"):
      doc = mycol.find_one({"channel_details.channel_Title":option},{'_id':0})
      SQL_table(doc)     
      st.success('successfully MIGRATE TO SQL', icon="✅")
elif add_selectbox == "QUERY DATA":
     add_selectbox = st.sidebar.selectbox("select_option",
    ["1.What are the names of all the videos and their corresponding channels?" , 
    "2.Which channels have the most number of videos, and how many videos dothey have?",
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
    "8.What are the names of all the channels that have published videos in the year 2022?",
    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"])
     
     if add_selectbox == "1.What are the names of all the videos and their corresponding channels?":
         
        mycursor.execute("""SELECT VIDEO_DETAILS.video_title,CHANNEL_DETAILS.channel_Title
        FROM VIDEO_DETAILS
        JOIN CHANNEL_DETAILS ON VIDEO_DETAILS.channelid= CHANNEL_DETAILS.channel_id;""")
        out =mycursor.fetchall()
        Q1 = pd.DataFrame(out,columns = ['video_title','channel_Title'])  
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
        Q1
     elif add_selectbox == "2.Which channels have the most number of videos, and how many videos dothey have?":
         mycursor.execute ("""SELECT
            CHANNEL_DETAILS.channel_Title,
            COUNT(VIDEO_DETAILS.video_id) AS number_of_videos
            FROM CHANNEL_DETAILS
            JOIN VIDEO_DETAILS ON CHANNEL_DETAILS.channel_id = VIDEO_DETAILS.channelid 
            GROUP BY CHANNEL_DETAILS.channel_Title
            ORDER BY number_of_videos DESC;""")
         out = mycursor.fetchall()
         Q2 = pd.DataFrame(out,columns = ['channel_Title','number_of_videos'])  
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q2
     elif add_selectbox == "3.What are the top 10 most viewed videos and their respective channels?":
         mycursor.execute("""SELECT
            VIDEO_DETAILS.video_title AS VideoTitle,
            VIDEO_DETAILS.viewcount AS ViewCount,
            CHANNEL_DETAILS.channel_Title AS ChannelName
            FROM VIDEO_DETAILS
            JOIN CHANNEL_DETAILS
            ON VIDEO_DETAILS.channelid = CHANNEL_DETAILS.channel_id
            ORDER BY VIDEO_DETAILS.viewcount DESC
            LIMIT 10;""")
         out =mycursor.fetchall()
         Q3 = pd.DataFrame(out,columns = ['VideoTitle','ViewCount','ChannelName']) 
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q3
     elif add_selectbox == "4.How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("""SELECT VIDEO_DETAILS.video_title, COUNT(COMMENT_DETAILS.commentid) AS comment_count
            FROM VIDEO_DETAILS
            LEFT JOIN COMMENT_DETAILS ON VIDEO_DETAILS.video_id 
            GROUP BY VIDEO_DETAILS.video_id, VIDEO_DETAILS.video_title;""")
        out =mycursor.fetchall()
        Q4 = pd.DataFrame(out,columns = ['video_title','comment_count'])  
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
        Q4
     elif add_selectbox == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
         mycursor.execute("""SELECT VIDEO_DETAILS.video_title, VIDEO_DETAILS.LikeCount, CHANNEL_DETAILS.channel_Title
                FROM VIDEO_DETAILS
                INNER JOIN CHANNEL_DETAILS ON VIDEO_DETAILS.channelid= CHANNEL_DETAILS.channel_id
                ORDER BY VIDEO_DETAILS.LikeCount DESC
                LIMIT 5;""")
         out =mycursor.fetchall()
         Q5 = pd.DataFrame(out,columns = ['video_title','LikeCount','channel_Title']) 
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q5
     elif add_selectbox == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
         mycursor.execute ("""SELECT VIDEO_DETAILS.video_title,
                             SUM(LikeCount) AS total_likes
                                FROM  VIDEO_DETAILS
                                GROUP BY VIDEO_DETAILS.video_title
                                LIMIT 10;""")
         out =mycursor.fetchall()
         Q6 = pd.DataFrame(out,columns = ['video_title',' total_likes']) 
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q6
     elif add_selectbox == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
         mycursor.execute ("""SELECT CHANNEL_DETAILS.channel_Title, SUM(VIDEO_DETAILS.viewcount) AS total_views
                        FROM CHANNEL_DETAILS
                        JOIN VIDEO_DETAILS ON CHANNEL_DETAILS.channel_id = VIDEO_DETAILS.channelid
                        GROUP BY CHANNEL_DETAILS.channel_Title;""")
         out =mycursor.fetchall()
         Q7 = pd.DataFrame(out,columns = ['channel_Title',' total_views']) 
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q7
     elif add_selectbox == "8.What are the names of all the channels that have published videos in the year 2022?":
         mycursor.execute("""
                            SELECT DISTINCT
                                strftime('%Y', substr(VIDEO_DETAILS.pubAt, 1, instr(VIDEO_DETAILS.pubAt, 'T') - 1)) AS Year,
                                CHANNEL_DETAILS.channel_Title AS ChannelName
                            FROM CHANNEL_DETAILS
                            INNER JOIN VIDEO_DETAILS ON CHANNEL_DETAILS.channel_id = VIDEO_DETAILS.channelid
                            WHERE strftime('%Y', substr(VIDEO_DETAILS.pubAt, 1, instr(VIDEO_DETAILS.pubAt, 'T') - 1)) = '2022';
                            """)
         out =mycursor.fetchall()
         Q8 = pd.DataFrame(out,columns = ['year','channel_name']) 
                        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q8
     elif add_selectbox == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
         
         mycursor.execute("""SELECT CHANNEL_DETAILS.channel_Title, 
                            time(
                                AVG(
                                    CAST(SUBSTR(VIDEO_DETAILS.duration, 1, 2) AS INTEGER) * 3600 +
                                    CAST(SUBSTR(VIDEO_DETAILS.duration, 4, 2) AS INTEGER) * 60 +
                                    CAST(SUBSTR(VIDEO_DETAILS.duration, 7, 2) AS INTEGER)
                                ), 'unixepoch') as avg_duration
                                    FROM CHANNEL_DETAILS
                                    JOIN VIDEO_DETAILS  ON CHANNEL_DETAILS.channel_id = VIDEO_DETAILS.channelid
                                    GROUP BY CHANNEL_DETAILS.channel_Title;""")
         out =mycursor.fetchall()
         Q9 = pd.DataFrame(out,columns = [' channel_name','average_duration']) 
        # print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q9
     elif add_selectbox =="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
         mycursor.execute("""SELECT VIDEO_DETAILS.video_title, CHANNEL_DETAILS.channel_Title, VIDEO_DETAILS.Comment_count
                                FROM VIDEO_DETAILS
                                JOIN CHANNEL_DETAILS ON VIDEO_DETAILS.channelid = CHANNEL_DETAILS.channel_id
                                ORDER BY VIDEO_DETAILS.Comment_count DESC
                                LIMIT 10;""")
         out =mycursor.fetchall() 
         Q10= pd.DataFrame(out,columns = ['video_title','channel_Title','Comment_count'])
        #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))
         Q10
    

    

