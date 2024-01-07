from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection

def Api_connect():
    Api_Id="AIzaSyD3-btnLS0Xlym4W53hDBnG9js_Zmr3xAM"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#Get channel information

def get_channel_info(channel_id):
        response3=youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id).execute()

        for i in response3['items']:
            shan=dict(channel_Name=i["snippet"]["title"],
                    channel_Id=i["id"],
                    Subscribers=i["statistics"]["subscriberCount"],
                    views=i["statistics"]["viewCount"],
                    Total_Videos=i["statistics"]["videoCount"],
                    channel_Discription=i["snippet"]["description"],
                    Playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return shan 

# Get playlist Deatil
def get_playlist_info(channel_id):

    next_page_token=None
    Ply_Data=[]
    while True:

        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for a in response['items']:
            data=dict(Playlist_Id=a["id"],
                    Title=a["snippet"]["title"],
                    channel_Id=a["snippet"]["channelId"],
                    Channel_Name=a["snippet"]["channelTitle"],
                    Publishedat=a["snippet"]["publishedAt"],
                    Video_Count=a["contentDetails"]["itemCount"])
            Ply_Data.append(data)
        next_page_token=response.get("nextPageToken")

        if next_page_token is None:
            break

    return Ply_Data

#getting Video Ids

def get_video_IDS(channel_id):   
    video_ids=[]
    response=youtube.channels().list(
            id=channel_id,
            part="contentDetails").execute()

    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None

    while True:

        response1=  youtube.playlistItems().list(
                    part="snippet",
                    playlistId=playlist_Id,
                    maxResults=50,
                    pageToken=next_page_token).execute()

        for b in range(len(response1["items"])):
                    video_ids.append(response1["items"][b]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get('nextPageToken')    

        if next_page_token is None:
                break        
        
    return video_ids


# Getting video Details
def get_video_info(video_ids):
    Video_Datas=[]

    for video_ID in video_ids:
            response2=youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_ID).execute()
            
            for item in response2["items"]:
                data=dict(channel_Name=item["snippet"]["channelTitle"],
                        channelId=item["snippet"]["channelId"],
                        video_Id=item["id"],
                        Title=item["snippet"]["title"],
                        Tags=item["snippet"].get("tags"),
                        Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                        Description=item["snippet"].get("descption"),
                        published_Date=item["snippet"]["publishedAt"],
                        Duration=item["contentDetails"]["duration"],
                        views=item["statistics"].get("viewCount"),
                        Likes=item["statistics"].get("likeCount"),
                        Comments=item["statistics"].get("commentCount"),
                        Defenition=item["contentDetails"]["definition"])
                
                Video_Datas.append(data)
    return Video_Datas
        

    
#Getting comment info
def get_Comment_info(video_ids):

    Comment_Data=[]

    try:
        for Video_IDS in (video_ids):
            response=youtube.commentThreads().list(
                    part="snippet",
                    videoId=Video_IDS,
                    maxResults=50
                ).execute()

            for item in response["items"]:
                data=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"],
                        video_id=item["snippet"]['videoId'],
                        Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                        )
                Comment_Data.append(data)

    except:
        pass

    return Comment_Data



#Mongodb connection

client = pymongo.MongoClient("mongodb+srv://shanmugapriyans209:shanlee@shanlee.vpqu7ez.mongodb.net/")

db=client["Youtube_Data"]


#Channel Details collection

def channel_details(channel_id):
    Chnnel_details=get_channel_info(channel_id)
    play_details=get_playlist_info(channel_id)
    vdeo_ids=get_video_IDS(channel_id)
    video_details=get_video_info(vdeo_ids)
    comment_details=get_Comment_info(vdeo_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":Chnnel_details,"playlist_information":play_details,
                      "Video_information":video_details,"Comment_information":comment_details})
    
    return "uploaded"


#channel creation

def Channel_Details():
    
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="shanlee",
        database="Youtube_Data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query='''DROP TABLE IF EXISTS Channels'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = """
                CREATE TABLE IF NOT EXISTS Channels (
                    channel_Name VARCHAR(100),
                    channel_Id VARCHAR(80) PRIMARY KEY,
                    Subscribers BIGINT,
                    views BIGINT,
                    Total_Videos INT,
                    channel_Discription TEXT,
                    Playlist_id VARCHAR(80)
                )
            """
    cursor.execute(create_query)
    mydb.commit()

    ch_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index, row in df.iterrows():
            insert_query = '''
                insert into Channels(channel_Name,
                    channel_Id,
                    Subscribers,
                    views,
                    Total_Videos,
                    channel_Discription,
                    Playlist_id)
                values(%s, %s, %s, %s, %s, %s, %s)
            '''

            values = (
                row["channel_Name"],
                row["channel_Id"],
                row["Subscribers"], 
                row["views"],
                row["Total_Videos"],
                row["channel_Discription"],
                row["Playlist_id"]
            )
            

            cursor.execute(insert_query, values)
            mydb.commit()



#Playlist creation

def Playlist_Details():
    

    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="shanlee",
        database="Youtube_Data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query='''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """
        CREATE TABLE IF NOT EXISTS playlists (
            Playlist_Id VARCHAR(100) PRIMARY KEY,
            Title VARCHAR(200) ,
            channel_Id VARCHAR(100),
            Channel_Name VARCHAR(100),
            Publishedat timestamp,
            Video_Count int
        )
    """

    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = """
            INSERT INTO playlists (Playlist_Id, Title, channel_Id, Channel_Name, Publishedat, Video_Count)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        values = (
            row["Playlist_Id"],
            row["Title"],
            row["channel_Id"],
            row["Channel_Name"],
            row["Publishedat"],
            row["Video_Count"]
        )

        cursor.execute(insert_query, values)  # Ensure this line is within the loop
        mydb.commit()




#videos table creation

def Videos_Details():
    

    mydb = psycopg2.connect(
                host="localhost",
                user="postgres",
                password="shanlee",
                database="Youtube_Data",
                port="5432"
            )
    cursor = mydb.cursor()

    drop_query='''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """
        CREATE TABLE IF NOT EXISTS videos (
                            Channel_Name Varchar(100),
                            ChannelId Varchar(100),
                            video_Id Varchar(30) PRIMARY KEY,
                            Title Varchar(100),
                            Tags text,
                            Thumbnail Varchar(100),
                            Description text,
                            published_Date timestamp,
                            Duration interval,
                            Views bigint,
                            Likes bigint,
                            Comments int,
                            Defenition varchar(50)
        )
    """


            
    cursor.execute(create_query)
    mydb.commit()

    vid_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vid_data in coll1.find({},{"_id":0,"Video_information":1}):
            for i in range (len(vid_data["Video_information"])):
                vid_list.append(vid_data["Video_information"][i])
                df2=pd.DataFrame(vid_list)



    for index, row in df2.iterrows():
            insert_query = '''
    INSERT INTO videos (Channel_Name, ChannelId, video_Id, Title, Tags, Thumbnail,
                            Description, published_Date, Duration, Views,Likes, Comments, Defenition)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

            values = (
                row["channel_Name"],
                row["channelId"],
                row["video_Id"], 
                row["Title"],
                row["Tags"],
                row["Thumbnail"],
                row["Description"],
                row["published_Date"], 
                row["Duration"],
                row["views"],
                row["Likes"],
                row["Comments"],
                row["Defenition"], 
            )
        


            cursor.execute(insert_query, values)
            mydb.commit()            



#comment table creation

def Comment_Details():    
    
    mydb = psycopg2.connect(
                host="localhost",
                user="postgres",
                password="shanlee",
                database="Youtube_Data",
                port="5432"
            )
    cursor = mydb.cursor()

    drop_query='''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """
        CREATE TABLE IF NOT EXISTS comments (
                            Comment_Id varchar(100) primary key,
                            video_id varchar(50),
                            Comment_Text text,
                            Comment_Author varchar(100),
                            Comment_Published timestamp
                            )"""


            
    cursor.execute(create_query)
    mydb.commit()

    Cmt_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for Cmt_data in coll1.find({},{"_id":0,"Comment_information":1}):
            for i in range(len(Cmt_data["Comment_information"])):
                Cmt_list.append(Cmt_data["Comment_information"][i])

    df3=pd.DataFrame(Cmt_list)


    for index, row in df3.iterrows():
            insert_query = '''
                insert into comments(Comment_Id,
                                        video_id,
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)
                values(%s, %s, %s, %s, %s)
            '''

            values = (
                row["Comment_Id"],
                row["video_id"],
                row["Comment_Text"], 
                row["Comment_Author"],
                row["Comment_Published"]
            )
        


            cursor.execute(insert_query, values)
            mydb.commit()




#Table creation 
            
def tables():
    Channel_Details()
    Playlist_Details()
    Videos_Details()
    Comment_Details()

    return "Table already created"



def show_channel_table():

    ch_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlist_table():
    pl_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)
    return df1


def show_Video_table():

    vid_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vid_data in coll1.find({},{"_id":0,"Video_information":1}):
        for i in range (len(vid_data["Video_information"])):
            vid_list.append(vid_data["Video_information"][i])
    df2=st.dataframe(vid_list)
    return df2


def show_Comment_table():
    Cmt_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for Cmt_data in coll1.find({},{"_id":0,"Comment_information":1}):
            for i in range(len(Cmt_data["Comment_information"])):
                Cmt_list.append(Cmt_data["Comment_information"][i])

    df3=st.dataframe(Cmt_list)

    return df3


# streamlit query 

with st.sidebar:
    st.title(":red[YOUTUBE]")
    st.header(":blue[Data Harvesting and Ware Housing]")
    st.subheader(":red[Skills Take Away]")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("Mongo DB")
    st.caption("API integration")
    st.caption("SQL")
    st.caption("Data Presentation using Stramlit")

channel_id=st.text_input("Enter the channel ID")

if st.button("Collecting Data"):
    ch_ids=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"])
    
    
    if channel_id in ch_ids:st.sucess("Channel Detail is already Exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Tables=tables()
    st.success(Tables)

show_tables=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_tables=="CHANNELS":
    show_channel_table()


elif show_tables=="PLAYLISTS":
    show_playlist_table()

elif show_tables=="VIDEOS":
    show_Video_table()

elif show_tables=="COMMENTS":
    show_Comment_table()
    


#SQL connection

mydb = psycopg2.connect(
                host="localhost",
                user="postgres",
                password="shanlee",
                database="Youtube_Data",
                port="5432"
            )
cursor = mydb.cursor()

Questions=st.selectbox("Select Your Questions",("1.All the Videos and The Channel Name",
                                             "2.Channels with most number of Videos",
                                             "3.10 Most viewed Videos",
                                             "4.Comments in each Videos",
                                             "5.Videos with Highest likes",
                                             "6.Likes of all Videos",
                                             "7.Views of each Channels",
                                             "8.Videos published in the Year of 2022",
                                             "9.Average Duration of all videos in each channels",
                                             "10.Videos with highest number of comments"))

if Questions=="1.All the Videos and The Channel Name":
    query1='''select title as videos,channel_Name as Channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    DF=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(DF)


elif Questions=="2.Channels with most number of Videos":
    query2='''select channel_Name as Channelname,total_videos as no_videos from channels order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    DF1=pd.DataFrame(t2,columns=["channel name","No of Videos"])
    st.write(DF1)

elif Questions=="3.10 Most viewed Videos":
    query3='''select channel_Name as Channelname,title as videotitle,views as views from videos
    where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    DF2=pd.DataFrame(t3,columns=["channelname","videotitle","views"])
    st.write(DF2)
    
elif Questions=="4.Comments in each Videos":
    query4='''select title as videotitle,comments as no_comments from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    DF3=pd.DataFrame(t4,columns=["video title","No of comments"])
    st.write(DF3)

elif Questions=="5.Videos with Highest likes":
    query5='''select title as videotitle,channel_Name as Channelname, likes as likecount from videos
    where likes is not null order by likes desc '''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    DF4=pd.DataFrame(t5,columns=["video title","channelname","likecount"])
    st.write(DF4)

elif Questions=="6.Likes of all Videos":
    query6='''select title as videotitle, likes as likecount from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    DF5=pd.DataFrame(t6,columns=["video title","likecount"])
    st.write(DF5)

elif Questions=="7.Views of each Channels":
    query7='''select channel_Name as Channelname, views as totalviews from channels '''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    DF6=pd.DataFrame(t7,columns=["video title","likecount"])
    st.write(DF6)

elif Questions=="8.Videos published in the Year of 2022":
    query8='''select title as videotitle,published_date as videoreleaseddate,channel_Name as Channelname from videos '''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    DF7=pd.DataFrame(t8,columns=["video title","published date","channelname"])
    st.write(DF7)

elif Questions=="9.Average Duration of all videos in each channels":
    query9='''select channel_Name as Channelname,AVG(duration) as AverageDuration  from videos 
    group by channel_Name '''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    DF8=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in DF8.iterrows():
        channel_title=row["channelname"]
        average_duration=["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    st.write(DF8)





elif Questions=="10.Videos with highest number of comments":
    query10='''select title as videotitle,channel_Name as Channelname, comments as commments from videos
    where comments is not null order by comments desc '''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    DF9=pd.DataFrame(t10,columns=["video title","channelname","comments"])
    st.write(DF9)




