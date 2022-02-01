import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
from secret import TOKEN, USER_ID

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

def validate_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print('No songs downlaoded, finishing execution')
        return False
    # primary key check
    if pd.Series(df['played_at']).is_unique:
        return True
    else:
        raise Exception("The primary key contains duplicate values")

    # check to see if there are any null values present in dataset
    if df.isnull().values.any():
        raise Exception("Your data contains null values")

    yesterday = datetime.datetime.now()
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df["timestamp"].tolist()

    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")

if __name__ == '__main__':

    headers = {
        'Accept': "application/json",
        'Content-Type': "application"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    if validate_data(song_df):
        print("Data valid, proceed to loading stage")

    # relational databases: mysql, postgresql, sqlite, store in
    # tables and rows

    '''
    non relational databases (mongo and dynamo db)
    store data in JSON documents, more flexible, easy to change schema
    '''


    '''
    on premise data storage
    cloud (stored on google, aws huge data centers)
    '''

   #Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
       CREATE TABLE IF NOT EXISTS my_played_tracks(
           song_name VARCHAR(200),
           artist_name VARCHAR(200),
           played_at VARCHAR(200),
           timestamp VARCHAR(200),
           CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
       )
       """
    cursor.execute(sql_query)
    print('Successfully opened database')

    try:
        song_df.to_sql("my_played_tracks", engine, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print('Closed database successfully')