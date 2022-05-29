from operator import index
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime
import json


def process_song_file(cur, filepath):
    """The process_song_file function is used to read the data contained in the JSON files saved in the song_data folder
    After reading the json the data are stored into two tables. i.e songs and artists
    """
    # open song file
    cumm_song_list = []
    for song_datum in filepath:
        with open(song_datum) as song_file:
            content = json.load(song_file)
            cumm_song_list.append(content)
    df = pd.DataFrame(cumm_song_list)        
# insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    uploaded_filepath = generate_filename("song_table") 
    song_data.to_csv(uploaded_filepath, index=False, header=True,sep=',', encoding='utf-8') #make a .csv file out of the song_data dataframe
    new_songs = open(uploaded_filepath)
    print('file opened in memory')
    
    #Read the newly created csv file into the table songs in the database
    copy_sql = '''COPY public.songs FROM STDIN WITH \
CSV \
HEADER \
DELIMITER AS ',' '''
    try:
        cur.copy_expert(sql=copy_sql,file=new_songs)
        print("file copied to table")
    except psycopg2.Error as e:
        print("could not copy file "+ uploaded_filepath+ " to the table")
        print(e)
        
              
    # insert artist record
    artist_data = artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']] #select colums required to define the artist
    """Remove values with same artist ID, keeps last record believing that is the most recent""" 
    artist_data_cleaned = artist_data.drop_duplicates(subset=["artist_id"], keep='last')
    uploaded_filepath = generate_filename("artist_table") 
    artist_data_cleaned.to_csv(uploaded_filepath, index=False, header=True,sep=',', encoding='utf-8')
    new_artist= open(uploaded_filepath)
    print('file opened in memory')
    
    copy_sql = '''COPY public.artists FROM STDIN WITH \
CSV \
HEADER \
DELIMITER AS ',' '''
    try:
        cur.copy_expert(sql=copy_sql,file=new_artist)
        print("file copied to table")
    except psycopg2.Error as e:
        print("could not copy file "+ uploaded_filepath+ " to the table\n")
        print(e)
   
   
   
def process_log_file(cur, filepath):
    """The process_song_file function is used to read the data contained in the JSON files saved in the log_data folder
    After reading the json the data are stored into three tables. i.e time_info and users and song_play
    
    The data inserted into the table are those whose value for the column page is "NextSong"
    the datetime hour, day week, month, year, and day_name are generated using the values in the timestamp column
    """
    # open log file
    log_df = pd.DataFrame()
    for i in range(len(filepath)):
        df = pd.read_json(filepath[i], lines=True)
        log_df = log_df.append(df)
       
    #df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    log_df = log_df[log_df['page']=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(log_df['ts'],  unit='ms')
    
    # insert time data records
    time_data = []
    for datetim in t:
        time_data.append([datetim, datetim.hour, datetim.day, datetim.week, datetim.month, datetim.year, datetim.day_name()])
    column_labels = ()
    time_df = pd.DataFrame(time_data, columns=["datetime","hour","day","week","month","year","weekday"])
    time_data_cleaned = time_df.drop_duplicates(subset=["datetime"], keep='first')
    
    
    try:
        for i, row in time_data_cleaned.iterrows():
            cur.execute(time_table_insert, list(row))
        #conn.commit()
    except psycopg2.Error as e:
        print("table insertion error: ")
        print(e)
    

    # load user table
    user_df = log_df[['userId','firstName','lastName','gender','level']]
    user_df['userId'] = user_df['userId'].astype(int) # change userId to integer from object
    user_data_cleaned = user_df.drop_duplicates(subset=['userId'],keep='last')# remove duplicate userId
    user_data_cleaned['userId'] = user_data_cleaned['userId'].astype(str)# change userId to string
    
    # insert user records
    try:
        for i, row in user_data_cleaned.iterrows():
            cur.execute(user_table_insert, row)
            #conn.commit()
    except psycopg2.Error as e:
        print("table insertion error: ")
        print(e)

    # insert songplay records
    for index, row in log_df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
            print(results)
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    func(cur,all_files)
    conn.commit
    print('{} files processed.'.format(num_files))

    # iterate over files and process
    # for i, datafile in enumerate(all_files, 1):
    #     func(cur, datafile)
    #     conn.commit()
    #     print('{}/{} files processed.'.format(i, num_files))

def generate_filename(data):
    upload_filename =datetime.datetime.now().strftime("%m%d%Y%H%M%S%f")+data
    upload_filename = upload_filename + ".csv"
    uploaded_filepath = "uploaded_data\\" + upload_filename
    return uploaded_filepath

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    print("start log_file processing")
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    print("closing connection")
    conn.close()


if __name__ == "__main__":
    main()