import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    '''
    This function is aimed at processing json files from `./data/song_data` to further 
    feed both songs and artists dimensional tables.
    If run successfuly this function inserts the data into both of the above mentioned tables.
    '''
    
    # open song file
    df = pd.read_json(filepath, lines=True) 

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)
    
    
def process_log_file(cur, filepath):
    
    '''
    This function is aimed at processing json files from `./data/log_data` to further 
    feed both time and users dimensional tables together with the songplay functional table.
    If run successfuly this function inserts the data into three above mentioned tables.
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    
    # convert timestamp column to datetime
    df['ts']=pd.to_datetime(df['ts'])
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.week
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.weekday
    
    # insert time data records
    time_data = list(zip(df['ts'],df['hour'],df['day'],df['week'],df['month'],df['year'],df['weekday']))
    column_lables = ['start_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(time_data, columns = column_lables)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    '''
    This function is aimed at processing multiple json files from the sources locations
    stated as `filepath` variable together with `func` variable that describes 
    the essential data manupulation to be done over the called json files.
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
        connect to sparkifydb database and then process song data and log data
    '''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    
    # create ER diagram of sparkifydb. Uncomment to regenerate the ER Diagram.
    #graph = create_schema_graph(metadata=MetaData('postgresql://student:student@127.0.0.1/sparkifydb'))
    #graph.write_png('img/sparkifydb_erd.png')

if __name__ == "__main__":
    main()