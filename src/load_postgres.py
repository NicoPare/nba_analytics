import psycopg2
import pandas as pd
import numpy as np
from extract_data import get_play_by_play_frame, get_game_id, get_data_path, get_csv_files, match_path# get_match_path
from sql.sql_query import *

def df_to_list(df):
    output = df.values.tolist()
    return output

conn = psycopg2.connect(database="nba_db",
                        user='admin', password='admin',
                        host='127.0.0.1', port='5432'
                        )

conn.autocommit = True
cursor = conn.cursor()

def load_pbp_postgres():
    df = get_play_by_play_frame()
    df = df[['actionNumber', 'clock', 'timeActual', 'period', 'actionType', 'description', 'scoreHome', 'scoreAway']]
    df = df.assign(game_id=get_game_id())

    cursor.execute(play_by_play_drop)
    cursor.execute(play_by_play_create)
    for i, row in df.iterrows():
        cursor.execute(play_by_play_insert, row)
        conn.commit()
    return print("Play by play data loaded successfully")

def get_storage_data(location):
    dir = match_path
    file = location
    df_n = get_csv_files(dir + file)
    return df_n, file

def load_data(df, file, drop_query, create_query, insert_query):
    stats = df
    dir = file
    if dir.find("player") != -1:
        stats = stats.assign(game_id=get_game_id())
    else:
        stats['GAME_ID'] = stats['GAME_ID'].astype(str).str.zfill(10)

    cursor.execute(drop_query)
    cursor.execute(create_query)
    for i, row in stats.iterrows():
        cursor.execute(insert_query, row)
        conn.commit()
    return print(f"Data from {file} loaded successfully")

def load_hist_shotchart(path, drop_query, create_query, insert_query):
    data = get_csv_files(path)
    cursor.execute(drop_query)
    cursor.execute(create_query)
    for i, row in data.iterrows():
        cursor.execute(insert_query, row)
        conn.commit()
    return print("Shotchart historical data loaded successfully")

load_data(get_storage_data('/h_players_file')[0], get_storage_data('/h_players_file')[1], home_player_stats_drop, home_player_stats_create, home_player_stats_insert)
load_data(get_storage_data("/a_players_file")[0], get_storage_data("/a_players_file")[1], away_player_stats_drop, away_player_stats_create, away_player_stats_insert)

load_data(get_storage_data("/shotchart/shot_chart_h")[0], get_storage_data("/shotchart/shot_chart_h")[1], home_shotchart_drop, home_shotchart_create, home_shotchart_insert)
load_data(get_storage_data("/shotchart/shot_chart_a")[0], get_storage_data("/shotchart/shot_chart_a")[1], away_shotchart_drop, away_shotchart_create, away_shotchart_insert)
load_pbp_postgres()
load_hist_shotchart((match_path + "\\historical\\home_team"), home_hist_shotchart_drop, home_hist_shotchart_create, home_hist_shotchart_insert)
load_hist_shotchart((match_path + "\\historical\\away_team"), away_hist_shotchart_drop, away_hist_shotchart_create, away_hist_shotchart_insert)