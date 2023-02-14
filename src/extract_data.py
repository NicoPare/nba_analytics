import os
import json
import requests
import sys
import glob
import pandas as pd
from nba_api.stats.endpoints import shotchartdetail
import numpy as np
import datetime
from random_selection import user_input
"""
def match_key():
    match_key = get_match_key()
    return match_key
"""

def get_data_path():
    directory_of_python_script = os.path.dirname(os.path.abspath(__file__))
    directory_of_python_script = directory_of_python_script + "\data"
    return directory_of_python_script

match_key = '00' + user_input
match_path = get_data_path() + "\\" + match_key
"""
def get_match_path():
    path = get_data_path() + "\\" + match_key
    return path
"""
## boxscore and playbyplay extraction

def api_response_json():
    with open('../nba_endpoint.json') as urls:
        file_contents = json.load(urls)

    boxscore_url = file_contents["boxscore"]
    play_by_play_url = file_contents["play_by_play"]

    #match = match_key()
    # temporary solution to test 1 particular match
    boxscore_url = boxscore_url.replace('{key}', match_key)
    play_by_play_url = play_by_play_url.replace('{key}', match_key)
    outResult, outResultJson_box = get_web_data(boxscore_url)
    outResult, outResultJson_pbp = get_web_data(play_by_play_url)

    # create match directory
    directory = match_path
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError as error:
        print("Directory '%s' can not be created" % directory)

    # save results in json file
    with open(match_path + '\\boxscore_api.json', 'w', encoding='utf-8') as f:
        json.dump(outResultJson_box, f, ensure_ascii=False, indent=4)

    # save results in json file
    with open(match_path + '\\play_by_play_api.json', 'w', encoding='utf-8') as f:
        json.dump(outResultJson_pbp, f, ensure_ascii=False, indent=4)

# function to get data from api

def get_web_data(url):
    response = requests.get(url)
    responsejson = json.loads(response.text)
    return response.text, responsejson


# paths
def get_json_path(number):  # 0 boxscore, 1 play by play
    api_files = glob.glob(match_path + '\*.json')
    directory = api_files[number]
    return directory

def get_play_by_play_frame ():
    path_play_by_play = get_json_path(1)  # play by play
    with open(path_play_by_play) as data_file:
        data = json.load(data_file)

    # cleaning json file
    f = data['game']

    my_list = list(f.values())
    dataset = my_list[1]

    df = pd.DataFrame(dataset)
    return df


## shotchart extraction

y = int(datetime.datetime.today().year)
s = int(y - 3)

# get game_id from previous loaded files
def get_game_id():
    f = open(f'data/{match_key}/boxscore_api.json', "r")
    data = json.loads(f.read())
    game_id = data['game']['gameId']

    return game_id

# get csv files from repository
def get_csv_files(location):
    csv_files = glob.glob(os.path.join(location, "*.csv"))
    for f in csv_files:
        df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    return df

# get team_id and player_id from files loaded in "get_csv_files" function
def get_teams_id(location):
    df = get_csv_files(location)
    team_id = df["team_id"].unique()
    team_id.tolist()
    player_id = df["player_id"].unique()
    player_id.tolist()
    return team_id, player_id


# creates a list of NBA seasons from start-end
def seasons_string(start_year,end_year):
    years = np.arange(start_year,end_year)
    seasons = []
    for year in years:
        string1 = str(year)
        string2 = str(year+1)
        season = '{}-{}'.format(string1,string2[-2:])
        seasons.append(season)
    return seasons

# get shotchart data from team_id
def get_shotchart_data(team_id):
    data = []
    for season in seasons_string(s, y):
        shotdata = shotchartdetail.ShotChartDetail(team_id=team_id,
                                                   # set to 0 as we are not querying any specific team/player
                                                   player_id=0,
                                                   season_nullable=season,
                                                   context_measure_simple='FGA')
        single_season = shotdata.get_data_frames()[0]
        single_season['SEASON'] = season
        data.append(single_season)

    data = pd.concat(data, ignore_index=True)
    output = get_game_id()
    data = data.loc[data["GAME_ID"].isin([output])]
    return data


# get historical data from each player
def get_h_player_shotchart_data(player):
    for season in seasons_string(s, y):
        shotdata = shotchartdetail.ShotChartDetail(team_id=0,
                                                   # set to 0 as we are not querying any specific team/player
                                                   player_id=player,
                                                   season_nullable=season,
                                                   context_measure_simple='FGA')
        single_season = shotdata.get_data_frames()[0]
        single_season['SEASON'] = season
    return single_season

def player_ids(path):
    df = get_csv_files(path)
    player_id = df["player_id"]
    player_id = player_id.tolist()
    return player_id