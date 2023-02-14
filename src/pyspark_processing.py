from pyspark.sql.functions import *
from pyspark.sql import SparkSession, SQLContext
import sys
import os
import pandas as pd
from src.extract_data import get_json_path, get_data_path, api_response_json, get_teams_id, get_shotchart_data, match_path, get_h_player_shotchart_data, get_csv_files, player_ids

from lib.logger import Log4j

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":

    # spark session
    # spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    spark = SparkSession \
        .builder \
        .appName("nba_api_project") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    logger = Log4j(spark)

    # run api response function
    api_response_json()

    path_boxscore = get_json_path(0)  # boxscore
    path_play_by_play = get_json_path(1)  # play by play

    # read json file
    df = spark.read.json(path_boxscore, multiLine=True)

    # create dataframe of head boxscore
    dfmatchf = df.select(df.game["gameId"].alias("game_id"), \
                         df.game["gameCode"].alias("game_code"), \
                         df.game.arena["arenaId"].alias("arena_id"), \
                         df.game.arena["arenaName"].alias("arena_name"), \
                         df.game.homeTeam["teamId"].alias("home_team_id"), \
                         df.game.homeTeam["teamName"].alias("home_team_name"), \
                         df.game.homeTeam["teamCity"].alias("home_team_city"), \
                         df.game.homeTeam["teamTricode"].alias("home_team_tricode"), \
                         df.game.homeTeam["score"].alias("home_team_score"), \
                         df.game.awayTeam["teamId"].alias("away_team_id"), \
                         df.game.awayTeam["teamName"].alias("away_team_name"), \
                         df.game.awayTeam["teamCity"].alias("away_team_city"), \
                         df.game.awayTeam["teamTricode"].alias("away_team_tricode"), \
                         df.game.awayTeam["score"].alias("away_team_score")
                         )

    # create pyspark dataframe with data from home players
    dfplayer = df.select(df.game.homeTeam["teamId"].alias("team_id"), \
                         df.game.homeTeam.players["personId"].alias("player_id"), \
                         df.game.homeTeam.players["name"].alias("player_name"), \
                         df.game.homeTeam.players["position"].alias("position"), \
                         df.game.homeTeam.players["jerseyNum"].alias("jersey"), \
                         df.game.homeTeam.players.statistics["fieldGoalsAttempted"].alias("field_goals_attempted"), \
                         df.game.homeTeam.players.statistics["fieldGoalsMade"].alias("field_goals_made"), \
                         df.game.homeTeam.players.statistics["fieldGoalsPercentage"].alias("field_goals_percentage"), \
                         df.game.homeTeam.players.statistics["threePointersAttempted"].alias("three_pointers_attemp"), \
                         df.game.homeTeam.players.statistics["threePointersMade"].alias("three_pointers_made"), \
                         df.game.homeTeam.players.statistics["threePointersPercentage"].alias(
                             "three_pointers_percentage"), \
                         df.game.homeTeam.players.statistics["twoPointersAttempted"].alias("two_pointers_attempted"), \
                         df.game.homeTeam.players.statistics["twoPointersMade"].alias("two_pointers_made"), \
                         df.game.homeTeam.players.statistics["twoPointersPercentage"].alias("two_pointers_percentage"), \
                         df.game.homeTeam.players.statistics["foulsPersonal"].alias("fouls_personal"), \
                         df.game.homeTeam.players.statistics["freeThrowsAttempted"].alias("free_throws_attemp"), \
                         df.game.homeTeam.players.statistics["freeThrowsMade"].alias("free_throws_made"), \
                         df.game.homeTeam.players.statistics["freeThrowsPercentage"].alias("free_throws_percentage"), \
                         df.game.homeTeam.players.statistics["points"].alias("points"), \
                         df.game.homeTeam.players.statistics["minutesCalculated"].alias("minutes"), \
                         df.game.homeTeam.players.statistics["reboundsDefensive"].alias("rebounds_defensive"), \
                         df.game.homeTeam.players.statistics["reboundsOffensive"].alias("rebounds_offensive"), \
                         df.game.homeTeam.players.statistics["reboundsTotal"].alias("rebounds_total"), \
                         df.game.homeTeam.players.statistics["assists"].alias("assists"), \
                         df.game.homeTeam.players.statistics["steals"].alias("steals"), \
                         df.game.homeTeam.players.statistics["turnovers"].alias("turnovers") \
                         )

    # create pyspark dataframe with data from away players
    dfplayer_a = df.select(df.game.awayTeam["teamId"].alias("team_id"), \
                           df.game.awayTeam.players["personId"].alias("player_id"), \
                           df.game.awayTeam.players["name"].alias("player_name"), \
                           df.game.awayTeam.players["position"].alias("position"), \
                           df.game.awayTeam.players["jerseyNum"].alias("jersey"), \
                           df.game.awayTeam.players.statistics["fieldGoalsAttempted"].alias("field_goals_attempted"), \
                           df.game.awayTeam.players.statistics["fieldGoalsMade"].alias("field_goals_made"), \
                           df.game.awayTeam.players.statistics["fieldGoalsPercentage"].alias("field_goals_percentage"), \
                           df.game.awayTeam.players.statistics["threePointersAttempted"].alias("three_pointers_attemp"), \
                           df.game.awayTeam.players.statistics["threePointersMade"].alias("three_pointers_made"), \
                           df.game.awayTeam.players.statistics["threePointersPercentage"].alias(
                               "three_pointers_percentage"), \
                           df.game.awayTeam.players.statistics["twoPointersAttempted"].alias("two_pointers_attempted"), \
                           df.game.awayTeam.players.statistics["twoPointersMade"].alias("two_pointers_made"), \
                           df.game.awayTeam.players.statistics["twoPointersPercentage"].alias(
                               "two_pointers_percentage"), \
                           df.game.awayTeam.players.statistics["foulsPersonal"].alias("fouls_personal"), \
                           df.game.awayTeam.players.statistics["freeThrowsAttempted"].alias("free_throws_attemp"), \
                           df.game.awayTeam.players.statistics["freeThrowsMade"].alias("free_throws_made"), \
                           df.game.awayTeam.players.statistics["freeThrowsPercentage"].alias("free_throws_percentage"), \
                           df.game.awayTeam.players.statistics["points"].alias("points"), \
                           df.game.awayTeam.players.statistics["minutesCalculated"].alias("minutes"), \
                           df.game.awayTeam.players.statistics["reboundsDefensive"].alias("rebounds_defensive"), \
                           df.game.awayTeam.players.statistics["reboundsOffensive"].alias("rebounds_offensive"), \
                           df.game.awayTeam.players.statistics["reboundsTotal"].alias("rebounds_total"), \
                           df.game.awayTeam.players.statistics["assists"].alias("assists"), \
                           df.game.awayTeam.players.statistics["steals"].alias("steals"), \
                           df.game.awayTeam.players.statistics["turnovers"].alias("turnovers") \
                           )

    # transform before load csv: home team
    dfpcsv = dfplayer.selectExpr("team_id", "inline(arrays_zip(player_id, \
                                                            player_name, \
                                                            position, \
                                                            jersey, \
                                                            field_goals_attempted, \
                                                            field_goals_made, \
                                                            field_goals_percentage, \
                                                            three_pointers_attemp, \
                                                            three_pointers_made, \
                                                            three_pointers_percentage, \
                                                            two_pointers_attempted, \
                                                            two_pointers_made, \
                                                            two_pointers_percentage, \
                                                            free_throws_attemp, \
                                                            free_throws_made, \
                                                            free_throws_percentage, \
                                                            points, \
                                                            minutes, \
                                                            rebounds_defensive, \
                                                            rebounds_offensive, \
                                                            rebounds_total, \
                                                            fouls_personal, \
                                                            assists, \
                                                            steals, \
                                                            turnovers \
                                                            ))"
                                 )

    # transform before load csv: away team
    dfpcsv_a = dfplayer_a.selectExpr("team_id", "inline(arrays_zip(player_id, \
                                                            player_name, \
                                                            position, \
                                                            jersey, \
                                                            field_goals_attempted, \
                                                            field_goals_made, \
                                                            field_goals_percentage, \
                                                            three_pointers_attemp, \
                                                            three_pointers_made, \
                                                            three_pointers_percentage, \
                                                            two_pointers_attempted, \
                                                            two_pointers_made, \
                                                            two_pointers_percentage, \
                                                            free_throws_attemp, \
                                                            free_throws_made, \
                                                            free_throws_percentage, \
                                                            points, \
                                                            minutes, \
                                                            rebounds_defensive, \
                                                            rebounds_offensive, \
                                                            rebounds_total, \
                                                            fouls_personal, \
                                                            assists, \
                                                            steals, \
                                                            turnovers \
                                                            ))"
                                     )

    # save data in local directory
    dfpcsv.write.options(header='True', delimiter=',') \
        .mode("overwrite") \
        .csv(match_path + "\\h_players_file")

    dfpcsv_a.write.options(header='True', delimiter=',') \
        .mode("overwrite") \
        .csv(match_path + "\\a_players_file")

    dfmatchf.write.options(header='True', delimiter=',') \
        .mode("overwrite") \
        .csv(match_path + "\\match_file")


    # shotchart processing
    team_id_h = get_teams_id(match_path + '\\h_players_file')[0]
    team_id_a = get_teams_id(match_path + '\\a_players_file')[0]
    df_h = get_shotchart_data(team_id_h)
    df_a = get_shotchart_data(team_id_a)

    dfh = spark.createDataFrame(df_h)
    dfa = spark.createDataFrame(df_a)

    # historical shotchart
    def load_historical(path):
        hist_df = []
        players = player_ids(path)

        for idx, i in enumerate(players):
            print(f"Getting data from {i} player_id")
            hist_df.append(get_h_player_shotchart_data(i))
            data = pd.concat(hist_df)

        if path.find("h_") != -1:
            # create directory to storage
            directory_hist = match_path + "\\historical\\home_team"
            try:
                os.makedirs(directory_hist, exist_ok=True)
            except OSError as error:
                print("Directory '%s' can not be created" % directory_hist)

            data.to_csv(directory_hist + "\\home_shotchart_players.csv", index=False, header=True)
        else:
            # create directory to storage
            directory_hist = match_path + "\\historical\\away_team"
            try:
                os.makedirs(directory_hist, exist_ok=True)
            except OSError as error:
                print("Directory '%s' can not be created" % directory_hist)

            data.to_csv(directory_hist + "\\away_shotchart_players.csv", index=False, header=True)

    # create directory to storage
    directory = match_path + "\\shotchart"

    try:
        os.makedirs(directory, exist_ok=True)
    except OSError as error:
        print("Directory '%s' can not be created" % directory)

    # save data in local directory
    dfh.write.format("csv")\
        .mode("overwrite") \
        .option("header", "true")\
        .csv(directory + "\\shot_chart_h")

    dfa.write.format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(directory + "\\shot_chart_a")

    load_historical(match_path + "\\h_players_file")
    load_historical(match_path + "\\a_players_file")

    logger.info("NBA data load finished")