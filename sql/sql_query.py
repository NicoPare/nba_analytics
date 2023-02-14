from src.extract_data import get_game_id

# DROP TABLES
play_by_play_drop = f"DROP TABLE IF EXISTS play_by_play_{get_game_id()}"
home_player_stats_drop = f"DROP TABLE IF EXISTS home_player_stats_{get_game_id()}"
away_player_stats_drop = f"DROP TABLE IF EXISTS away_player_stats_{get_game_id()}"
home_shotchart_drop = f"DROP TABLE IF EXISTS home_shotchart_stats_{get_game_id()}"
away_shotchart_drop = f"DROP TABLE IF EXISTS away_shotchart_stats_{get_game_id()}"
home_hist_shotchart_drop = f"DROP TABLE IF EXISTS home_hist_shotchart_stats_{get_game_id()}"
away_hist_shotchart_drop = f"DROP TABLE IF EXISTS away_hist_shotchart_stats_{get_game_id()}"

# CREATE TABLES
play_by_play_create = (f""" CREATE TABLE IF NOT EXISTS play_by_play_{get_game_id()} \
    (
        action_number   int,
        clock           varchar,
        time_actual     varchar,
        period          int,
        action_type     varchar,
        description     varchar,
        score_home      int,
        score_away      int,
        game_id         varchar
    );
""")

home_player_stats_create = ( f""" CREATE TABLE IF NOT EXISTS home_player_stats_{get_game_id()} \
    (        
        team_id                     varchar,
        player_id                   varchar,
        player_name                 varchar,
        position                    varchar,
        jersey                      int,
        field_goals_attempted       int,
        field_goals_made            int,
        field_goals_percentage      decimal,
        three_pointers_attemp       int,
        three_pointers_made         int,
        three_pointers_percentage   decimal,
        two_pointers_attempted      int,
        two_pointers_made           int,
        two_pointers_percentage     decimal,
        free_throws_attemp          int,
        free_throws_made            int,
        free_throws_percentage      decimal,
        points                      int,
        minutes                     varchar,
        rebounds_defensive          int,
        rebounds_offensive          int,
        rebounds_total              int,
        fouls_personal              int,
        assists                     int,
        steals                      int,
        turnovers                   int,
        game_id                     varchar
    );
""")

away_player_stats_create = ( f""" CREATE TABLE IF NOT EXISTS away_player_stats_{get_game_id()} \
    (        
        team_id                     varchar,
        player_id                   varchar,
        player_name                 varchar,
        position                    varchar,
        jersey                      int,
        field_goals_attempted       int,
        field_goals_made            int,
        field_goals_percentage      decimal,
        three_pointers_attemp       int,
        three_pointers_made         int,
        three_pointers_percentage   decimal,
        two_pointers_attempted      int,
        two_pointers_made           int,
        two_pointers_percentage     decimal,
        free_throws_attemp          int,
        free_throws_made            int,
        free_throws_percentage      decimal,
        points                      int,
        minutes                     varchar,
        rebounds_defensive          int,
        rebounds_offensive          int,
        rebounds_total              int,
        fouls_personal              int,
        assists                     int,
        steals                      int,
        turnovers                   int,
        game_id                     varchar
    );
""")

home_shotchart_create = ( f""" CREATE TABLE IF NOT EXISTS home_shotchart_stats_{get_game_id()} \
    (        
        grid_type           varchar,
        game_id             varchar,
        game_event_id       varchar,
        player_id           varchar,
        player_name         varchar,
        team_id             varchar,
        team_name           varchar,
        period              int,
        minutes_remaining   decimal,
        seconds_remaining   decimal,
        event_type          varchar,
        action_type         varchar,
        shot_type           varchar,
        shot_zone_basic     varchar,
        shot_zone_area      varchar,
        shot_zone_range     varchar,
        shot_distance       decimal,
        loc_y               int,
        loc_x               int,
        shot_attempted_flag int,
        shot_made_flag      int,
        game_date           varchar,
        htm                 varchar,
        vtm                 varchar,
        season              varchar
    );
""")

away_shotchart_create = ( f""" CREATE TABLE IF NOT EXISTS away_shotchart_stats_{get_game_id()} \
    (        
        grid_type           varchar,
        game_id             varchar,
        game_event_id       varchar,
        player_id           varchar,
        player_name         varchar,
        team_id             varchar,
        team_name           varchar,
        period              int,
        minutes_remaining   decimal,
        seconds_remaining   decimal,
        event_type          varchar,
        action_type         varchar,
        shot_type           varchar,
        shot_zone_basic     varchar,
        shot_zone_area      varchar,
        shot_zone_range     varchar,
        shot_distance       decimal,
        loc_y               int,
        loc_x               int,
        shot_attempted_flag int,
        shot_made_flag      int,
        game_date           varchar,
        htm                 varchar,
        vtm                 varchar,
        season              varchar
    );
""")

home_hist_shotchart_create = ( f""" CREATE TABLE IF NOT EXISTS home_hist_shotchart_stats_{get_game_id()} \
    (        
        grid_type           varchar,
        game_id             varchar,
        game_event_id       varchar,
        player_id           varchar,
        player_name         varchar,
        team_id             varchar,
        team_name           varchar,
        period              int,
        minutes_remaining   decimal,
        seconds_remaining   decimal,
        event_type          varchar,
        action_type         varchar,
        shot_type           varchar,
        shot_zone_basic     varchar,
        shot_zone_area      varchar,
        shot_zone_range     varchar,
        shot_distance       decimal,
        loc_y               int,
        loc_x               int,
        shot_attempted_flag int,
        shot_made_flag      int,
        game_date           varchar,
        htm                 varchar,
        vtm                 varchar,
        season              varchar
    );
""")

away_hist_shotchart_create = ( f""" CREATE TABLE IF NOT EXISTS away_hist_shotchart_stats_{get_game_id()} \
    (        
        grid_type           varchar,
        game_id             varchar,
        game_event_id       varchar,
        player_id           varchar,
        player_name         varchar,
        team_id             varchar,
        team_name           varchar,
        period              int,
        minutes_remaining   decimal,
        seconds_remaining   decimal,
        event_type          varchar,
        action_type         varchar,
        shot_type           varchar,
        shot_zone_basic     varchar,
        shot_zone_area      varchar,
        shot_zone_range     varchar,
        shot_distance       decimal,
        loc_y               int,
        loc_x               int,
        shot_attempted_flag int,
        shot_made_flag      int,
        game_date           varchar,
        htm                 varchar,
        vtm                 varchar,
        season              varchar
    );
""")


# INSERT RECORDS
play_by_play_insert = (f""" INSERT INTO play_by_play_{get_game_id()} (action_number, clock, time_actual, \
    period, action_type, description, score_home, score_away, game_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

home_player_stats_insert = (f""" INSERT INTO home_player_stats_{get_game_id()} ( \
        team_id,
        player_id,
        player_name,
        position,
        jersey,
        field_goals_attempted,
        field_goals_made,
        field_goals_percentage,
        three_pointers_attemp,
        three_pointers_made,
        three_pointers_percentage,
        two_pointers_attempted,
        two_pointers_made,
        two_pointers_percentage,
        free_throws_attemp,
        free_throws_made,
        free_throws_percentage,
        points,
        minutes,
        rebounds_defensive,
        rebounds_offensive,
        rebounds_total,
        fouls_personal,
        assists,
        steals,
        turnovers,
        game_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

away_player_stats_insert = (f""" INSERT INTO away_player_stats_{get_game_id()} ( \
        team_id,
        player_id,
        player_name,
        position,
        jersey,
        field_goals_attempted,
        field_goals_made,
        field_goals_percentage,
        three_pointers_attemp,
        three_pointers_made,
        three_pointers_percentage,
        two_pointers_attempted,
        two_pointers_made,
        two_pointers_percentage,
        free_throws_attemp,
        free_throws_made,
        free_throws_percentage,
        points,
        minutes,
        rebounds_defensive,
        rebounds_offensive,
        rebounds_total,
        fouls_personal,
        assists,
        steals,
        turnovers,
        game_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

home_shotchart_insert = (f""" INSERT INTO home_shotchart_stats_{get_game_id()} ( \
        grid_type, 
        game_id, 
        game_event_id, 
        player_id, 
        player_name, 
        team_id, 
        team_name, 
        period, 
        minutes_remaining, 
        seconds_remaining, 
        event_type, 
        action_type, 
        shot_type, 
        shot_zone_basic, 
        shot_zone_area, 
        shot_zone_range, 
        shot_distance, 
        loc_y, 
        loc_x, 
        shot_attempted_flag, 
        shot_made_flag, 
        game_date, 
        htm, 
        vtm, 
        season)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

away_shotchart_insert = (f""" INSERT INTO away_shotchart_stats_{get_game_id()} ( \
        grid_type, 
        game_id, 
        game_event_id, 
        player_id, 
        player_name, 
        team_id, 
        team_name, 
        period, 
        minutes_remaining, 
        seconds_remaining, 
        event_type, 
        action_type, 
        shot_type, 
        shot_zone_basic, 
        shot_zone_area, 
        shot_zone_range, 
        shot_distance, 
        loc_y, 
        loc_x, 
        shot_attempted_flag, 
        shot_made_flag, 
        game_date, 
        htm, 
        vtm, 
        season)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

home_hist_shotchart_insert = (f""" INSERT INTO home_hist_shotchart_stats_{get_game_id()} ( \
        grid_type, 
        game_id, 
        game_event_id, 
        player_id, 
        player_name, 
        team_id, 
        team_name, 
        period, 
        minutes_remaining, 
        seconds_remaining, 
        event_type, 
        action_type, 
        shot_type, 
        shot_zone_basic, 
        shot_zone_area, 
        shot_zone_range, 
        shot_distance, 
        loc_y, 
        loc_x, 
        shot_attempted_flag, 
        shot_made_flag, 
        game_date, 
        htm, 
        vtm, 
        season)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

away_hist_shotchart_insert = (f""" INSERT INTO away_hist_shotchart_stats_{get_game_id()} ( \
        grid_type, 
        game_id, 
        game_event_id, 
        player_id, 
        player_name, 
        team_id, 
        team_name, 
        period, 
        minutes_remaining, 
        seconds_remaining, 
        event_type, 
        action_type, 
        shot_type, 
        shot_zone_basic, 
        shot_zone_area, 
        shot_zone_range, 
        shot_distance, 
        loc_y, 
        loc_x, 
        shot_attempted_flag, 
        shot_made_flag, 
        game_date, 
        htm, 
        vtm, 
        season)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

