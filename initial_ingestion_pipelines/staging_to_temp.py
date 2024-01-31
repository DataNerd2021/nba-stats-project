# -------- Import Required Libraries ---------

import polars as pl
import pandas as pd
from datetime import datetime


# -------- Establish Connecion with PostgreSQL --------


uri = 'postgresql://postgres:postgres@localhost:5432/nba_stats'


# ------- Clean "stg_player_game_stats" Table -----------


# get table data and define as Polars DataFrame

player_season_stats = pl.read_sql('SELECT * FROM staging.stg_player_season_stats', connection_uri=uri)


# filter table to only include seasons after 2003

player_season_stats = player_season_stats.filter(pl.col('SEASON_ID').is_in(['2003-04', '2004-05', '2005-06', '2006-07', '2007-08', '2008-09', '2009-10', '2010-11', '2012-13', '2013-14', '2014-15', '2015-16', '2016-17', '2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23', '2023-24']))


# make column headers lowercase

player_season_stats.columns = [x.lower() for x in player_season_stats.columns]


# drop unnecessary columns

player_season_stats = player_season_stats.drop('league_id')


# rename column headers

player_season_stats = player_season_stats.rename({'season_id':'season', 'gp':'games_played', 'gs':'games_started', 'min':'minutes_played', 'fgm':'field_goals_made', 'fga':'field_goal_attempts', 'fg_pct':'field_goal_percentage', 'fg3m':'3pt_made', 'fg3a':'3pt_attempts', 'fg3_pct':'3pt_percentage', 'ftm':'free_throws_made', 'fta':'free_throw_attempts', 'ft_pct':'free_throw_percentage', 'oreb':'offensive_rebounds', 'dreb':'defensive_rebounds', 'reb':'rebounds', 'ast':'assists', 'stl':'steals', 'blk':'blocks', 'tov':'turnovers', 'pf':'personal_fouls', 'pts':'points'})


# create "team_name" column

dic = { 'ATL': 'Atlanta Hawks', 'BKN': 'Brooklyn Nets', 'BOS': 'Boston Celtics', 'CHA':'Charlotte Hornets', 'CHI':'Chicago Bulls', 'CLE':'Cleveland Cavaliers', 'DAL':'Dallas Mavericks', 'DEN':'Denver Nuggets', 'DET':'Detroit Pistons', 'GSW':'Golden State Warriors', 'HOU':'Houston Rockets', 'IND':'Indiana Pacers', 'LAC':'LA Clippers', 'LAL':'Los Angeles Lakers', 'MEM':'Memphis Grizzlies', 'MIA':'Miami Heat', 'MIL':'Milwaukee Bucks', 'MIN':'Minnesota Timberwolves', 'NJN':'New Jersey Nets', 'NOH':'New Orleans Hornets', 'NOK':'New Orleans/Oklahoma City Hornets', 'NOP':'New Orleans Pelicans', 'NYK':'New York Knicks', 'OKC':'Oklahoma City Thunder', 'ORL':'Orlando Magic', 'PHI':'Philadelphia 76ers', 'PHX':'Phoenix Suns', 'POR':'Portland Trail Blazers', 'SAC':'Sacramento Kings', 'SAS':'San Antonio Spurs', 'SEA':'Seattle SuperSonics', 'TOR':'Toronto Raptors', 'UTA':'Utah Jazz', 'WAS':'Washington Wizards'}

mapper = pl.DataFrame({
    "keys": list(dic.keys()),
    "team_name": list(dic.values())
})

team_names = pl.DataFrame(pl.Series(player_season_stats['team_abbreviation']).to_frame("keys").join(mapper, on="keys", how="left").to_series(1))

player_season_stats = pl.concat([player_season_stats, team_names], how='horizontal')


# reorder columns

player_season_stats = player_season_stats.select(['player_id', 'season', 'player_name', 'team_id', 'team_name', 'player_age', 'games_played', 'games_started', 'minutes_played', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'field_goals_made', 'field_goal_attempts', 'field_goal_percentage', '3pt_made', '3pt_attempts', '3pt_percentage', 'free_throws_made', 'free_throw_attempts', 'free_throw_percentage', 'offensive_rebounds', 'defensive_rebounds', 'personal_fouls'])


# load final DataFrame to PostgreSQl "temp" Schema as "player_season_stats"

player_season_stats = player_season_stats.to_pandas()

player_season_stats.to_sql('player_season_stats', con=uri, index=False, if_exists='replace', schema='temp')


# ------------ Clean All "stg_[season]_games" Tables ----------------


# collate all tables as DataFrames

stg_2004 = pl.read_sql('SELECT * FROM staging."stg_2003-04_games"', connection_uri=uri)
stg_2005 = pl.read_sql('SELECT * FROM staging."stg_2004-05_games"', connection_uri=uri)
stg_2007 = pl.read_sql('SELECT * FROM staging."stg_2006-07_games"', connection_uri=uri)
stg_2008 = pl.read_sql('SELECT * FROM staging."stg_2007-08_games"', connection_uri=uri)
stg_2009 = pl.read_sql('SELECT * FROM staging."stg_2008-09_games"', connection_uri=uri)
stg_2010 = pl.read_sql('SELECT * FROM staging."stg_2009-10_games"', connection_uri=uri)
stg_2011 = pl.read_sql('SELECT * FROM staging."stg_2010-11_games"', connection_uri=uri)
stg_2013 = pl.read_sql('SELECT * FROM staging."stg_2012-13_games"', connection_uri=uri)
stg_2014 = pl.read_sql('SELECT * FROM staging."stg_2013-14_games"', connection_uri=uri)
stg_2015 = pl.read_sql('SELECT * FROM staging."stg_2014-15_games"', connection_uri=uri)
stg_2016 = pl.read_sql('SELECT * FROM staging."stg_2015-16_games"', connection_uri=uri)
stg_2017 = pl.read_sql('SELECT * FROM staging."stg_2016-17_games"', connection_uri=uri)
stg_2018 = pl.read_sql('SELECT * FROM staging."stg_2017-18_games"', connection_uri=uri)
stg_2019 = pl.read_sql('SELECT * FROM staging."stg_2018-19_games"', connection_uri=uri)
stg_2020 = pl.read_sql('SELECT * FROM staging."stg_2019-20_games"', connection_uri=uri)
stg_2021 = pl.read_sql('SELECT * FROM staging."stg_2020-21_games"', connection_uri=uri)
stg_2022 = pl.read_sql('SELECT * FROM staging."stg_2021-22_games"', connection_uri=uri)
stg_2023 = pl.read_sql('SELECT * FROM staging."stg_2022-23_games"', connection_uri=uri)
stg_2024 = pl.read_sql('SELECT * FROM staging."stg_2023-24_games"', connection_uri=uri)

team_game_stats = pl.concat([stg_2004, stg_2005, stg_2007, stg_2008, stg_2009, stg_2010, stg_2011, stg_2013, stg_2014, stg_2015, stg_2016, stg_2017, stg_2018, stg_2019, stg_2020, stg_2021, stg_2022, stg_2023, stg_2024])


# make column headers lowercase

team_game_stats.columns = [x.lower() for x in team_game_stats.columns]


# remove unnecessary columns

team_game_stats = team_game_stats.drop(['blka', 'gp_rank', 'w_rank', 'l_rank', 'w_pct_rank', 'min_rank', 'fgm_rank', 'fga_rank', 'fg_pct_rank', 'fg3m_rank', 'fg3a_rank', 'fg3_pct_rank', 'ftm_rank', 'fta_rank', 'ft_pct_rank', 'oreb_rank', 'dreb_rank', 'reb_rank', 'ast_rank', 'tov_rank', 'stl_rank', 'blk_rank', 'blka_rank', 'pf_rank', 'pfd_rank', 'pts_rank', 'plus_minus_rank', 'available_flag'])


# rename column headers

team_game_stats = team_game_stats.rename({'season_year':'season', 'wl':'win_lose', 'min':'game_length_minutes', 'fgm':'field_goals_made', 'fga':'field_goal_attempts', 'fg_pct':'field_goal_percentage', 'fg3m':'3pt_made', 'fg3a':'3pt_attempts', 'fg3_pct':'3pt_percentage', 'ftm':'free_throws_made', 'fta':'free_throw_attempts', 'ft_pct':'free_throw_percentage', 'oreb':'offensive_rebounds', 'dreb':'defensive_rebounds', 'reb':'rebounds', 'ast':'assists', 'tov':'turnovers', 'stl':'steals', 'blk':'blocks', 'pf':'personal_fouls', 'pfd':'personal_fouls_drawn', 'pts':'points'})


# split "matchup" column into three columns

team_game_stats = team_game_stats.with_columns(pl.col('matchup').str.split(' '))


# create "game_type" column based on second value in "matchup" column split

team_game_stats = team_game_stats.with_columns(pl.col("matchup").apply(lambda x: x[1] if len(x) >= 3 else None, return_dtype=pl.Object).alias('game_type'))


# create "opponent" column based on third value in "matchup" column split

team_game_stats = team_game_stats.with_columns(pl.col("matchup").apply(lambda x: x[2] if len(x) >= 3 else None, return_dtype=pl.Object).alias('opponent'))


# replace values in "opponent" column with team full names

dic = { 'ATL': 'Atlanta Hawks', 'BKN': 'Brooklyn Nets', 'BOS': 'Boston Celtics', 'CHA':'Charlotte Hornets', 'CHI':'Chicago Bulls', 'CLE':'Cleveland Cavaliers', 'DAL':'Dallas Mavericks', 'DEN':'Denver Nuggets', 'DET':'Detroit Pistons', 'GSW':'Golden State Warriors', 'HOU':'Houston Rockets', 'IND':'Indiana Pacers', 'LAC':'LA Clippers', 'LAL':'Los Angeles Lakers', 'MEM':'Memphis Grizzlies', 'MIA':'Miami Heat', 'MIL':'Milwaukee Bucks', 'MIN':'Minnesota Timberwolves', 'NJN':'New Jersey Nets', 'NOH':'New Orleans Hornets', 'NOK':'New Orleans/Oklahoma City Hornets', 'NOP':'New Orleans Pelicans', 'NYK':'New York Knicks', 'OKC':'Oklahoma City Thunder', 'ORL':'Orlando Magic', 'PHI':'Philadelphia 76ers', 'PHX':'Phoenix Suns', 'POR':'Portland Trail Blazers', 'SAC':'Sacramento Kings', 'SAS':'San Antonio Spurs', 'SEA':'Seattle SuperSonics', 'TOR':'Toronto Raptors', 'UTA':'Utah Jazz', 'WAS':'Washington Wizards'}

team_game_stats = team_game_stats.with_columns('opponent', team_game_stats['opponent'].apply(lambda x: dic.get(x, x)))


# reorder columns and drop unnecessary columns

team_game_stats = team_game_stats.select(['season', 'team_id', 'team_name', 'game_type', 'opponent', 'game_id', 'game_date', 'win_lose', 'game_length_minutes', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'field_goals_made', 'field_goal_attempts', '3pt_made', '3pt_attempts', '3pt_percentage', 'free_throws_made', 'free_throw_attempts', 'free_throw_percentage', 'offensive_rebounds', 'defensive_rebounds', 'personal_fouls', 'personal_fouls_drawn', 'plus_minus'])


# create "matchup_title" by concatenating "team_name", "game_type", and "opponent" columns

team_game_stats = team_game_stats.with_columns((team_game_stats['team_name'].apply(str) + " " + team_game_stats['game_type'].apply(str) + " " + team_game_stats['opponent'].apply(str)).alias('matchup_title'))


# convert "game_type" column to string and replace values

team_game_stats = team_game_stats.with_columns((team_game_stats['game_type'].apply(str)))

team_game_stats = team_game_stats.with_columns((pl.when(pl.col('game_type') == 'vs.').then('Home').otherwise('Away')).alias('game_type'))


# reorder columns and drop unnecessary columns

team_game_stats = team_game_stats.select(['season', 'team_id', 'team_name', 'opponent', 'game_type', 'game_id', 'game_date', 'matchup_title', 'win_lose', 'game_length_minutes', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'field_goals_made', 'field_goal_attempts', '3pt_made', '3pt_attempts', '3pt_percentage', 'free_throws_made', 'free_throw_attempts', 'free_throw_percentage', 'offensive_rebounds', 'defensive_rebounds', 'personal_fouls', 'personal_fouls_drawn', 'plus_minus'])


# convert "game_date" to Date data type

team_game_stats = team_game_stats.with_columns(team_game_stats['game_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')).cast(pl.Date))


# load final DataFrame to PostgreSQL "temp" schema as "team_game_stats"

team_game_stats = team_game_stats.to_pandas()

team_game_stats.to_sql('team_game_stats', con=uri, index=False, if_exists='replace', schema='temp')



# ---------- Clean "stg_player_game_stats" Table


# get table data and define as Polars DataFrame

player_game_stats = pl.read_sql('SELECT * FROM staging.stg_player_game_stats', connection_uri=uri)


# make column headers lowercase

player_game_stats.columns = [x.lower() for x in player_game_stats.columns]


# remove unnecessary columns

player_game_stats = player_game_stats.drop(['nickname', 'blka', 'nba_fantasy_pts', 'dd2', 'td3', 'wnba_fantasy_pts', 'gp_rank', 'w_rank', 'l_rank', 'w_pct_rank', 'min_rank', 'fgm_rank', 'fga_rank', 'fg_pct_rank', 'fg3m_rank', 'fg3a_rank', 'fg3_pct_rank', 'ftm_rank', 'fta_rank', 'ft_pct_rank', 'oreb_rank', 'dreb_rank', 'reb_rank', 'ast_rank', 'tov_rank', 'stl_rank', 'blk_rank', 'blka_rank', 'pf_rank', 'pfd_rank', 'pts_rank', 'plus_minus_rank', 'nba_fantasy_pts_rank', 'dd2_rank', 'td3_rank', 'wnba_fantasy_pts_rank', 'available_flag'])


# rename column headers

player_game_stats = player_game_stats.rename({'season_year':'season', 'wl':'win_lose', 'min':'minutes_played', 'fgm':'field_goals_made', 'fga':'field_goal_attempts', 'fg_pct':'field_goal_percentage', 'fg3m':'3pt_made', 'fg3a':'3pt_attempts', 'fg3_pct':'3pt_percentage', 'ftm':'free_throws_made', 'fta':'free_throw_attempts', 'ft_pct':'free_throw_percentage', 'oreb':'offensive_rebounds', 'dreb':'defensive_rebounds', 'reb':'rebounds', 'ast':'assists', 'tov':'turnovers', 'stl':'steals', 'blk':'blocks', 'pf':'personal_fouls', 'pfd':'personal_fouls_drawn', 'pts':'points'})


# split "matchup" column into three columns

player_game_stats = player_game_stats.with_columns(pl.col('matchup').str.split(' '))


# create "game_type" column based on second value in "matchup" column split

player_game_stats = player_game_stats.with_columns(pl.col("matchup").apply(lambda x: x[1] if len(x) >= 3 else None, return_dtype=pl.Object).alias('game_type'))


# create "opponent" column based on third value in "matchup" column split

player_game_stats = player_game_stats.with_columns(pl.col("matchup").apply(lambda x: x[2] if len(x) >= 3 else None, return_dtype=pl.Object).alias('opponent'))


# replace values in "opponent" column with team full names

dic = { 'ATL': 'Atlanta Hawks', 'BKN': 'Brooklyn Nets', 'BOS': 'Boston Celtics', 'CHA':'Charlotte Hornets', 'CHI':'Chicago Bulls', 'CLE':'Cleveland Cavaliers', 'DAL':'Dallas Mavericks', 'DEN':'Denver Nuggets', 'DET':'Detroit Pistons', 'GSW':'Golden State Warriors', 'HOU':'Houston Rockets', 'IND':'Indiana Pacers', 'LAC':'LA Clippers', 'LAL':'Los Angeles Lakers', 'MEM':'Memphis Grizzlies', 'MIA':'Miami Heat', 'MIL':'Milwaukee Bucks', 'MIN':'Minnesota Timberwolves', 'NJN':'New Jersey Nets', 'NOH':'New Orleans Hornets', 'NOK':'New Orleans/Oklahoma City Hornets', 'NOP':'New Orleans Pelicans', 'NYK':'New York Knicks', 'OKC':'Oklahoma City Thunder', 'ORL':'Orlando Magic', 'PHI':'Philadelphia 76ers', 'PHX':'Phoenix Suns', 'POR':'Portland Trail Blazers', 'SAC':'Sacramento Kings', 'SAS':'San Antonio Spurs', 'SEA':'Seattle SuperSonics', 'TOR':'Toronto Raptors', 'UTA':'Utah Jazz', 'WAS':'Washington Wizards'}

player_game_stats = player_game_stats.with_columns('opponent', player_game_stats['opponent'].apply(lambda x: dic.get(x, x)))


# reorder columns and drop unnecessary columns

player_game_stats = player_game_stats.select(['season', 'player_id', 'player_name', 'team_id', 'team_name', 'game_type', 'opponent', 'game_id', 'game_date', 'win_lose', 'minutes_played', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'field_goals_made', 'field_goal_attempts', '3pt_made', '3pt_attempts', '3pt_percentage', 'free_throws_made', 'free_throw_attempts', 'free_throw_percentage', 'offensive_rebounds', 'defensive_rebounds', 'personal_fouls', 'personal_fouls_drawn', 'plus_minus'])


# create "matchup_title" column by concatenating "team_name", "game_type", and "opponent" columns

player_game_stats = player_game_stats.with_columns((player_game_stats['team_name'].apply(str) + " " + player_game_stats['game_type'].apply(str) + " " + player_game_stats['opponent'].apply(str)).alias('matchup_title'))


# convert "game_type" column to string and replace values

player_game_stats = player_game_stats.with_columns((player_game_stats['game_type'].apply(str)))
player_game_stats = player_game_stats.with_columns((pl.when(pl.col('game_type') == 'vs.').then('Home').otherwise('Away')).alias('game_type'))


# reorder columns and drop unnecessary columns

player_game_stats = player_game_stats.select(['player_id', 'player_name', 'season', 'team_id', 'team_name', 'opponent', 'game_type', 'game_id', 'game_date', 'matchup_title', 'win_lose', 'minutes_played', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'field_goals_made', 'field_goal_attempts', '3pt_made', '3pt_attempts', '3pt_percentage', 'free_throws_made', 'free_throw_attempts', 'free_throw_percentage', 'offensive_rebounds', 'defensive_rebounds', 'personal_fouls', 'personal_fouls_drawn', 'plus_minus'])


# convert "game_date" column to Date

player_game_stats = player_game_stats.with_columns(player_game_stats['game_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')).cast(pl.Date))


# load final DataFrame to PostgreSQL "temp" schema as "player_game_stats"

player_game_stats = player_game_stats.to_pandas()

player_game_stats.to_sql('player_game_stats', con=uri, index=False, if_exists='replace', schema='temp')