# Import Required Libraries


from pymongo import MongoClient
from bson.json_util import dumps
from bson.json_util import loads
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine



# --------------- Establish MongoDB Connection ---------------------


client = MongoClient('localhost', 27017)

db = client['NBA-Stats']

player_cltn = db['players']
games_cltn = db['Games']
player_game_stats = db['player_game_stats']



# --------------- Establish PostgreSQL Connection --------------------


conn = create_engine('postgresql+psycopg2://postgres:postgres\
@localhost/nba_stats')



# ------- Define Function to Retrieve MongoDB Query Results ----------


def get_results(query):
    return loads(dumps(query))



# --------- Initialize Dictionary to Load into "player_game_stats" Collection ---


# Get All Relevant Data from "Player_Stats" Collection

query = get_results(player_cltn.find({}, {"resultSets": {"$slice": [0, 1]}}))

# Define Dictionary Keys as List

headers = query[0]['resultSets'][0]['headers']

# Define Dictionary Values as List

results = query[0]['resultSets'][0]['rowSet'][0]

# Initialize Dictionary with "headers" as Keys and "results" as Values

res = {}
for key in headers:
            # res[key] = results[vali]
            for value in results:
                res[key] = value
                results.remove(value)
                break
            

# ------ Load All Data From "Player_Stats" Collection to Pandas DataFrame --------


# define DataFrame with test data

df = pd.DataFrame(res, index=[0])

# redefine "query" variable

query = get_results(player_cltn.find({}, {"resultSets": {"$slice": [0, 1]}}))

# redefine "headers" variable

headers = query[0]['resultSets'][0]['headers']
headers.append('player_name')

# Use tqdm to view upload progress

for player in tqdm(query, ):
    for results in player['resultSets'][0]['rowSet']:
        results.append(player['player name'])
        res = {}
        for key in headers:
            for value in results:
                res[key] = value
                results.remove(value)
                break
        df = pd.concat([df, pd.DataFrame(res, index=[0])])
        
temp_player_stats = df 


# -------- Add Descriptive Column to Pandas DataFrame ------------

temp_player_stats['Category'] = 'Regular Season Totals - By Season'


# --------- Remove Null Values and Convert Data Types as Necessary --------


# define integer columns as list

int_columns = ['GS', 'FGM', 'FGA', 'FG3M', 'FG3A', 'OREB', 'DREB', 'REB', 'STL', 'BLK', 'TOV']

# define float columns as list

float_columns = ['MIN', 'FG_PCT', 'FG3_PCT', 'FT_PCT']


# convert all integer columns (as previously defined) to integer data type

for column in int_columns:
    temp_player_stats[column].fillna(value=-1, inplace=True)
    temp_player_stats[column] = temp_player_stats[column].astype(int)

# convert all float columns (as previously defined) to float data type

for column in float_columns:
    temp_player_stats[column].dropna(inplace=True)
    temp_player_stats[column] = temp_player_stats[column].astype(float)
    

# -------- Load Pandas DataFrame to PostgreSQL "Staging" schema as "stg_player_season_stats"

temp_player_stats.to_sql('stg_player_season_stats', con=conn, index=False, if_exists='append', schema='staging')


# -------- Gather All Team Season Data from MongoDB, Convert to Pandas DataFrame, and Upload to PostgreSQL "Staging" Schema as "stg_[season]_games"


# define seasons to collect (exclude 2005-06 and 2011-12 due to lack of data)

seasons = ["2003-04", "2004-05", "2006-07", "2007-08", "2008-09", "2009-10", "2010-11", "2012-13", "2013-14", "2014-15", "2015-16", "2016-17", "2017-18", "2018-19", "2019-20", "2020-21", "2021-22", "2022-23", "2023-24"]

# primary data collection loop

for season in seasons:
    results = get_results(games_cltn.find({"SEASON_YEAR":season}))
    try:
        res = results[0]
        df = pd.DataFrame(res, index=[0])
    except IndexError:
        pass
    
    i = 1
    for player in tqdm(results, ):
        try:
            res = results[i]
            df = pd.concat([df, pd.DataFrame(res, index=[0])])
        except IndexError:
            pass
        i += 1
    try:
        df.drop(columns=['_id'], inplace=True)
    except:
        df.to_sql(f"stg_{season}_games", index=False, schema='staging', if_exists='replace', con=conn)
    df.to_sql(f"stg_{season}_games", index=False, schema='staging', if_exists='replace', con=conn)
    
    
# ---------- Gather All Player Game Stats from API ------------------

query = get_results(db.player_game_stats.find({}))


# ------- Collate All Query Results Into DataFrames and Upload Each DataFrame to the Staging Schema as the "stg_player_game_stats" Table -----

df_concatenated1 = pd.concat([pd.DataFrame(row, index=[0]).drop(columns=['_id']) for row in tqdm(query[:100000],)], ignore_index=True)
df_concatenated1.to_sql('stg_player_game_stats', con=conn, index=False, schema='staging', if_exists='replace')
print('Batch 1 uploaded')
df_concatenated2 = pd.concat([pd.DataFrame(row, index=[0]).drop(columns=['_id']) for row in tqdm(query[100000:200000],)], ignore_index=True)
df_concatenated2.to_sql('stg_player_game_stats', con=conn, index=False, schema='staging', if_exists='append')
print('Batch 2 uploaded')
df_concatenated3 = pd.concat([pd.DataFrame(row, index=[0]).drop(columns=['_id']) for row in tqdm(query[200000:300000],)], ignore_index=True)
df_concatenated3.to_sql('stg_player_game_stats', con=conn, index=False, schema='staging', if_exists='append')
print('Batch 3 uploaded')
df_concatenated4 = pd.concat([pd.DataFrame(row, index=[0]).drop(columns=['_id']) for row in tqdm(query[300000:400000],)], ignore_index=True)
df_concatenated4.to_sql('stg_player_game_stats', con=conn, index=False, schema='staging', if_exists='append')
print('Batch 4 uploaded')
df_concatenated5 = pd.concat([pd.DataFrame(row, index=[0]).drop(columns=['_id']) for row in tqdm(query[400000:],)], ignore_index=True)
df_concatenated5.to_sql('stg_player_game_stats', con=conn, index=False, schema='staging', if_exists='append')
print('Batch 5 uploaded')