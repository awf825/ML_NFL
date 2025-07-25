import re
import sqlite3
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta


def get_date(date_string):
    year1,month,day = re.search(r'(\d+)-\d+-(\d\d)(\d\d)', date_string).groups()
    year = year1 if int(month) > 8 else int(year1) + 1
    return datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')

con = sqlite3.connect("../../Data/odds.sqlite")
datasets = ["odds_2023-24", "odds_2022-23", "odds_2021-22"]
for dataset in tqdm(datasets):
    data = pd.read_sql_query(f"select * from \"{dataset}\"", con, index_col="index")
    teams_last_played = {}
    for index, row in data.iterrows():
        if 'Home' not in row or 'Away' not in row:
            continue
        if row['Home'] not in teams_last_played:
            teams_last_played[row['Home']] = get_date(row['Date'])
            home_games_rested = 10 # start of season, big number
        else:
            current_date = get_date(row['Date'])
            home_games_rested = (current_date - teams_last_played[row['Home']]).days if 0 < (current_date - teams_last_played[row['Home']]).days < 9 else 9
            teams_last_played[row['Home']] = current_date
        if row['Away'] not in teams_last_played:
            teams_last_played[row['Away']] = get_date(row['Date'])
            away_games_rested = 10 # start of season, big number
        else:
            current_date = get_date(row['Date'])
            away_games_rested = (current_date - teams_last_played[row['Away']]).days if 0 < (current_date - teams_last_played[row['Away']]).days < 9 else 9
            teams_last_played[row['Away']] = current_date
        
        # update date
        data.at[index,'Days_Rest_Home'] = home_games_rested
        data.at[index,'Days_Rest_Away'] = away_games_rested

        # print(f"{row['Away']} @ {row['Home']} games rested: {away_games_rested} @ {home_games_rested}")

    # write data to db
    data.to_sql(dataset, con, if_exists="replace")

con.close()
