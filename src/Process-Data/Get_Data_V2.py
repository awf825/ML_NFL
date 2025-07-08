## V2 -> 
#   USE ADVANCED PASSING STATS AND SAVE AS PASSER ROW FOR INDIVIDUAL QB. USE AGGREGATE PASSER DATA TO FILL GAME DF FOR TRAINING
#   DO THE SAME FOR ADVANCED RUSHING, BUT NO NEED FOR INDIVIUDAL RUSHERS
#   GATHER PUNTING stats and save to table. like passer, use this to populate game dataframe up front
#   ASSESS STRENGTH OF SCHEDULE in games df; use teams db table to record this 
#   USE PLAYER SNAPS TABLE TO ASSESS THE NUMBER OF OFFENSIVE LINEMAN WHO HAVE PLAYED AT LEAST 50% OF SNAPS IN A SINGLE GAME
#   record spread, weather, ROOF, AND surface 

import argparse
import os
import random
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from urllib.request import Request, urlopen
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, Comment

from abbrev import team_abbrev_index

parser = argparse.ArgumentParser(description='Model to Run')
parser.add_argument('-week', help="Most recent week of season (int)")
args = parser.parse_args()

from tqdm import tqdm

sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.table_headers import game_table_headers, team_table_headers
from src.Utils.get_div_match import get_div_match
from src.Utils.get_prev_year_rank import get_prev_year_rank

# years = [2018,2019,2020,2021,2022,2023,2024]
years = [2018]
# RANKS THROUGH 18 WEEKS; 18th is WC round until 2021
# weeks = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]

games_con = sqlite3.connect("../../Data/v2/games.sqlite")
teams_con = sqlite3.connect("../../Data/v2/teams.sqlite")
teams_cursor = teams_con.cursor()
passers_con = sqlite3.connect("../../Data/v2/passers.sqlite")
player_snaps_con = sqlite3.connect("../../Data/v2/player_snaps.sqlite")
player_snaps_cursor = player_snaps_con.cursor()

for year in years:
    season_data = []
    if year > 2020:
        weeks = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]
    else:
        weeks = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
    for week in weeks:
        vis_team_data = []
        vis_passer_data = []
        vis_punter_data = []
        vis_snaps_data = []

        home_team_data = []
        home_passer_data = []
        home_punter_data = []
        home_snaps_data = []

        print(f'scraping week {week}, {year}...')
        url = f"https://www.pro-football-reference.com/years/{year}/week_{week}.htm"
        req = Request(url)
        response = urlopen(req)
        soup = BeautifulSoup(response, features="html.parser")
        summaries = soup.findAll('div', {'class': 'game_summary'})
        for summary in summaries:
            time.sleep(3.5)
            try:
                gamelink = summary.find('td', {'class': 'gamelink'}).find('a')
                game_url = f"https://www.pro-football-reference.com{gamelink['href']}"
                game_req = Request(game_url)
                game_response = urlopen(game_req)
                print(game_response.code)
                game_soup = BeautifulSoup(game_response, features="html.parser")
                scoring_table = game_soup.find('table', id="scoring")
                home = scoring_table.find('th', {'data-stat': "home_team_score"}).get_text()
                vis = scoring_table.find('th', {'data-stat': "vis_team_score"}).get_text()
                last_row = scoring_table("tr")[-1]
                AWAY_SCORE = last_row.find('td', {'data-stat':'vis_team_score'}).get_text()
                HOME_SCORE = last_row.find('td', {'data-stat':'home_team_score'}).get_text()

                # could use more of this info in the future...

                scoring_table = game_soup.find('table', id="scoring")
                last_row = scoring_table("tr")[-1]

                # Find the commented-out HTML
                comments = game_soup.find_all(string=lambda text: isinstance(text, Comment))

                home_team_win = 0
                for comment in comments:
                    x = comment.extract()
                    vis_full = ''
                    home_full = ''
                    if "game_info" in x:
                        try:
                            comment_soup =  BeautifulSoup(x, 'html.parser')
                            table = comment_soup.find('table', id="game_info")
                            points_row = table.findAll("tr")[-1]
                            spread_row = table.findAll("tr")[-2]
                            weather_row = table.findAll("tr")[-3]
                            surface_row = table.findAll("tr")[-6]
                            roof_row = table.findAll("tr")[-7]

                            OU = points_row.find('td', {'data-stat':'stat'}).get_text().split(" ")[0]
                            if int(AWAY_SCORE)+int(HOME_SCORE) > float(OU):
                                OU_COVER = 1.0
                            else:
                                OU_COVER = 0.0

                            # spread is always expressed as the favorite
                            SPREAD = spread_row.find('td', {'data-stat':'stat'}).get_text().split("-")[1]
                            favorite = spread_row.find('td', {'data-stat':'stat'}).get_text().split("-")[0]
                            if favorite == vis_full and ( AWAY_SCORE-HOME_SCORE > SPREAD ):
                                SPREAD_COVER = 1.0
                            elif favorite == home_full and ( HOME_SCORE-AWAY_SCORE > SPREAD ):
                                SPREAD_COVER = 1.0
                            else:
                                SPREAD_COVER = 0.0

                            weather = weather_row.find('td', {'data-stat':'stat'}).get_text().split(",")
                            TEMP = weather[0].strip()
                            WIND_SPEED = weather[1].strip()

                            SURFACE = weather_row.find('td', {'data-stat':'stat'}).get_text().strip()
                            ROOF = roof_row.find('td', {'data-stat':'stat'}).get_text().strip()

                            # TODO: get divisional matchup status

                        except:
                            OU_COVER = -1
                            OU = -1
                            SPREAD = -1
                            SPREAD_COVER = -1
                            WIND_SPEED = -1
                            TEMP = -1
                            SURFACE = -1
                            ROOF = -1
                    if "team_stats" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="team_stats")
                        rows = table.findAll("tr")

                        vis_full = rows[0].find('th', {'data-stat':'vis_stat'}).get_text()
                        home_full = rows[0].find('th', {'data-stat':'home_stat'}).get_text()

                        # vis = team_abbrev_index.get(vis_full)
                        # home = team_abbrev_index.get(home_full)

                        ### START COLLECTING GAME STATS DATA
                        ### THIS WILL NEED TO BE INJECTED INTO TEAMS TABLES
                        ### BUT FOR THE GAME DF TO BE TRAINED ON, NEED TO TAKE A RUNNING AVERAGE (AND PERHAPS RANK) 
                        ### OF THE TEAM STATS, PASSER STATS, PUNTER STATS, 
                        
                        for row in rows[1:]:
                            table_row = []
                            scraped_rows = row.findAll(["td", "th"])
                            # triples of column name, visiting stat, home stat
                            column_name = scraped_rows[0].get_text()
                            vis_stat = scraped_rows[1].get_text()
                            home_stat = scraped_rows[2].get_text()
                            if column_name == "First Downs":
                                AWAY_FD = vis_stat
                                HOME_FD = home_stat
                            elif column_name == "Sacked-Yards":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                AWAY_SACKED = vis_split[0]
                                AWAY_SACKED_YDS = vis_split[1]
                                HOME_SACKED = home_split[0]
                                HOME_SACKED_YDS = home_split[1]

                            elif column_name == "Total Yards":
                                AWAY_TOTAL_YDS = vis_stat
                                HOME_TOTAL_YDS = home_stat
                            elif column_name == "Fumbles-Lost":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                AWAY_FUMBLES = vis_split[0]
                                AWAY_FUMBLES_LOST = vis_split[1]
                                HOME_FUMBLES = home_split[0]
                                HOME_FUMBLES_LOST = home_split[1]

                            elif column_name == "Turnovers":
                                AWAY_TO = vis_stat
                                HOME_TO = home_stat
                            elif column_name == "Penalties-Yards":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                AWAY_PENALTIES = vis_split[0]
                                AWAY_PENALTY_YARDS = vis_split[1]
                                HOME_PENALTIES = home_split[0]
                                HOME_PENALTY_YARDS = home_split[1]
                            elif column_name == "Third Down Conv.":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                if int(vis_split[1]) > 0:
                                    AWAY_3RD_DOWN = int(vis_split[0])
                                    AWAY_3RD_DOWN_ATT = int(vis_split[1])
                                    AWAY_3RD_CONV = int(vis_split[0]) / int(vis_split[1])
                                else:
                                    AWAY_3RD_DOWN = 0.0
                                    AWAY_3RD_DOWN_ATT = 0.0
                                    AWAY_3RD_CONV = 0.0

                                if int(home_split[1]) > 0:
                                    HOME_3RD_DOWN = int(home_split[0])
                                    HOME_3RD_DOWN_ATT = int(home_split[1])
                                    HOME_3RD_CONV = int(home_split[0]) / int(home_split[1])
                                else: 
                                    HOME_3RD_DOWN = 0.0
                                    HOME_3RD_DOWN_ATT = 0.0
                                    HOME_3RD_CONV = 0.0

                            elif column_name == "Time of Possession":
                                vis_split = vis_stat.split(":")
                                home_split = home_stat.split(":")

                                # round to nearest minute
                                if int(vis_split[1]) > 30:
                                    AWAY_TOP = int(vis_split[0])+1
                                else:
                                    AWAY_TOP = int(vis_split[0])

                                if int(home_split[1]) > 30:
                                    HOME_TOP = int(home_split[0])+1
                                else:
                                    HOME_TOP = int(home_split[0])
                            else: 
                                print('column invalid, OR NOT USED')

                        if int(HOME_SCORE) > int(AWAY_SCORE):
                            home_team_win = 1.0 
                        else:
                            home_team_win = 0.0

                        
                        #####################################################
                        #####   BEGIN COLLECTING DATA FOR TEAM TABLES   #####
                        #####################################################

                        # to get sos, get the win pct of each team and use it for sos of the other team
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"teams_{year}\"", teams_con)
                        if table.empty:
                            # if this table is empty, this data will never be available to any team
                            AWAY_SOS = -1
                            HOME_SOS = -1
                            AWAY_SOS_GAME = -1
                            HOME_SOS_GAME = -1
                            AWAY_FD_MEAN = -1 
                            AWAY_FD_STD = -1
                            AWAY_FD_MAX = -1
                            AWAY_FD_MIN = -1
                            AWAY_FD_AGAINST_MEAN = -1
                            AWAY_FD_AGAINST_STD = -1
                            AWAY_FD_AGAINST_MAX = -1
                            AWAY_FD_AGAINST_MIN = -1
                            AWAY_SACKS_MEAN = -1 
                            AWAY_SACKS_STD = -1
                            AWAY_SACKS_MAX = -1
                            AWAY_SACKS_MIN = -1
                            AWAY_SACKS_AGAINST_MEAN = -1 
                            AWAY_SACKS_AGAINST_STD = -1
                            AWAY_SACKS_AGAINST_MAX = -1
                            AWAY_SACKS_AGAINST_MIN = -1
                            AWAY_SACK_YDS_MEAN = -1 
                            AWAY_SACK_YDS_STD = -1
                            AWAY_SACK_YDS_MAX = -1
                            AWAY_SACK_YDS_MIN = -1
                            AWAY_SACK_YDS_AGAINST_MEAN = -1 
                            AWAY_SACK_YDS_AGAINST_STD = -1
                            AWAY_SACK_YDS_AGAINST_MAX = -1
                            AWAY_SACK_YDS_AGAINST_MIN = -1
                            AWAY_TOTAL_YDS_MEAN = -1 
                            AWAY_TOTAL_YDS_STD = -1
                            AWAY_TOTAL_YDS_MAX = -1
                            AWAY_TOTAL_YDS_MIN = -1
                            AWAY_FUMBLES_MEAN = -1 
                            AWAY_FUMBLES_STD = -1
                            AWAY_FUMBLES_MAX = -1
                            AWAY_FUMBLES_MIN = -1
                            AWAY_FUMBLES_LOST_MEAN = -1 
                            AWAY_FUMBLES_LOST_STD = -1
                            AWAY_FUMBLES_LOST_MAX = -1
                            AWAY_FUMBLES_LOST_MIN = -1
                            AWAY_TO_MEAN = -1 
                            AWAY_TO_STD = -1
                            AWAY_TO_MAX = -1
                            AWAY_TO_MIN = -1
                            AWAY_TO_AGAINST_MEAN = -1 
                            AWAY_TO_AGAINST_STD = -1
                            AWAY_TO_AGAINST_MAX = -1
                            AWAY_TO_AGAINST_MIN = -1
                            AWAY_PENALTIES_MEAN = -1 
                            AWAY_PENALTIES_STD = -1
                            AWAY_PENALTIES_MAX = -1
                            AWAY_PENALTIES_MIN = -1
                            AWAY_PENALTY_YARDS_MEAN = -1 
                            AWAY_PENALTY_YARDS_STD = -1
                            AWAY_PENALTY_YARDS_MAX = -1
                            AWAY_PENALTY_YARDS_MIN = -1
                            AWAY_3RD_DOWN_MEAN = -1 
                            AWAY_3RD_DOWN_STD = -1
                            AWAY_3RD_DOWN_MAX = -1
                            AWAY_3RD_DOWN_MIN = -1
                            AWAY_3RD_DOWN_ATT_MEAN = -1 
                            AWAY_3RD_DOWN_ATT_STD = -1
                            AWAY_3RD_DOWN_ATT_MAX = -1
                            AWAY_3RD_DOWN_ATT_MIN = -1
                            AWAY_3RD_DOWN_CONV_MEAN = -1 
                            AWAY_3RD_DOWN_CONV_STD = -1
                            AWAY_3RD_DOWN_CONV_MAX = -1
                            AWAY_3RD_DOWN_CONV_MIN = -1
                            AWAY_3RD_DOWN_AGAINST_MEAN = -1
                            AWAY_3RD_DOWN_AGAINST_STD = -1
                            AWAY_3RD_DOWN_AGAINST_MAX = -1
                            AWAY_3RD_DOWN_AGAINST_MIN = -1
                            AWAY_3RD_DOWN_ATT_AGAINST_MEAN = -1
                            AWAY_3RD_DOWN_ATT_AGAINST_STD = -1
                            AWAY_3RD_DOWN_ATT_AGAINST_MAX = -1
                            AWAY_3RD_DOWN_ATT_AGAINST_MIN = -1
                            AWAY_3RD_DOWN_CONV_AGAINST_MEAN = -1
                            AWAY_3RD_DOWN_CONV_AGAINST_STD = -1
                            AWAY_3RD_DOWN_CONV_AGAINST_MAX = -1
                            AWAY_3RD_DOWN_CONV_AGAINST_MIN = -1
                            AWAY_TOP_MEAN = -1 
                            AWAY_TOP_STD = -1
                            AWAY_TOP_MAX = -1
                            AWAY_TOP_MIN = -1 
                            HOME_FD_MEAN = -1 
                            HOME_FD_STD = -1
                            HOME_FD_MAX = -1
                            HOME_FD_MIN = -1
                            HOME_FD_AGAINST_MEAN = -1
                            HOME_FD_AGAINST_STD = -1
                            HOME_FD_AGAINST_MAX = -1
                            HOME_FD_AGAINST_MIN = -1
                            HOME_SACKS_MEAN = -1 
                            HOME_SACKS_STD = -1
                            HOME_SACKS_MAX = -1
                            HOME_SACKS_MIN = -1
                            HOME_SACKS_AGAINST_MEAN = -1 
                            HOME_SACKS_AGAINST_STD = -1
                            HOME_SACKS_AGAINST_MAX = -1
                            HOME_SACKS_AGAINST_MIN = -1
                            HOME_SACK_YDS_MEAN = -1 
                            HOME_SACK_YDS_STD = -1
                            HOME_SACK_YDS_MAX = -1
                            HOME_SACK_YDS_MIN = -1
                            HOME_SACK_YDS_AGAINST_MEAN = -1 
                            HOME_SACK_YDS_AGAINST_STD = -1
                            HOME_SACK_YDS_AGAINST_MAX = -1
                            HOME_SACK_YDS_AGAINST_MIN = -1
                            HOME_TOTAL_YDS_MEAN = -1 
                            HOME_TOTAL_YDS_STD = -1
                            HOME_TOTAL_YDS_MAX = -1
                            HOME_TOTAL_YDS_MIN = -1
                            HOME_FUMBLES_MEAN = -1 
                            HOME_FUMBLES_STD = -1
                            HOME_FUMBLES_MAX = -1
                            HOME_FUMBLES_MIN = -1
                            HOME_FUMBLES_LOST_MEAN = -1 
                            HOME_FUMBLES_LOST_STD = -1
                            HOME_FUMBLES_LOST_MAX = -1
                            HOME_FUMBLES_LOST_MIN = -1
                            HOME_TO_MEAN = -1 
                            HOME_TO_STD = -1
                            HOME_TO_MAX = -1
                            HOME_TO_MIN = -1
                            HOME_TO_AGAINST_MEAN = -1 
                            HOME_TO_AGAINST_STD = -1
                            HOME_TO_AGAINST_MAX = -1
                            HOME_TO_AGAINST_MIN = -1
                            HOME_PENALTIES_MEAN = -1 
                            HOME_PENALTIES_STD = -1
                            HOME_PENALTIES_MAX = -1
                            HOME_PENALTIES_MIN = -1
                            HOME_PENALTY_YARDS_MEAN = -1 
                            HOME_PENALTY_YARDS_STD = -1
                            HOME_PENALTY_YARDS_MAX = -1
                            HOME_PENALTY_YARDS_MIN = -1
                            HOME_3RD_DOWN_MEAN = -1 
                            HOME_3RD_DOWN_STD = -1
                            HOME_3RD_DOWN_MAX = -1
                            HOME_3RD_DOWN_MIN = -1
                            HOME_3RD_DOWN_ATT_MEAN = -1 
                            HOME_3RD_DOWN_ATT_STD = -1
                            HOME_3RD_DOWN_ATT_MAX = -1
                            HOME_3RD_DOWN_ATT_MIN = -1
                            HOME_3RD_DOWN_CONV_MEAN = -1 
                            HOME_3RD_DOWN_CONV_STD = -1
                            HOME_3RD_DOWN_CONV_MAX = -1
                            HOME_3RD_DOWN_CONV_MIN = -1
                            HOME_3RD_DOWN_AGAINST_MEAN = -1
                            HOME_3RD_DOWN_AGAINST_STD = -1
                            HOME_3RD_DOWN_AGAINST_MAX = -1
                            HOME_3RD_DOWN_AGAINST_MIN = -1
                            HOME_3RD_DOWN_ATT_AGAINST_MEAN = -1
                            HOME_3RD_DOWN_ATT_AGAINST_STD = -1
                            HOME_3RD_DOWN_ATT_AGAINST_MAX = -1
                            HOME_3RD_DOWN_ATT_AGAINST_MIN = -1
                            HOME_3RD_DOWN_CONV_AGAINST_MEAN = -1
                            HOME_3RD_DOWN_CONV_AGAINST_STD = -1
                            HOME_3RD_DOWN_CONV_AGAINST_MAX = -1
                            HOME_3RD_DOWN_CONV_AGAINST_MIN = -1
                            HOME_TOP_MEAN = -1 
                            HOME_TOP_STD = -1
                            HOME_TOP_MAX = -1
                            HOME_TOP_MIN = -1
                        else:
                            teams_cursor.execute(f"SELECT avg(WIN) FROM teams_{year} WHERE (WEEK < ?) and TEAM_NAME = ? order by WEEK desc;", (week, vis))
                            away_win_pct = teams_cursor.fetchone()
                            HOME_SOS_GAME = away_win_pct[0] #add to opposite teams sos

                            teams_cursor.execute(f"SELECT avg(WIN) FROM teams_{year} WHERE (WEEK < ?) and TEAM_NAME = ? order by WEEK desc;", (week, home))
                            home_win_pct = teams_cursor.fetchone()
                            AWAY_SOS_GAME = home_win_pct[0] #add to opposite teams sos

                            away_df = pd.read_sql_query(f"SELECT * FROM teams_{year} WHERE (WEEK < {week}) and TEAM_NAME = {vis} order by WEEK desc;", teams_con)
                            home_df = pd.read_sql_query(f"SELECT * FROM teams_{year} WHERE (WEEK < {week}) and TEAM_NAME = {home} order by WEEK desc;", teams_con)

                            AWAY_SOS = away_df['SOS'].mean()
                            HOME_SOS = home_df['SOS'].mean()
                            AWAY_FD_MEAN = away_df['FD'].mean()
                            AWAY_FD_STD = away_df['FD'].std()
                            AWAY_FD_MIN = away_df['FD'].min()
                            AWAY_FD_MAX = away_df['FD'].max()
                            AWAY_FD_AGAINST_MEAN = away_df['FD_AGAINST'].mean()
                            AWAY_FD_AGAINST_STD = away_df['FD_AGAINST'].std()
                            AWAY_FD_AGAINST_MAX = away_df['FD_AGAINST'].max()
                            AWAY_FD_AGAINST_MIN = away_df['FD_AGAINST'].min()
                            AWAY_SACKS_MEAN = away_df['SACKS'].mean() 
                            AWAY_SACKS_STD = away_df['SACKS'].std()
                            AWAY_SACKS_MAX = away_df['SACKS'].max()
                            AWAY_SACKS_MIN = away_df['SACKS'].min()
                            AWAY_SACKS_AGAINST_MEAN = away_df['SACKS_AGAINST'].mean() 
                            AWAY_SACKS_AGAINST_STD = away_df['SACKS_AGAINST'].std()
                            AWAY_SACKS_AGAINST_MAX = away_df['SACKS_AGAINST'].max()
                            AWAY_SACKS_AGAINST_MIN = away_df['SACKS_AGAINST'].min()
                            AWAY_SACK_YDS_MEAN = away_df['SACK_YDS'].mean() 
                            AWAY_SACK_YDS_STD = away_df['SACK_YDS'].std()
                            AWAY_SACK_YDS_MAX = away_df['SACK_YDS'].max()
                            AWAY_SACK_YDS_MIN = away_df['SACK_YDS'].min()
                            AWAY_SACK_YDS_AGAINST_MEAN = away_df['SACK_YDS_AGAINST'].mean() 
                            AWAY_SACK_YDS_AGAINST_STD = away_df['SACK_YDS_AGAINST'].std()
                            AWAY_SACK_YDS_AGAINST_MAX = away_df['SACK_YDS_AGAINST'].max()
                            AWAY_SACK_YDS_AGAINST_MIN = away_df['SACK_YDS_AGAINST'].min()
                            AWAY_TOTAL_YDS_MEAN = away_df['TOTAL_YDS'].mean() 
                            AWAY_TOTAL_YDS_STD = away_df['TOTAL_YDS'].std()
                            AWAY_TOTAL_YDS_MAX = away_df['TOTAL_YDS'].max()
                            AWAY_TOTAL_YDS_MIN = away_df['TOTAL_YDS'].min()
                            AWAY_FUMBLES_MEAN = away_df['FUMBLES'].mean() 
                            AWAY_FUMBLES_STD = away_df['FUMBLES'].std()
                            AWAY_FUMBLES_MAX = away_df['FUMBLES'].max()
                            AWAY_FUMBLES_MIN = away_df['FUMBLES'].min()
                            AWAY_FUMBLES_LOST_MEAN = away_df['FUMBLES_LOST'].mean() 
                            AWAY_FUMBLES_LOST_STD = away_df['FUMBLES_LOST'].std()
                            AWAY_FUMBLES_LOST_MAX = away_df['FUMBLES_LOST'].max()
                            AWAY_FUMBLES_LOST_MIN = away_df['FUMBLES_LOST'].min()
                            AWAY_TO_MEAN = away_df['TO'].mean() 
                            AWAY_TO_STD = away_df['TO'].std()
                            AWAY_TO_MAX = away_df['TO'].max()
                            AWAY_TO_MIN = away_df['TO'].min()
                            AWAY_TO_AGAINST_MEAN = away_df['TO_AGAINST'].mean() 
                            AWAY_TO_AGAINST_STD = away_df['TO_AGAINST'].std()
                            AWAY_TO_AGAINST_MAX = away_df['TO_AGAINST'].max()
                            AWAY_TO_AGAINST_MIN = away_df['TO_AGAINST'].min()
                            AWAY_PENALTIES_MEAN = away_df['PENALTIES'].mean() 
                            AWAY_PENALTIES_STD = away_df['PENALTIES'].std()
                            AWAY_PENALTIES_MAX = away_df['PENALTIES'].max()
                            AWAY_PENALTIES_MIN = away_df['PENALTIES'].min()
                            AWAY_PENALTY_YARDS_MEAN = away_df['PENALTY_YARDS'].mean() 
                            AWAY_PENALTY_YARDS_STD = away_df['PENALTY_YARDS'].std()
                            AWAY_PENALTY_YARDS_MAX = away_df['PENALTY_YARDS'].max()
                            AWAY_PENALTY_YARDS_MIN = away_df['PENALTY_YARDS'].min()
                            AWAY_3RD_DOWN_MEAN = away_df['3RD_DOWN'].mean() 
                            AWAY_3RD_DOWN_STD = away_df['3RD_DOWN'].std()
                            AWAY_3RD_DOWN_MAX = away_df['3RD_DOWN'].max()
                            AWAY_3RD_DOWN_MIN = away_df['3RD_DOWN'].min()
                            AWAY_3RD_DOWN_ATT_MEAN = away_df['3RD_DOWN_ATT'].mean() 
                            AWAY_3RD_DOWN_ATT_STD = away_df['3RD_DOWN_ATT'].std()
                            AWAY_3RD_DOWN_ATT_MAX = away_df['3RD_DOWN_ATT'].max()
                            AWAY_3RD_DOWN_ATT_MIN = away_df['3RD_DOWN_ATT'].min()
                            AWAY_3RD_DOWN_CONV_MEAN = away_df['3RD_DOWN_CONV'].mean() 
                            AWAY_3RD_DOWN_CONV_STD = away_df['3RD_DOWN_CONV'].std()
                            AWAY_3RD_DOWN_CONV_MAX = away_df['3RD_DOWN_CONV'].max()
                            AWAY_3RD_DOWN_CONV_MIN = away_df['3RD_DOWN_CONV'].min()
                            AWAY_3RD_DOWN_AGAINST_MEAN = away_df['3RD_DOWN_AGAINST'].mean()
                            AWAY_3RD_DOWN_AGAINST_STD = away_df['3RD_DOWN_AGAINST'].std()
                            AWAY_3RD_DOWN_AGAINST_MAX = away_df['3RD_DOWN_AGAINST'].max()
                            AWAY_3RD_DOWN_AGAINST_MIN = away_df['3RD_DOWN_AGAINST'].min()
                            AWAY_3RD_DOWN_ATT_AGAINST_MEAN = away_df['3RD_DOWN_ATT_AGAINST'].mean()
                            AWAY_3RD_DOWN_ATT_AGAINST_STD = away_df['3RD_DOWN_ATT_AGAINST'].std()
                            AWAY_3RD_DOWN_ATT_AGAINST_MAX = away_df['3RD_DOWN_ATT_AGAINST'].max()
                            AWAY_3RD_DOWN_ATT_AGAINST_MIN = away_df['3RD_DOWN_ATT_AGAINST'].min()
                            AWAY_3RD_DOWN_CONV_AGAINST_MEAN = away_df['3RD_DOWN_CONV_AGAINST'].mean()
                            AWAY_3RD_DOWN_CONV_AGAINST_STD = away_df['3RD_DOWN_CONV_AGAINST'].std()
                            AWAY_3RD_DOWN_CONV_AGAINST_MAX = away_df['3RD_DOWN_CONV_AGAINST'].max()
                            AWAY_3RD_DOWN_CONV_AGAINST_MIN = away_df['3RD_DOWN_CONV_AGAINST'].min()
                            AWAY_TOP_MEAN = away_df['TOP'].mean() 
                            AWAY_TOP_STD = away_df['TOP'].std()
                            AWAY_TOP_MAX = away_df['TOP'].max()
                            AWAY_TOP_MIN = away_df['TOP'].min() 
                            HOME_FD_MEAN = home_df['FD'].mean()
                            HOME_FD_STD = home_df['FD'].std()
                            HOME_FD_MIN = home_df['FD'].min()
                            HOME_FD_MAX = home_df['FD'].max()
                            HOME_FD_AGAINST_MEAN = home_df['FD_AGAINST'].mean()
                            HOME_FD_AGAINST_STD = home_df['FD_AGAINST'].std()
                            HOME_FD_AGAINST_MAX = home_df['FD_AGAINST'].max()
                            HOME_FD_AGAINST_MIN = home_df['FD_AGAINST'].min()
                            HOME_SACKS_MEAN = home_df['SACKS'].mean() 
                            HOME_SACKS_STD = home_df['SACKS'].std()
                            HOME_SACKS_MAX = home_df['SACKS'].max()
                            HOME_SACKS_MIN = home_df['SACKS'].min()
                            HOME_SACKS_AGAINST_MEAN = home_df['SACKS_AGAINST'].mean() 
                            HOME_SACKS_AGAINST_STD = home_df['SACKS_AGAINST'].std()
                            HOME_SACKS_AGAINST_MAX = home_df['SACKS_AGAINST'].max()
                            HOME_SACKS_AGAINST_MIN = home_df['SACKS_AGAINST'].min()
                            HOME_SACK_YDS_MEAN = home_df['SACK_YDS'].mean() 
                            HOME_SACK_YDS_STD = home_df['SACK_YDS'].std()
                            HOME_SACK_YDS_MAX = home_df['SACK_YDS'].max()
                            HOME_SACK_YDS_MIN = home_df['SACK_YDS'].min()
                            HOME_SACK_YDS_AGAINST_MEAN = home_df['SACK_YDS_AGAINST'].mean() 
                            HOME_SACK_YDS_AGAINST_STD = home_df['SACK_YDS_AGAINST'].std()
                            HOME_SACK_YDS_AGAINST_MAX = home_df['SACK_YDS_AGAINST'].max()
                            HOME_SACK_YDS_AGAINST_MIN = home_df['SACK_YDS_AGAINST'].min()
                            HOME_TOTAL_YDS_MEAN = home_df['TOTAL_YDS'].mean() 
                            HOME_TOTAL_YDS_STD = home_df['TOTAL_YDS'].std()
                            HOME_TOTAL_YDS_MAX = home_df['TOTAL_YDS'].max()
                            HOME_TOTAL_YDS_MIN = home_df['TOTAL_YDS'].min()
                            HOME_FUMBLES_MEAN = home_df['FUMBLES'].mean() 
                            HOME_FUMBLES_STD = home_df['FUMBLES'].std()
                            HOME_FUMBLES_MAX = home_df['FUMBLES'].max()
                            HOME_FUMBLES_MIN = home_df['FUMBLES'].min()
                            HOME_FUMBLES_LOST_MEAN = home_df['FUMBLES_LOST'].mean() 
                            HOME_FUMBLES_LOST_STD = home_df['FUMBLES_LOST'].std()
                            HOME_FUMBLES_LOST_MAX = home_df['FUMBLES_LOST'].max()
                            HOME_FUMBLES_LOST_MIN = home_df['FUMBLES_LOST'].min()
                            HOME_TO_MEAN = home_df['TO'].mean() 
                            HOME_TO_STD = home_df['TO'].std()
                            HOME_TO_MAX = home_df['TO'].max()
                            HOME_TO_MIN = home_df['TO'].min()
                            HOME_TO_AGAINST_MEAN = home_df['TO_AGAINST'].mean() 
                            HOME_TO_AGAINST_STD = home_df['TO_AGAINST'].std()
                            HOME_TO_AGAINST_MAX = home_df['TO_AGAINST'].max()
                            HOME_TO_AGAINST_MIN = home_df['TO_AGAINST'].min()
                            HOME_PENALTIES_MEAN = home_df['PENALTIES'].mean() 
                            HOME_PENALTIES_STD = home_df['PENALTIES'].std()
                            HOME_PENALTIES_MAX = home_df['PENALTIES'].max()
                            HOME_PENALTIES_MIN = home_df['PENALTIES'].min()
                            HOME_PENALTY_YARDS_MEAN = home_df['PENALTY_YARDS'].mean() 
                            HOME_PENALTY_YARDS_STD = home_df['PENALTY_YARDS'].std()
                            HOME_PENALTY_YARDS_MAX = home_df['PENALTY_YARDS'].max()
                            HOME_PENALTY_YARDS_MIN = home_df['PENALTY_YARDS'].min()
                            HOME_3RD_DOWN_MEAN = home_df['3RD_DOWN'].mean() 
                            HOME_3RD_DOWN_STD = home_df['3RD_DOWN'].std()
                            HOME_3RD_DOWN_MAX = home_df['3RD_DOWN'].max()
                            HOME_3RD_DOWN_MIN = home_df['3RD_DOWN'].min()
                            HOME_3RD_DOWN_ATT_MEAN = home_df['3RD_DOWN_ATT'].mean() 
                            HOME_3RD_DOWN_ATT_STD = home_df['3RD_DOWN_ATT'].std()
                            HOME_3RD_DOWN_ATT_MAX = home_df['3RD_DOWN_ATT'].max()
                            HOME_3RD_DOWN_ATT_MIN = home_df['3RD_DOWN_ATT'].min()
                            HOME_3RD_DOWN_CONV_MEAN = home_df['3RD_DOWN_CONV'].mean() 
                            HOME_3RD_DOWN_CONV_STD = home_df['3RD_DOWN_CONV'].std()
                            HOME_3RD_DOWN_CONV_MAX = home_df['3RD_DOWN_CONV'].max()
                            HOME_3RD_DOWN_CONV_MIN = home_df['3RD_DOWN_CONV'].min()
                            HOME_3RD_DOWN_AGAINST_MEAN = home_df['3RD_DOWN_AGAINST'].mean()
                            HOME_3RD_DOWN_AGAINST_STD = home_df['3RD_DOWN_AGAINST'].std()
                            HOME_3RD_DOWN_AGAINST_MAX = home_df['3RD_DOWN_AGAINST'].max()
                            HOME_3RD_DOWN_AGAINST_MIN = home_df['3RD_DOWN_AGAINST'].min()
                            HOME_3RD_DOWN_ATT_AGAINST_MEAN = home_df['3RD_DOWN_ATT_AGAINST'].mean()
                            HOME_3RD_DOWN_ATT_AGAINST_STD = home_df['3RD_DOWN_ATT_AGAINST'].std()
                            HOME_3RD_DOWN_ATT_AGAINST_MAX = home_df['3RD_DOWN_ATT_AGAINST'].max()
                            HOME_3RD_DOWN_ATT_AGAINST_MIN = home_df['3RD_DOWN_ATT_AGAINST'].min()
                            HOME_3RD_DOWN_CONV_AGAINST_MEAN = home_df['3RD_DOWN_CONV_AGAINST'].mean()
                            HOME_3RD_DOWN_CONV_AGAINST_STD = home_df['3RD_DOWN_CONV_AGAINST'].std()
                            HOME_3RD_DOWN_CONV_AGAINST_MAX = home_df['3RD_DOWN_CONV_AGAINST'].max()
                            HOME_3RD_DOWN_CONV_AGAINST_MIN = home_df['3RD_DOWN_CONV_AGAINST'].min()
                            HOME_TOP_MEAN = home_df['TOP'].mean() 
                            HOME_TOP_STD = home_df['TOP'].std()
                            HOME_TOP_MAX = home_df['TOP'].max()
                            HOME_TOP_MIN = home_df['TOP'].min()

                        # now append the data, so it doesn't mess up the historical load
                        vis_team_data.append({
                            'SEASON':year,
                            'WEEK':week,
                            'WIN': 0.0 if home_team_win else 1.0,
                            'TEAM_NAME':vis,
                            'SCORE':AWAY_SCORE,
                            'SOS':AWAY_SOS_GAME,
                            'TOP':AWAY_TOP, 
                            'FD':AWAY_FD,
                            'SACKS':AWAY_SACKED,
                            'SACK_YDS':AWAY_SACKED_YDS,
                            'TOTAL_YDS':AWAY_TOTAL_YDS,
                            'FUMBLES':AWAY_FUMBLES,
                            'FUMBLES_LOST':AWAY_FUMBLES_LOST,
                            'TO':AWAY_TO,
                            'PENALTIES':AWAY_PENALTIES,
                            'PENALTY_YARDS':AWAY_PENALTY_YARDS,
                            '3RD_DOWN':AWAY_3RD_DOWN,
                            '3RD_DOWN_ATT':AWAY_3RD_DOWN_ATT,
                            '3RD_CONV':AWAY_3RD_CONV,
                            'FD_AGAINST':HOME_FD,
                            'SACKS_AGAINST':HOME_SACKED,
                            'SACK_YDS_AGAINST':HOME_SACKED_YDS,
                            'TOTAL_YDS_AGAINST':HOME_TOTAL_YDS,
                            'FUMBLES_AGAINST':HOME_FUMBLES,
                            'FUMBLES_LOST_AGAINST':HOME_FUMBLES_LOST,
                            'TO_AGAINST':HOME_TO,
                            'PENALTIES_AGAINST':HOME_PENALTIES,
                            'PENALTY_YARDS_AGAINST':HOME_PENALTY_YARDS,
                            '3RD_DOWN_AGAINST':HOME_3RD_DOWN,
                            '3RD_DOWN_ATT_AGAINST':HOME_3RD_DOWN_ATT,
                            '3RD_CONV_AGAINST':HOME_3RD_CONV
                        })
                        vis_team_data = pd.DataFrame(vis_team_data, columns=team_table_headers)
                        vis_team_data.to_sql(f"teams_{year}", teams_con, if_exists="append")

                        home_team_data.append({
                            'SEASON':year,
                            'WEEK':week,
                            'WIN':home_team_win,
                            'TEAM_NAME':vis,
                            'SCORE':HOME_SCORE,
                            'SOS':HOME_SOS_GAME,
                            'TOP':HOME_TOP, 
                            'FD':HOME_FD,
                            'SACKS':HOME_SACKED,
                            'SACK_YDS':HOME_SACKED_YDS,
                            'TOTAL_YDS':HOME_TOTAL_YDS,
                            'FUMBLES':HOME_FUMBLES,
                            'FUMBLES_LOST':HOME_FUMBLES_LOST,
                            'TO':HOME_TO,
                            'PENALTIES':HOME_PENALTIES,
                            'PENALTY_YARDS':HOME_PENALTY_YARDS,
                            '3RD_DOWN':HOME_3RD_DOWN,
                            '3RD_DOWN_ATT':HOME_3RD_DOWN_ATT,
                            '3RD_CONV':HOME_3RD_CONV,
                            'FD_AGAINST':AWAY_FD,
                            'SACKS_AGAINST':AWAY_SACKED,
                            'SACK_YDS_AGAINST':AWAY_SACKED_YDS,
                            'TOTAL_YDS_AGAINST':AWAY_TOTAL_YDS,
                            'FUMBLES_AGAINST':AWAY_FUMBLES,
                            'FUMBLES_LOST_AGAINST':AWAY_FUMBLES_LOST,
                            'TO_AGAINST':AWAY_TO,
                            'PENALTIES_AGAINST':AWAY_PENALTIES,
                            'PENALTY_YARDS_AGAINST':AWAY_PENALTY_YARDS,
                            '3RD_DOWN_AGAINST':AWAY_3RD_DOWN,
                            '3RD_DOWN_ATT_AGAINST':AWAY_3RD_DOWN_ATT,
                            '3RD_CONV_AGAINST':AWAY_3RD_CONV
                        })

                        home_team_data = pd.DataFrame(home_team_data, columns=team_table_headers)
                        home_team_data.to_sql(f"teams_{year}", teams_con, if_exists="append")

                        #####################################################
                        #####   END COLLECTING DATA FOR TEAM TABLES     #####
                        #####################################################

                    #########################################################
                    #####   BEGIN COLLECTING DATA FOR PASSER TABLES     #####
                    #########################################################
                    if "passing_advanced" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="passing_advanced")
                        adv_passing_table = comment_soup.find('table', id="passing_advanced")
                        adv_passing_rows = table.findAll("tr")

                        df = pd.read_html(str(adv_passing_table))[0]

                        df = df[df['Player'] != 'Player']
                        df['WEEK'] = week
                        df['YEAR'] = year
                        df['BIG_GAME_W'] = 0 # use as a default value: -1 means this was not a 'big game'
                        df['BIG_GAME_L'] = 0
                        df['PLAYOFF_W'] = 0
                        df['PLAYOFF_L'] = 0
                        df['CHAMP_W'] = 0
                        df['CHAMP_L'] = 0

                        visiting_passer = df.loc[df['Tm'] == vis].iloc[0]
                        home_passer = df.loc[df['Tm'] == home].iloc[0]

                        SPREAD_ABS = abs(float(SPREAD))
                        if SPREAD_ABS <= 3.0:
                            if home_team_win > 0:
                                home_passer['BIG_GAME_W'] = 1.0
                                visiting_passer['BIG_GAME_L'] = 1.0
                            else:
                                visiting_passer['BIG_GAME_W'] = 1.0
                                home_passer['BIG_GAME_L'] = 1.0

                        if year < 2021:
                            # account for extra game
                            if week > 17:
                                if home_team_win > 0:
                                    home_passer['PLAYOFF_W'] = 1.0
                                    visiting_passer['PLAYOFF_L'] = 1.0
                                else:
                                    visiting_passer['PLAYOFF_W'] = 1.0
                                    home_passer['PLAYOFF_L'] = 1.0

                            if week == 21:
                                if home_team_win > 0:
                                    home_passer['CHAMP_W'] = 1.0
                                    visiting_passer['CHAMP_L'] = 1.0
                                else:
                                    visiting_passer['CHAMP_W'] = 1.0
                                    home_passer['CHAMP_L'] = 1.0

                        else:
                            if week > 18:
                                if home_team_win > 0:
                                    home_passer['PLAYOFF_W'] = 1.0
                                    visiting_passer['PLAYOFF_L'] = 1.0
                                else:
                                    visiting_passer['PLAYOFF_W'] = 1.0
                                    home_passer['PLAYOFF_L'] = 1.0

                            
                            if week == 22:
                                if home_team_win > 0:
                                    home_passer['CHAMP_W'] = 1.0
                                    visiting_passer['CHAMP_L'] = 1.0
                                else:
                                    visiting_passer['CHAMP_W'] = 1.0
                                    home_passer['CHAMP_L'] = 1.0


                        passers_frame = pd.DataFrame([visiting_passer.to_dict(), home_passer.to_dict()])
                        passers_frame.reset_index(drop=True, inplace=True)

                        passers_frame['1D%'] = passers_frame['1D%'].str.replace('%', '', regex=False).astype(float) / 100
                        passers_frame['Drop%'] = passers_frame['Drop%'].str.replace('%', '', regex=False).astype(float) / 100
                        passers_frame['Prss%'] = passers_frame['Prss%'].str.replace('%', '', regex=False).astype(float) / 100
                        passers_frame['Bad%'] = passers_frame['Bad%'].str.replace('%', '', regex=False).astype(float) / 100
                        
                        print(passers_frame)

                        # If season passing table is empty, we want to pass -1 into each variable for the game stats
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"passers_{year}\"", passers_con)
                        if table.empty:
                            AWAY_PASS_COMP_MEAN = -1 
                            AWAY_PASS_COMP_STD = -1
                            AWAY_PASS_COMP_MAX = -1
                            AWAY_PASS_COMP_MIN = -1
                            AWAY_PASS_ATT_MEAN = -1 
                            AWAY_PASS_ATT_STD = -1
                            AWAY_PASS_ATT_MAX = -1
                            AWAY_PASS_ATT_MIN = -1
                            AWAY_PASS_YDS_MEAN = -1 
                            AWAY_PASS_YDS_STD = -1
                            AWAY_PASS_YDS_MAX = -1
                            AWAY_PASS_YDS_MIN = -1
                            AWAY_PASS_1D_MEAN = -1 
                            AWAY_PASS_1D_STD = -1
                            AWAY_PASS_1D_MAX = -1
                            AWAY_PASS_1D_MIN = -1
                            AWAY_PASS_1DPCT_MEAN = -1 
                            AWAY_PASS_1DPCT_STD = -1
                            AWAY_PASS_1DPCT_MAX = -1
                            AWAY_PASS_1DPCT_MIN = -1
                            AWAY_PASS_IAY_MEAN = -1 
                            AWAY_PASS_IAY_STD = -1
                            AWAY_PASS_IAY_MAX = -1
                            AWAY_PASS_IAY_MIN = -1
                            AWAY_PASS_IAYPA_MEAN = -1 
                            AWAY_PASS_IAYPA_STD = -1
                            AWAY_PASS_IAYPA_MAX = -1
                            AWAY_PASS_IAYPA_MIN = -1
                            AWAY_PASS_CAY_MEAN = -1 
                            AWAY_PASS_CAY_STD = -1
                            AWAY_PASS_CAY_MAX = -1
                            AWAY_PASS_CAY_MIN = -1
                            AWAY_PASS_CAYCMP_MEAN = -1 
                            AWAY_PASS_CAYCMP_STD = -1
                            AWAY_PASS_CAYCMP_MAX = -1
                            AWAY_PASS_CAYCMP_MIN = -1
                            AWAY_PASS_CAYPA_MEAN = -1 
                            AWAY_PASS_CAYPA_STD = -1
                            AWAY_PASS_CAYPA_MAX = -1
                            AWAY_PASS_CAYPA_MIN = -1
                            AWAY_PASS_YAC_MEAN = -1 
                            AWAY_PASS_YAC_STD = -1
                            AWAY_PASS_YAC_MAX = -1
                            AWAY_PASS_YAC_MIN = -1
                            AWAY_PASS_YACCMP_MEAN = -1 
                            AWAY_PASS_YACCMP_STD = -1
                            AWAY_PASS_YACCMP_MAX = -1
                            AWAY_PASS_YACCMP_MIN = -1
                            AWAY_PASS_DROPS_MEAN = -1 
                            AWAY_PASS_DROPS_STD = -1
                            AWAY_PASS_DROPS_MAX = -1
                            AWAY_PASS_DROPS_MIN = -1
                            AWAY_PASS_DROPPCT_MEAN = -1 
                            AWAY_PASS_DROPPCT_STD = -1
                            AWAY_PASS_DROPPCT_MAX = -1
                            AWAY_PASS_DROPPCT_MIN = -1
                            AWAY_PASS_BADTH_MEAN = -1 
                            AWAY_PASS_BADTH_STD = -1
                            AWAY_PASS_BADTH_MAX = -1
                            AWAY_PASS_BADTH_MIN = -1
                            AWAY_PASS_SK_MEAN = -1 
                            AWAY_PASS_SK_STD = -1
                            AWAY_PASS_SK_MAX = -1
                            AWAY_PASS_SK_MIN = -1
                            AWAY_PASS_BLITZ_MEAN = -1 
                            AWAY_PASS_BLITZ_STD = -1
                            AWAY_PASS_BLITZ_MAX = -1
                            AWAY_PASS_BLITZ_MIN = -1
                            AWAY_PASS_HRRY_MEAN = -1 
                            AWAY_PASS_HRRY_STD = -1
                            AWAY_PASS_HRRY_MAX = -1
                            AWAY_PASS_HRRY_MIN = -1
                            AWAY_PASS_HITS_MEAN = -1 
                            AWAY_PASS_HITS_STD = -1
                            AWAY_PASS_HITS_MAX = -1
                            AWAY_PASS_HITS_MIN = -1
                            AWAY_PASS_PRSS_MEAN = -1 
                            AWAY_PASS_PRSS_STD = -1
                            AWAY_PASS_PRSS_MAX = -1
                            AWAY_PASS_PRSS_MIN = -1
                            AWAY_PASS_PRSSPCT_MEAN = -1 
                            AWAY_PASS_PRSSPCT_STD = -1
                            AWAY_PASS_PRSSPCT_MAX = -1
                            AWAY_PASS_PRSSPCT_MIN = -1
                            AWAY_PASS_SCRM_MEAN = -1 
                            AWAY_PASS_SCRM_STD = -1
                            AWAY_PASS_SCRM_MAX = -1
                            AWAY_PASS_SCRM_MIN = -1
                            AWAY_PASS_YDSSCRM_MEAN = -1 
                            AWAY_PASS_YDSSCRM_STD = -1
                            AWAY_PASS_YDSSCRM_MAX = -1
                            AWAY_PASS_YDSSCRM_MIN = -1
                            HOME_PASS_ATT_MEAN = -1 
                            HOME_PASS_ATT_STD = -1
                            HOME_PASS_ATT_MAX = -1
                            HOME_PASS_ATT_MIN = -1
                            HOME_PASS_YDS_MEAN = -1 
                            HOME_PASS_YDS_STD = -1
                            HOME_PASS_YDS_MAX = -1
                            HOME_PASS_YDS_MIN = -1
                            HOME_PASS_1D_MEAN = -1 
                            HOME_PASS_1D_STD = -1
                            HOME_PASS_1D_MAX = -1
                            HOME_PASS_1D_MIN = -1
                            HOME_PASS_1DPCT_MEAN = -1 
                            HOME_PASS_1DPCT_STD = -1
                            HOME_PASS_1DPCT_MAX = -1
                            HOME_PASS_1DPCT_MIN = -1
                            HOME_PASS_IAY_MEAN = -1 
                            HOME_PASS_IAY_STD = -1
                            HOME_PASS_IAY_MAX = -1
                            HOME_PASS_IAY_MIN = -1
                            HOME_PASS_IAYPA_MEAN = -1 
                            HOME_PASS_IAYPA_STD = -1
                            HOME_PASS_IAYPA_MAX = -1
                            HOME_PASS_IAYPA_MIN = -1
                            HOME_PASS_CAY_MEAN = -1 
                            HOME_PASS_CAY_STD = -1
                            HOME_PASS_CAY_MAX = -1
                            HOME_PASS_CAY_MIN = -1
                            HOME_PASS_CAYCMP_MEAN = -1 
                            HOME_PASS_CAYCMP_STD = -1
                            HOME_PASS_CAYCMP_MAX = -1
                            HOME_PASS_CAYCMP_MIN = -1
                            HOME_PASS_CAYPA_MEAN = -1 
                            HOME_PASS_CAYPA_STD = -1
                            HOME_PASS_CAYPA_MAX = -1
                            HOME_PASS_CAYPA_MIN = -1
                            HOME_PASS_YAC_MEAN = -1 
                            HOME_PASS_YAC_STD = -1
                            HOME_PASS_YAC_MAX = -1
                            HOME_PASS_YAC_MIN = -1
                            HOME_PASS_YACCMP_MEAN = -1 
                            HOME_PASS_YACCMP_STD = -1
                            HOME_PASS_YACCMP_MAX = -1
                            HOME_PASS_YACCMP_MIN = -1
                            HOME_PASS_DROPS_MEAN = -1 
                            HOME_PASS_DROPS_STD = -1
                            HOME_PASS_DROPS_MAX = -1
                            HOME_PASS_DROPS_MIN = -1
                            HOME_PASS_DROPPCT_MEAN = -1 
                            HOME_PASS_DROPPCT_STD = -1
                            HOME_PASS_DROPPCT_MAX = -1
                            HOME_PASS_DROPPCT_MIN = -1
                            HOME_PASS_BADTH_MEAN = -1 
                            HOME_PASS_BADTH_STD = -1
                            HOME_PASS_BADTH_MAX = -1
                            HOME_PASS_BADTH_MIN = -1
                            HOME_PASS_SK_MEAN = -1 
                            HOME_PASS_SK_STD = -1
                            HOME_PASS_SK_MAX = -1
                            HOME_PASS_SK_MIN = -1
                            HOME_PASS_BLITZ_MEAN = -1 
                            HOME_PASS_BLITZ_STD = -1
                            HOME_PASS_BLITZ_MAX = -1
                            HOME_PASS_BLITZ_MIN = -1
                            HOME_PASS_HRRY_MEAN = -1 
                            HOME_PASS_HRRY_STD = -1
                            HOME_PASS_HRRY_MAX = -1
                            HOME_PASS_HRRY_MIN = -1
                            HOME_PASS_HITS_MEAN = -1 
                            HOME_PASS_HITS_STD = -1
                            HOME_PASS_HITS_MAX = -1
                            HOME_PASS_HITS_MIN = -1
                            HOME_PASS_PRSS_MEAN = -1 
                            HOME_PASS_PRSS_STD = -1
                            HOME_PASS_PRSS_MAX = -1
                            HOME_PASS_PRSS_MIN = -1
                            HOME_PASS_PRSSPCT_MEAN = -1 
                            HOME_PASS_PRSSPCT_STD = -1
                            HOME_PASS_PRSSPCT_MAX = -1
                            HOME_PASS_PRSSPCT_MIN = -1
                            HOME_PASS_SCRM_MEAN = -1 
                            HOME_PASS_SCRM_STD = -1
                            HOME_PASS_SCRM_MAX = -1
                            HOME_PASS_SCRM_MIN = -1
                            HOME_PASS_YDSSCRM_MEAN = -1 
                            HOME_PASS_YDSSCRM_STD = -1
                            HOME_PASS_YDSSCRM_MAX = -1
                            HOME_PASS_YDSSCRM_MIN = -1
                            # AWAY_PASS_BIG_GAME_W = -1
                            # AWAY_PASS_BIG_GAME_L = -1
                            # AWAY_PASS_PLAYOFF_W = -1
                            # AWAY_PASS_PLAYOFF_L = -1
                            # AWAY_PASS_CHAMP_W = -1
                            # AWAY_PASS_CHAMP_L = -1
                        else:
                            # if the table is not empty, take the mean, std, max and min from each of the RELEVANT VALues
                            away_passer_df = pd.read_sql_query(f"SELECT * FROM passers_{year} WHERE (WEEK < {week}) and Player = {visiting_passer['Player']} order by WEEK desc;", passers_con)
                            home_passer_df = pd.read_sql_query(f"SELECT * FROM passers_{year} WHERE (WEEK < {week}) and Player = {home_passer['Player']} order by WEEK desc;", passers_con)

                            AWAY_PASS_COMP_MEAN = away_passer_df['Cmp'].mean()
                            AWAY_PASS_COMP_STD = away_passer_df['Cmp'].std()
                            AWAY_PASS_COMP_MAX = away_passer_df['Cmp'].max()
                            AWAY_PASS_COMP_MIN = away_passer_df['Cmp'].min()
                            AWAY_PASS_ATT_MEAN = away_passer_df['Att'].mean() 
                            AWAY_PASS_ATT_STD = away_passer_df['Att'].std()
                            AWAY_PASS_ATT_MAX = away_passer_df['Att'].max()
                            AWAY_PASS_ATT_MIN = away_passer_df['Att'].min()
                            AWAY_PASS_YDS_MEAN = away_passer_df['Yds'].mean() 
                            AWAY_PASS_YDS_STD = away_passer_df['Yds'].std()
                            AWAY_PASS_YDS_MAX = away_passer_df['Yds'].max()
                            AWAY_PASS_YDS_MIN = away_passer_df['Yds'].min()
                            AWAY_PASS_1D_MEAN = away_passer_df['1D'].mean() 
                            AWAY_PASS_1D_STD = away_passer_df['1D'].std()
                            AWAY_PASS_1D_MAX = away_passer_df['1D'].max()
                            AWAY_PASS_1D_MIN = away_passer_df['1D'].min()
                            AWAY_PASS_1DPCT_MEAN = away_passer_df['1D%'].mean() 
                            AWAY_PASS_1DPCT_STD = away_passer_df['1D%'].std()
                            AWAY_PASS_1DPCT_MAX = away_passer_df['1D%'].max()
                            AWAY_PASS_1DPCT_MIN = away_passer_df['1D%'].min()
                            AWAY_PASS_IAY_MEAN = away_passer_df['IAY'].mean() 
                            AWAY_PASS_IAY_STD = away_passer_df['IAY'].std()
                            AWAY_PASS_IAY_MAX = away_passer_df['IAY'].max()
                            AWAY_PASS_IAY_MIN = away_passer_df['IAY'].min()
                            AWAY_PASS_IAYPA_MEAN = away_passer_df['IAY/PA'].mean() 
                            AWAY_PASS_IAYPA_STD = away_passer_df['IAY/PA'].std()
                            AWAY_PASS_IAYPA_MAX = away_passer_df['IAY/PA'].max()
                            AWAY_PASS_IAYPA_MIN = away_passer_df['IAY/PA'].min()
                            AWAY_PASS_CAY_MEAN = away_passer_df['CAY'].mean() 
                            AWAY_PASS_CAY_STD = away_passer_df['CAY'].std()
                            AWAY_PASS_CAY_MAX = away_passer_df['CAY'].max()
                            AWAY_PASS_CAY_MIN = away_passer_df['CAY'].min()
                            AWAY_PASS_CAYCMP_MEAN = away_passer_df['CAY/Cmp'].mean() 
                            AWAY_PASS_CAYCMP_STD = away_passer_df['CAY/Cmp'].std()
                            AWAY_PASS_CAYCMP_MAX = away_passer_df['CAY/Cmp'].max()
                            AWAY_PASS_CAYCMP_MIN = away_passer_df['CAY/Cmp'].min()
                            AWAY_PASS_CAYPA_MEAN = away_passer_df['CAY/PA'].mean() 
                            AWAY_PASS_CAYPA_STD = away_passer_df['CAY/PA'].std()
                            AWAY_PASS_CAYPA_MAX = away_passer_df['CAY/PA'].max()
                            AWAY_PASS_CAYPA_MIN = away_passer_df['CAY/PA'].min()
                            AWAY_PASS_YAC_MEAN = away_passer_df['YAC'].mean() 
                            AWAY_PASS_YAC_STD = away_passer_df['YAC'].std()
                            AWAY_PASS_YAC_MAX = away_passer_df['YAC'].max()
                            AWAY_PASS_YAC_MIN = away_passer_df['YAC'].min()
                            AWAY_PASS_YACCMP_MEAN = away_passer_df['YAC/Cmp'].mean() 
                            AWAY_PASS_YACCMP_STD = away_passer_df['YAC/Cmp'].std()
                            AWAY_PASS_YACCMP_MAX = away_passer_df['YAC/Cmp'].max()
                            AWAY_PASS_YACCMP_MIN = away_passer_df['YAC/Cmp'].min()
                            AWAY_PASS_DROPS_MEAN = away_passer_df['Drops'].mean() 
                            AWAY_PASS_DROPS_STD = away_passer_df['Drops'].std()
                            AWAY_PASS_DROPS_MAX = away_passer_df['Drops'].max()
                            AWAY_PASS_DROPS_MIN = away_passer_df['Drops'].min()
                            AWAY_PASS_DROPPCT_MEAN = away_passer_df['Drop%'].mean() 
                            AWAY_PASS_DROPPCT_STD = away_passer_df['Drop%'].std()
                            AWAY_PASS_DROPPCT_MAX = away_passer_df['Drop%'].max()
                            AWAY_PASS_DROPPCT_MIN = away_passer_df['Drop%'].min()
                            AWAY_PASS_BADTH_MEAN = away_passer_df['BadTh'].mean() 
                            AWAY_PASS_BADTH_STD = away_passer_df['BadTh'].std()
                            AWAY_PASS_BADTH_MAX = away_passer_df['BadTh'].max()
                            AWAY_PASS_BADTH_MIN = away_passer_df['BadTh'].min()
                            AWAY_PASS_SK_MEAN = away_passer_df['Sk'].mean() 
                            AWAY_PASS_SK_STD = away_passer_df['Sk'].std()
                            AWAY_PASS_SK_MAX = away_passer_df['Sk'].max()
                            AWAY_PASS_SK_MIN = away_passer_df['Sk'].min()
                            AWAY_PASS_BLITZ_MEAN = away_passer_df['Bltz'].mean() 
                            AWAY_PASS_BLITZ_STD = away_passer_df['Bltz'].std()
                            AWAY_PASS_BLITZ_MAX = away_passer_df['Bltz'].max()
                            AWAY_PASS_BLITZ_MIN = away_passer_df['Bltz'].min()
                            AWAY_PASS_HRRY_MEAN = away_passer_df['Hrry'].mean() 
                            AWAY_PASS_HRRY_STD = away_passer_df['Hrry'].std()
                            AWAY_PASS_HRRY_MAX = away_passer_df['Hrry'].max()
                            AWAY_PASS_HRRY_MIN = away_passer_df['Hrry'].min()
                            AWAY_PASS_HITS_MEAN = away_passer_df['Hits'].mean() 
                            AWAY_PASS_HITS_STD = away_passer_df['Hits'].std()
                            AWAY_PASS_HITS_MAX = away_passer_df['Hits'].max()
                            AWAY_PASS_HITS_MIN = away_passer_df['Hits'].min()
                            AWAY_PASS_PRSS_MEAN = away_passer_df['Prss'].mean() 
                            AWAY_PASS_PRSS_STD = away_passer_df['Prss'].std()
                            AWAY_PASS_PRSS_MAX = away_passer_df['Prss'].max()
                            AWAY_PASS_PRSS_MIN = away_passer_df['Prss'].min()
                            AWAY_PASS_PRSSPCT_MEAN = away_passer_df['Prss%'].mean() 
                            AWAY_PASS_PRSSPCT_STD = away_passer_df['Prss%'].std()
                            AWAY_PASS_PRSSPCT_MAX = away_passer_df['Prss%'].max()
                            AWAY_PASS_PRSSPCT_MIN = away_passer_df['Prss%'].min()
                            AWAY_PASS_SCRM_MEAN = away_passer_df['Scrm'].mean() 
                            AWAY_PASS_SCRM_STD = away_passer_df['Scrm'].std()
                            AWAY_PASS_SCRM_MAX = away_passer_df['Scrm'].max()
                            AWAY_PASS_SCRM_MIN = away_passer_df['Scrm'].min()
                            AWAY_PASS_YDSSCRM_MEAN = away_passer_df['Yds/Scrm'].mean() 
                            AWAY_PASS_YDSSCRM_STD = away_passer_df['Yds/Scrm'].std()
                            AWAY_PASS_YDSSCRM_MAX = away_passer_df['Yds/Scrm'].max()
                            AWAY_PASS_YDSSCRM_MIN = away_passer_df['Yds/Scrm'].min()
                            HOME_PASS_COMP_MEAN = home_passer_df['Cmp'].mean()
                            HOME_PASS_COMP_STD = home_passer_df['Cmp'].std()
                            HOME_PASS_COMP_MAX = home_passer_df['Cmp'].min()
                            HOME_PASS_COMP_MIN = home_passer_df['Cmp'].max()
                            HOME_PASS_ATT_MEAN = home_passer_df['Att'].mean() 
                            HOME_PASS_ATT_STD = home_passer_df['Att'].std()
                            HOME_PASS_ATT_MAX = home_passer_df['Att'].max()
                            HOME_PASS_ATT_MIN = home_passer_df['Att'].min()
                            HOME_PASS_YDS_MEAN = home_passer_df['Yds'].mean() 
                            HOME_PASS_YDS_STD = home_passer_df['Yds'].std()
                            HOME_PASS_YDS_MAX = home_passer_df['Yds'].max()
                            HOME_PASS_YDS_MIN = home_passer_df['Yds'].min()
                            HOME_PASS_1D_MEAN = home_passer_df['1D'].mean() 
                            HOME_PASS_1D_STD = home_passer_df['1D'].std()
                            HOME_PASS_1D_MAX = home_passer_df['1D'].max()
                            HOME_PASS_1D_MIN = home_passer_df['1D'].min()
                            HOME_PASS_1DPCT_MEAN = home_passer_df['1D%'].mean() 
                            HOME_PASS_1DPCT_STD = home_passer_df['1D%'].std()
                            HOME_PASS_1DPCT_MAX = home_passer_df['1D%'].max()
                            HOME_PASS_1DPCT_MIN = home_passer_df['1D%'].min()
                            HOME_PASS_IAY_MEAN = home_passer_df['IAY'].mean() 
                            HOME_PASS_IAY_STD = home_passer_df['IAY'].std()
                            HOME_PASS_IAY_MAX = home_passer_df['IAY'].max()
                            HOME_PASS_IAY_MIN = home_passer_df['IAY'].min()
                            HOME_PASS_IAYPA_MEAN = home_passer_df['IAY/PA'].mean() 
                            HOME_PASS_IAYPA_STD = home_passer_df['IAY/PA'].std()
                            HOME_PASS_IAYPA_MAX = home_passer_df['IAY/PA'].max()
                            HOME_PASS_IAYPA_MIN = home_passer_df['IAY/PA'].min()
                            HOME_PASS_CAY_MEAN = home_passer_df['CAY'].mean() 
                            HOME_PASS_CAY_STD = home_passer_df['CAY'].std()
                            HOME_PASS_CAY_MAX = home_passer_df['CAY'].max()
                            HOME_PASS_CAY_MIN = home_passer_df['CAY'].min()
                            HOME_PASS_CAYCMP_MEAN = home_passer_df['CAY/Cmp'].mean() 
                            HOME_PASS_CAYCMP_STD = home_passer_df['CAY/Cmp'].std()
                            HOME_PASS_CAYCMP_MAX = home_passer_df['CAY/Cmp'].max()
                            HOME_PASS_CAYCMP_MIN = home_passer_df['CAY/Cmp'].min()
                            HOME_PASS_CAYPA_MEAN = home_passer_df['CAY/PA'].mean() 
                            HOME_PASS_CAYPA_STD = home_passer_df['CAY/PA'].std()
                            HOME_PASS_CAYPA_MAX = home_passer_df['CAY/PA'].max()
                            HOME_PASS_CAYPA_MIN = home_passer_df['CAY/PA'].min()
                            HOME_PASS_YAC_MEAN = home_passer_df['YAC'].mean() 
                            HOME_PASS_YAC_STD = home_passer_df['YAC'].std()
                            HOME_PASS_YAC_MAX = home_passer_df['YAC'].max()
                            HOME_PASS_YAC_MIN = home_passer_df['YAC'].min()
                            HOME_PASS_YACCMP_MEAN = home_passer_df['YAC/Cmp'].mean() 
                            HOME_PASS_YACCMP_STD = home_passer_df['YAC/Cmp'].std()
                            HOME_PASS_YACCMP_MAX = home_passer_df['YAC/Cmp'].max()
                            HOME_PASS_YACCMP_MIN = home_passer_df['YAC/Cmp'].min()
                            HOME_PASS_DROPS_MEAN = home_passer_df['Drops'].mean() 
                            HOME_PASS_DROPS_STD = home_passer_df['Drops'].std()
                            HOME_PASS_DROPS_MAX = home_passer_df['Drops'].max()
                            HOME_PASS_DROPS_MIN = home_passer_df['Drops'].min()
                            HOME_PASS_DROPPCT_MEAN = home_passer_df['Drop%'].mean() 
                            HOME_PASS_DROPPCT_STD = home_passer_df['Drop%'].std()
                            HOME_PASS_DROPPCT_MAX = home_passer_df['Drop%'].max()
                            HOME_PASS_DROPPCT_MIN = home_passer_df['Drop%'].min()
                            HOME_PASS_BADTH_MEAN = home_passer_df['BadTh'].mean() 
                            HOME_PASS_BADTH_STD = home_passer_df['BadTh'].std()
                            HOME_PASS_BADTH_MAX = home_passer_df['BadTh'].max()
                            HOME_PASS_BADTH_MIN = home_passer_df['BadTh'].min()
                            HOME_PASS_SK_MEAN = home_passer_df['Sk'].mean() 
                            HOME_PASS_SK_STD = home_passer_df['Sk'].std()
                            HOME_PASS_SK_MAX = home_passer_df['Sk'].max()
                            HOME_PASS_SK_MIN = home_passer_df['Sk'].min()
                            HOME_PASS_BLITZ_MEAN = home_passer_df['Bltz'].mean() 
                            HOME_PASS_BLITZ_STD = home_passer_df['Bltz'].std()
                            HOME_PASS_BLITZ_MAX = home_passer_df['Bltz'].max()
                            HOME_PASS_BLITZ_MIN = home_passer_df['Bltz'].min()
                            HOME_PASS_HRRY_MEAN = home_passer_df['Hrry'].mean() 
                            HOME_PASS_HRRY_STD = home_passer_df['Hrry'].std()
                            HOME_PASS_HRRY_MAX = home_passer_df['Hrry'].max()
                            HOME_PASS_HRRY_MIN = home_passer_df['Hrry'].min()
                            HOME_PASS_HITS_MEAN = home_passer_df['Hits'].mean() 
                            HOME_PASS_HITS_STD = home_passer_df['Hits'].std()
                            HOME_PASS_HITS_MAX = home_passer_df['Hits'].max()
                            HOME_PASS_HITS_MIN = home_passer_df['Hits'].min()
                            HOME_PASS_PRSS_MEAN = home_passer_df['Prss'].mean() 
                            HOME_PASS_PRSS_STD = home_passer_df['Prss'].std()
                            HOME_PASS_PRSS_MAX = home_passer_df['Prss'].max()
                            HOME_PASS_PRSS_MIN = home_passer_df['Prss'].min()
                            HOME_PASS_PRSSPCT_MEAN = home_passer_df['Prss%'].mean() 
                            HOME_PASS_PRSSPCT_STD = home_passer_df['Prss%'].std()
                            HOME_PASS_PRSSPCT_MAX = home_passer_df['Prss%'].max()
                            HOME_PASS_PRSSPCT_MIN = home_passer_df['Prss%'].min()
                            HOME_PASS_SCRM_MEAN = home_passer_df['Scrm'].mean() 
                            HOME_PASS_SCRM_STD = home_passer_df['Scrm'].std()
                            HOME_PASS_SCRM_MAX = home_passer_df['Scrm'].max()
                            HOME_PASS_SCRM_MIN = home_passer_df['Scrm'].min()
                            HOME_PASS_YDSSCRM_MEAN = home_passer_df['Yds/Scrm'].mean() 
                            HOME_PASS_YDSSCRM_STD = home_passer_df['Yds/Scrm'].std()
                            HOME_PASS_YDSSCRM_MAX = home_passer_df['Yds/Scrm'].max()
                            HOME_PASS_YDSSCRM_MIN = home_passer_df['Yds/Scrm'].min()

                        # If CAREER passing table is empty, we want to pass passer's career stats from 2018 to present day
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='passers_2018-present'", passers_con)
                        if table.empty:
                            # DROP SCRIMMAGE DATA AND DROP DATA FOR CAREER STATS
                            AWAY_PASS_COMP_CAREER_MEAN = -1 
                            AWAY_PASS_COMP_CAREER_STD = -1
                            AWAY_PASS_COMP_CAREER_MAX = -1
                            AWAY_PASS_COMP_CAREER_MIN = -1
                            AWAY_PASS_ATT_CAREER_MEAN = -1 
                            AWAY_PASS_ATT_CAREER_STD = -1
                            AWAY_PASS_ATT_CAREER_MAX = -1
                            AWAY_PASS_ATT_CAREER_MIN = -1
                            AWAY_PASS_YDS_CAREER_MEAN = -1 
                            AWAY_PASS_YDS_CAREER_STD = -1
                            AWAY_PASS_YDS_CAREER_MAX = -1
                            AWAY_PASS_YDS_CAREER_MIN = -1
                            AWAY_PASS_1D_CAREER_MEAN = -1 
                            AWAY_PASS_1D_CAREER_STD = -1
                            AWAY_PASS_1D_CAREER_MAX = -1
                            AWAY_PASS_1D_CAREER_MIN = -1
                            AWAY_PASS_1DPCT_CAREER_MEAN = -1 
                            AWAY_PASS_1DPCT_CAREER_STD = -1
                            AWAY_PASS_1DPCT_CAREER_MAX = -1
                            AWAY_PASS_1DPCT_CAREER_MIN = -1
                            AWAY_PASS_IAY_CAREER_MEAN = -1 
                            AWAY_PASS_IAY_CAREER_STD = -1
                            AWAY_PASS_IAY_CAREER_MAX = -1
                            AWAY_PASS_IAY_CAREER_MIN = -1
                            AWAY_PASS_IAYPA_CAREER_MEAN = -1 
                            AWAY_PASS_IAYPA_CAREER_STD = -1
                            AWAY_PASS_IAYPA_CAREER_MAX = -1
                            AWAY_PASS_IAYPA_CAREER_MIN = -1
                            AWAY_PASS_CAY_CAREER_MEAN = -1 
                            AWAY_PASS_CAY_CAREER_STD = -1
                            AWAY_PASS_CAY_CAREER_MAX = -1
                            AWAY_PASS_CAY_CAREER_MIN = -1
                            AWAY_PASS_CAYCMP_CAREER_MEAN = -1 
                            AWAY_PASS_CAYCMP_CAREER_STD = -1
                            AWAY_PASS_CAYCMP_CAREER_MAX = -1
                            AWAY_PASS_CAYCMP_CAREER_MIN = -1
                            AWAY_PASS_CAYPA_CAREER_MEAN = -1 
                            AWAY_PASS_CAYPA_CAREER_STD = -1
                            AWAY_PASS_CAYPA_CAREER_MAX = -1
                            AWAY_PASS_CAYPA_CAREER_MIN = -1
                            AWAY_PASS_YAC_CAREER_MEAN = -1 
                            AWAY_PASS_YAC_CAREER_STD = -1
                            AWAY_PASS_YAC_CAREER_MAX = -1
                            AWAY_PASS_YAC_CAREER_MIN = -1
                            AWAY_PASS_YACCMP_CAREER_MEAN = -1 
                            AWAY_PASS_YACCMP_CAREER_STD = -1
                            AWAY_PASS_YACCMP_CAREER_MAX = -1
                            AWAY_PASS_YACCMP_CAREER_MIN = -1
                            AWAY_PASS_BADTH_CAREER_MEAN = -1 
                            AWAY_PASS_BADTH_CAREER_STD = -1
                            AWAY_PASS_BADTH_CAREER_MAX = -1
                            AWAY_PASS_BADTH_CAREER_MIN = -1
                            AWAY_PASS_SK_CAREER_MEAN = -1 
                            AWAY_PASS_SK_CAREER_STD = -1
                            AWAY_PASS_SK_CAREER_MAX = -1
                            AWAY_PASS_SK_CAREER_MIN = -1
                            AWAY_PASS_BLITZ_CAREER_MEAN = -1 
                            AWAY_PASS_BLITZ_CAREER_STD = -1
                            AWAY_PASS_BLITZ_CAREER_MAX = -1
                            AWAY_PASS_BLITZ_CAREER_MIN = -1
                            AWAY_PASS_HRRY_CAREER_MEAN = -1 
                            AWAY_PASS_HRRY_CAREER_STD = -1
                            AWAY_PASS_HRRY_CAREER_MAX = -1
                            AWAY_PASS_HRRY_CAREER_MIN = -1
                            AWAY_PASS_HITS_CAREER_MEAN = -1 
                            AWAY_PASS_HITS_CAREER_STD = -1
                            AWAY_PASS_HITS_CAREER_MAX = -1
                            AWAY_PASS_HITS_CAREER_MIN = -1
                            AWAY_PASS_PRSS_CAREER_MEAN = -1 
                            AWAY_PASS_PRSS_CAREER_STD = -1
                            AWAY_PASS_PRSS_CAREER_MAX = -1
                            AWAY_PASS_PRSS_CAREER_MIN = -1
                            AWAY_PASS_PRSSPCT_CAREER_MEAN = -1 
                            AWAY_PASS_PRSSPCT_CAREER_STD = -1
                            AWAY_PASS_PRSSPCT_CAREER_MAX = -1
                            AWAY_PASS_PRSSPCT_CAREER_MIN = -1
                            HOME_PASS_ATT_CAREER_MEAN = -1 
                            HOME_PASS_ATT_CAREER_STD = -1
                            HOME_PASS_ATT_CAREER_MAX = -1
                            HOME_PASS_ATT_CAREER_MIN = -1
                            HOME_PASS_YDS_CAREER_MEAN = -1 
                            HOME_PASS_YDS_CAREER_STD = -1
                            HOME_PASS_YDS_CAREER_MAX = -1
                            HOME_PASS_YDS_CAREER_MIN = -1
                            HOME_PASS_1D_CAREER_MEAN = -1 
                            HOME_PASS_1D_CAREER_STD = -1
                            HOME_PASS_1D_CAREER_MAX = -1
                            HOME_PASS_1D_CAREER_MIN = -1
                            HOME_PASS_1DPCT_CAREER_MEAN = -1 
                            HOME_PASS_1DPCT_CAREER_STD = -1
                            HOME_PASS_1DPCT_CAREER_MAX = -1
                            HOME_PASS_1DPCT_CAREER_MIN = -1
                            HOME_PASS_IAY_CAREER_MEAN = -1 
                            HOME_PASS_IAY_CAREER_STD = -1
                            HOME_PASS_IAY_CAREER_MAX = -1
                            HOME_PASS_IAY_CAREER_MIN = -1
                            HOME_PASS_IAYPA_CAREER_MEAN = -1 
                            HOME_PASS_IAYPA_CAREER_STD = -1
                            HOME_PASS_IAYPA_CAREER_MAX = -1
                            HOME_PASS_IAYPA_CAREER_MIN = -1
                            HOME_PASS_CAY_CAREER_MEAN = -1 
                            HOME_PASS_CAY_CAREER_STD = -1
                            HOME_PASS_CAY_CAREER_MAX = -1
                            HOME_PASS_CAY_CAREER_MIN = -1
                            HOME_PASS_CAYCMP_CAREER_MEAN = -1 
                            HOME_PASS_CAYCMP_CAREER_STD = -1
                            HOME_PASS_CAYCMP_CAREER_MAX = -1
                            HOME_PASS_CAYCMP_CAREER_MIN = -1
                            HOME_PASS_CAYPA_CAREER_MEAN = -1 
                            HOME_PASS_CAYPA_CAREER_STD = -1
                            HOME_PASS_CAYPA_CAREER_MAX = -1
                            HOME_PASS_CAYPA_CAREER_MIN = -1
                            HOME_PASS_YAC_CAREER_MEAN = -1 
                            HOME_PASS_YAC_CAREER_STD = -1
                            HOME_PASS_YAC_CAREER_MAX = -1
                            HOME_PASS_YAC_CAREER_MIN = -1
                            HOME_PASS_YACCMP_CAREER_MEAN = -1 
                            HOME_PASS_YACCMP_CAREER_STD = -1
                            HOME_PASS_YACCMP_CAREER_MAX = -1
                            HOME_PASS_YACCMP_CAREER_MIN = -1
                            HOME_PASS_BADTH_CAREER_MEAN = -1 
                            HOME_PASS_BADTH_CAREER_STD = -1
                            HOME_PASS_BADTH_CAREER_MAX = -1
                            HOME_PASS_BADTH_CAREER_MIN = -1
                            HOME_PASS_SK_CAREER_MEAN = -1 
                            HOME_PASS_SK_CAREER_STD = -1
                            HOME_PASS_SK_CAREER_MAX = -1
                            HOME_PASS_SK_CAREER_MIN = -1
                            HOME_PASS_BLITZ_CAREER_MEAN = -1 
                            HOME_PASS_BLITZ_CAREER_STD = -1
                            HOME_PASS_BLITZ_CAREER_MAX = -1
                            HOME_PASS_BLITZ_CAREER_MIN = -1
                            HOME_PASS_HRRY_CAREER_MEAN = -1 
                            HOME_PASS_HRRY_CAREER_STD = -1
                            HOME_PASS_HRRY_CAREER_MAX = -1
                            HOME_PASS_HRRY_CAREER_MIN = -1
                            HOME_PASS_HITS_CAREER_MEAN = -1 
                            HOME_PASS_HITS_CAREER_STD = -1
                            HOME_PASS_HITS_CAREER_MAX = -1
                            HOME_PASS_HITS_CAREER_MIN = -1
                            HOME_PASS_PRSS_CAREER_MEAN = -1 
                            HOME_PASS_PRSS_CAREER_STD = -1
                            HOME_PASS_PRSS_CAREER_MAX = -1
                            HOME_PASS_PRSS_CAREER_MIN = -1
                            HOME_PASS_PRSSPCT_CAREER_MEAN = -1 
                            HOME_PASS_PRSSPCT_CAREER_STD = -1
                            HOME_PASS_PRSSPCT_CAREER_MAX = -1
                            HOME_PASS_PRSSPCT_CAREER_MIN = -1

                            AWAY_PASS_BIG_GAME_W = -1
                            AWAY_PASS_BIG_GAME_L = -1
                            AWAY_PASS_PLAYOFF_W = -1
                            AWAY_PASS_PLAYOFF_L = -1
                            AWAY_PASS_CHAMP_W = -1
                            AWAY_PASS_CHAMP_L = -1
                            HOME_PASS_BIG_GAME_W = -1
                            HOME_PASS_BIG_GAME_L = -1
                            HOME_PASS_PLAYOFF_W = -1
                            HOME_PASS_PLAYOFF_L = -1
                            HOME_PASS_CHAMP_W = -1
                            HOME_PASS_CHAMP_L = -1
                        else:
                            # if the table is not empty, take the mean, std, max and min from each of the RELEVANT VALues
                            away_passer_career_df = pd.read_sql_query(f"SELECT * FROM passers_2018-present WHERE and Player = {visiting_passer['Player']};", passers_con)
                            home_passer_career_df = pd.read_sql_query(f"SELECT * FROM passers_2018-present WHERE and Player = {home_passer['Player']};", passers_con)

                            AWAY_PASS_COMP_CAREER_MEAN = away_passer_career_df['Cmp'].mean()
                            AWAY_PASS_COMP_CAREER_STD = away_passer_career_df['Cmp'].std()
                            AWAY_PASS_COMP_CAREER_MAX = away_passer_career_df['Cmp'].max()
                            AWAY_PASS_COMP_CAREER_MIN = away_passer_career_df['Cmp'].min()
                            AWAY_PASS_ATT_CAREER_MEAN = away_passer_career_df['Att'].mean() 
                            AWAY_PASS_ATT_CAREER_STD = away_passer_career_df['Att'].std()
                            AWAY_PASS_ATT_CAREER_MAX = away_passer_career_df['Att'].max()
                            AWAY_PASS_ATT_CAREER_MIN = away_passer_career_df['Att'].min()
                            AWAY_PASS_YDS_CAREER_MEAN = away_passer_career_df['Yds'].mean() 
                            AWAY_PASS_YDS_CAREER_STD = away_passer_career_df['Yds'].std()
                            AWAY_PASS_YDS_CAREER_MAX = away_passer_career_df['Yds'].max()
                            AWAY_PASS_YDS_CAREER_MIN = away_passer_career_df['Yds'].min()
                            AWAY_PASS_1D_CAREER_MEAN = away_passer_career_df['1D'].mean() 
                            AWAY_PASS_1D_CAREER_STD = away_passer_career_df['1D'].std()
                            AWAY_PASS_1D_CAREER_MAX = away_passer_career_df['1D'].max()
                            AWAY_PASS_1D_CAREER_MIN = away_passer_career_df['1D'].min()
                            AWAY_PASS_1DPCT_CAREER_MEAN = away_passer_career_df['1D%'].mean() 
                            AWAY_PASS_1DPCT_CAREER_STD = away_passer_career_df['1D%'].std()
                            AWAY_PASS_1DPCT_CAREER_MAX = away_passer_career_df['1D%'].max()
                            AWAY_PASS_1DPCT_CAREER_MIN = away_passer_career_df['1D%'].min()
                            AWAY_PASS_IAY_CAREER_MEAN = away_passer_career_df['IAY'].mean() 
                            AWAY_PASS_IAY_CAREER_STD = away_passer_career_df['IAY'].std()
                            AWAY_PASS_IAY_CAREER_MAX = away_passer_career_df['IAY'].max()
                            AWAY_PASS_IAY_CAREER_MIN = away_passer_career_df['IAY'].min()
                            AWAY_PASS_IAYPA_CAREER_MEAN = away_passer_career_df['IAY/PA'].mean() 
                            AWAY_PASS_IAYPA_CAREER_STD = away_passer_career_df['IAY/PA'].std()
                            AWAY_PASS_IAYPA_CAREER_MAX = away_passer_career_df['IAY/PA'].max()
                            AWAY_PASS_IAYPA_CAREER_MIN = away_passer_career_df['IAY/PA'].min()
                            AWAY_PASS_CAY_CAREER_MEAN = away_passer_career_df['CAY'].mean() 
                            AWAY_PASS_CAY_CAREER_STD = away_passer_career_df['CAY'].std()
                            AWAY_PASS_CAY_CAREER_MAX = away_passer_career_df['CAY'].max()
                            AWAY_PASS_CAY_CAREER_MIN = away_passer_career_df['CAY'].min()
                            AWAY_PASS_CAYCMP_CAREER_MEAN = away_passer_career_df['CAY/Cmp'].mean() 
                            AWAY_PASS_CAYCMP_CAREER_STD = away_passer_career_df['CAY/Cmp'].std()
                            AWAY_PASS_CAYCMP_CAREER_MAX = away_passer_career_df['CAY/Cmp'].max()
                            AWAY_PASS_CAYCMP_CAREER_MIN = away_passer_career_df['CAY/Cmp'].min()
                            AWAY_PASS_CAYPA_CAREER_MEAN = away_passer_career_df['CAY/PA'].mean() 
                            AWAY_PASS_CAYPA_CAREER_STD = away_passer_career_df['CAY/PA'].std()
                            AWAY_PASS_CAYPA_CAREER_MAX = away_passer_career_df['CAY/PA'].max()
                            AWAY_PASS_CAYPA_CAREER_MIN = away_passer_career_df['CAY/PA'].min()
                            AWAY_PASS_YAC_CAREER_MEAN = away_passer_career_df['YAC'].mean() 
                            AWAY_PASS_YAC_CAREER_STD = away_passer_career_df['YAC'].std()
                            AWAY_PASS_YAC_CAREER_MAX = away_passer_career_df['YAC'].max()
                            AWAY_PASS_YAC_CAREER_MIN = away_passer_career_df['YAC'].min()
                            AWAY_PASS_YACCMP_CAREER_MEAN = away_passer_career_df['YAC/Cmp'].mean() 
                            AWAY_PASS_YACCMP_CAREER_STD = away_passer_career_df['YAC/Cmp'].std()
                            AWAY_PASS_YACCMP_CAREER_MAX = away_passer_career_df['YAC/Cmp'].max()
                            AWAY_PASS_YACCMP_CAREER_MIN = away_passer_career_df['YAC/Cmp'].min()
                            AWAY_PASS_BADTH_CAREER_MEAN = away_passer_career_df['BadTh'].mean() 
                            AWAY_PASS_BADTH_CAREER_STD = away_passer_career_df['BadTh'].std()
                            AWAY_PASS_BADTH_CAREER_MAX = away_passer_career_df['BadTh'].max()
                            AWAY_PASS_BADTH_CAREER_MIN = away_passer_career_df['BadTh'].min()
                            AWAY_PASS_SK_CAREER_MEAN = away_passer_career_df['Sk'].mean() 
                            AWAY_PASS_SK_CAREER_STD = away_passer_career_df['Sk'].std()
                            AWAY_PASS_SK_CAREER_MAX = away_passer_career_df['Sk'].max()
                            AWAY_PASS_SK_CAREER_MIN = away_passer_career_df['Sk'].min()
                            AWAY_PASS_BLITZ_CAREER_MEAN = away_passer_career_df['Bltz'].mean() 
                            AWAY_PASS_BLITZ_CAREER_STD = away_passer_career_df['Bltz'].std()
                            AWAY_PASS_BLITZ_CAREER_MAX = away_passer_career_df['Bltz'].max()
                            AWAY_PASS_BLITZ_CAREER_MIN = away_passer_career_df['Bltz'].min()
                            AWAY_PASS_HRRY_CAREER_MEAN = away_passer_career_df['Hrry'].mean() 
                            AWAY_PASS_HRRY_CAREER_STD = away_passer_career_df['Hrry'].std()
                            AWAY_PASS_HRRY_CAREER_MAX = away_passer_career_df['Hrry'].max()
                            AWAY_PASS_HRRY_CAREER_MIN = away_passer_career_df['Hrry'].min()
                            AWAY_PASS_HITS_CAREER_MEAN = away_passer_career_df['Hits'].mean() 
                            AWAY_PASS_HITS_CAREER_STD = away_passer_career_df['Hits'].std()
                            AWAY_PASS_HITS_CAREER_MAX = away_passer_career_df['Hits'].max()
                            AWAY_PASS_HITS_CAREER_MIN = away_passer_career_df['Hits'].min()
                            AWAY_PASS_PRSS_CAREER_MEAN = away_passer_career_df['Prss'].mean() 
                            AWAY_PASS_PRSS_CAREER_STD = away_passer_career_df['Prss'].std()
                            AWAY_PASS_PRSS_CAREER_MAX = away_passer_career_df['Prss'].max()
                            AWAY_PASS_PRSS_CAREER_MIN = away_passer_career_df['Prss'].min()
                            AWAY_PASS_PRSSPCT_CAREER_MEAN = away_passer_career_df['Prss%'].mean() 
                            AWAY_PASS_PRSSPCT_CAREER_STD = away_passer_career_df['Prss%'].std()
                            AWAY_PASS_PRSSPCT_CAREER_MAX = away_passer_career_df['Prss%'].max()
                            AWAY_PASS_PRSSPCT_CAREER_MIN = away_passer_career_df['Prss%'].min()

                            AWAY_PASS_BIG_GAME_W = away_passer_career_df['BIG_GAME_W'].sum()
                            AWAY_PASS_BIG_GAME_L = away_passer_career_df['BIG_GAME_L'].sum()
                            AWAY_PASS_PLAYOFF_W = away_passer_career_df['PLAYOFF_W'].sum()
                            AWAY_PASS_PLAYOFF_L = away_passer_career_df['PLAYOFF_L'].sum()
                            AWAY_PASS_CHAMP_W = away_passer_career_df['CHAMP_W'].sum()
                            AWAY_PASS_CHAMP_L = away_passer_career_df['CHAMP_L'].sum()

                            HOME_PASS_COMP_CAREER_MEAN = home_passer_career_df['Cmp'].mean()
                            HOME_PASS_COMP_CAREER_STD = home_passer_career_df['Cmp'].std()
                            HOME_PASS_COMP_CAREER_MAX = home_passer_career_df['Cmp'].min()
                            HOME_PASS_COMP_CAREER_MIN = home_passer_career_df['Cmp'].max()
                            HOME_PASS_ATT_CAREER_MEAN = home_passer_career_df['Att'].mean() 
                            HOME_PASS_ATT_CAREER_STD = home_passer_career_df['Att'].std()
                            HOME_PASS_ATT_CAREER_MAX = home_passer_career_df['Att'].max()
                            HOME_PASS_ATT_CAREER_MIN = home_passer_career_df['Att'].min()
                            HOME_PASS_YDS_CAREER_MEAN = home_passer_career_df['Yds'].mean() 
                            HOME_PASS_YDS_CAREER_STD = home_passer_career_df['Yds'].std()
                            HOME_PASS_YDS_CAREER_MAX = home_passer_career_df['Yds'].max()
                            HOME_PASS_YDS_CAREER_MIN = home_passer_career_df['Yds'].min()
                            HOME_PASS_1D_CAREER_MEAN = home_passer_career_df['1D'].mean() 
                            HOME_PASS_1D_CAREER_STD = home_passer_career_df['1D'].std()
                            HOME_PASS_1D_CAREER_MAX = home_passer_career_df['1D'].max()
                            HOME_PASS_1D_CAREER_MIN = home_passer_career_df['1D'].min()
                            HOME_PASS_1DPCT_CAREER_MEAN = home_passer_career_df['1D%'].mean() 
                            HOME_PASS_1DPCT_CAREER_STD = home_passer_career_df['1D%'].std()
                            HOME_PASS_1DPCT_CAREER_MAX = home_passer_career_df['1D%'].max()
                            HOME_PASS_1DPCT_CAREER_MIN = home_passer_career_df['1D%'].min()
                            HOME_PASS_IAY_CAREER_MEAN = home_passer_career_df['IAY'].mean() 
                            HOME_PASS_IAY_CAREER_STD = home_passer_career_df['IAY'].std()
                            HOME_PASS_IAY_CAREER_MAX = home_passer_career_df['IAY'].max()
                            HOME_PASS_IAY_CAREER_MIN = home_passer_career_df['IAY'].min()
                            HOME_PASS_IAYPA_CAREER_MEAN = home_passer_career_df['IAY/PA'].mean() 
                            HOME_PASS_IAYPA_CAREER_STD = home_passer_career_df['IAY/PA'].std()
                            HOME_PASS_IAYPA_CAREER_MAX = home_passer_career_df['IAY/PA'].max()
                            HOME_PASS_IAYPA_CAREER_MIN = home_passer_career_df['IAY/PA'].min()
                            HOME_PASS_CAY_CAREER_MEAN = home_passer_career_df['CAY'].mean() 
                            HOME_PASS_CAY_CAREER_STD = home_passer_career_df['CAY'].std()
                            HOME_PASS_CAY_CAREER_MAX = home_passer_career_df['CAY'].max()
                            HOME_PASS_CAY_CAREER_MIN = home_passer_career_df['CAY'].min()
                            HOME_PASS_CAYCMP_CAREER_MEAN = home_passer_career_df['CAY/Cmp'].mean() 
                            HOME_PASS_CAYCMP_CAREER_STD = home_passer_career_df['CAY/Cmp'].std()
                            HOME_PASS_CAYCMP_CAREER_MAX = home_passer_career_df['CAY/Cmp'].max()
                            HOME_PASS_CAYCMP_CAREER_MIN = home_passer_career_df['CAY/Cmp'].min()
                            HOME_PASS_CAYPA_CAREER_MEAN = home_passer_career_df['CAY/PA'].mean() 
                            HOME_PASS_CAYPA_CAREER_STD = home_passer_career_df['CAY/PA'].std()
                            HOME_PASS_CAYPA_CAREER_MAX = home_passer_career_df['CAY/PA'].max()
                            HOME_PASS_CAYPA_CAREER_MIN = home_passer_career_df['CAY/PA'].min()
                            HOME_PASS_YAC_CAREER_MEAN = home_passer_career_df['YAC'].mean() 
                            HOME_PASS_YAC_CAREER_STD = home_passer_career_df['YAC'].std()
                            HOME_PASS_YAC_CAREER_MAX = home_passer_career_df['YAC'].max()
                            HOME_PASS_YAC_CAREER_MIN = home_passer_career_df['YAC'].min()
                            HOME_PASS_YACCMP_CAREER_MEAN = home_passer_career_df['YAC/Cmp'].mean() 
                            HOME_PASS_YACCMP_CAREER_STD = home_passer_career_df['YAC/Cmp'].std()
                            HOME_PASS_YACCMP_CAREER_MAX = home_passer_career_df['YAC/Cmp'].max()
                            HOME_PASS_YACCMP_CAREER_MIN = home_passer_career_df['YAC/Cmp'].min()
                            HOME_PASS_BADTH_CAREER_MEAN = home_passer_career_df['BadTh'].mean() 
                            HOME_PASS_BADTH_CAREER_STD = home_passer_career_df['BadTh'].std()
                            HOME_PASS_BADTH_CAREER_MAX = home_passer_career_df['BadTh'].max()
                            HOME_PASS_BADTH_CAREER_MIN = home_passer_career_df['BadTh'].min()
                            HOME_PASS_SK_CAREER_MEAN = home_passer_career_df['Sk'].mean() 
                            HOME_PASS_SK_CAREER_STD = home_passer_career_df['Sk'].std()
                            HOME_PASS_SK_CAREER_MAX = home_passer_career_df['Sk'].max()
                            HOME_PASS_SK_CAREER_MIN = home_passer_career_df['Sk'].min()
                            HOME_PASS_BLITZ_CAREER_MEAN = home_passer_career_df['Bltz'].mean() 
                            HOME_PASS_BLITZ_CAREER_STD = home_passer_career_df['Bltz'].std()
                            HOME_PASS_BLITZ_CAREER_MAX = home_passer_career_df['Bltz'].max()
                            HOME_PASS_BLITZ_CAREER_MIN = home_passer_career_df['Bltz'].min()
                            HOME_PASS_HRRY_CAREER_MEAN = home_passer_career_df['Hrry'].mean() 
                            HOME_PASS_HRRY_CAREER_STD = home_passer_career_df['Hrry'].std()
                            HOME_PASS_HRRY_CAREER_MAX = home_passer_career_df['Hrry'].max()
                            HOME_PASS_HRRY_CAREER_MIN = home_passer_career_df['Hrry'].min()
                            HOME_PASS_HITS_CAREER_MEAN = home_passer_career_df['Hits'].mean() 
                            HOME_PASS_HITS_CAREER_STD = home_passer_career_df['Hits'].std()
                            HOME_PASS_HITS_CAREER_MAX = home_passer_career_df['Hits'].max()
                            HOME_PASS_HITS_CAREER_MIN = home_passer_career_df['Hits'].min()
                            HOME_PASS_PRSS_CAREER_MEAN = home_passer_career_df['Prss'].mean() 
                            HOME_PASS_PRSS_CAREER_STD = home_passer_career_df['Prss'].std()
                            HOME_PASS_PRSS_CAREER_MAX = home_passer_career_df['Prss'].max()
                            HOME_PASS_PRSS_CAREER_MIN = home_passer_career_df['Prss'].min()
                            HOME_PASS_PRSSPCT_CAREER_MEAN = home_passer_career_df['Prss%'].mean() 
                            HOME_PASS_PRSSPCT_CAREER_STD = home_passer_career_df['Prss%'].std()
                            HOME_PASS_PRSSPCT_CAREER_MAX = home_passer_career_df['Prss%'].max()
                            HOME_PASS_PRSSPCT_CAREER_MIN = home_passer_career_df['Prss%'].min()

                            HOME_PASS_BIG_GAME_W = home_passer_career_df['BIG_GAME_W'].sum()
                            HOME_PASS_BIG_GAME_L = home_passer_career_df['BIG_GAME_L'].sum()
                            HOME_PASS_PLAYOFF_W = home_passer_career_df['PLAYOFF_W'].sum()
                            HOME_PASS_PLAYOFF_L = home_passer_career_df['PLAYOFF_L'].sum()
                            HOME_PASS_CHAMP_W = home_passer_career_df['CHAMP_W'].sum()
                            HOME_PASS_CHAMP_L = home_passer_career_df['CHAMP_L'].sum()

                        # at any rate: append the flat passer values to passers_{year}, as well as passers_2018-present, after adding 'big game' wins
                        passers_frame.to_sql(f"passers_{year}", passers_con, if_exists="append")
                        passers_frame.to_sql(f"passers_2018-present", passers_con, if_exists="append")

                    #######################################################
                    #####   END COLLECTING DATA FOR PASSER TABLES     #####
                    #######################################################

                    #########################################################
                    #####   BEGIN COLLECTING DATA FOR SNAPS TABLES      #####
                    #########################################################
                    if "home_starters" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="home_starters")
                        home_starts_table = comment_soup.find('table', id="home_starters")

                        df = pd.read_html(str(home_starts_table))[0]
                        df['Team'] = home
                        df['Week'] = week

                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"starters_{year}\"", player_snaps_con)
                        if table.empty:
                            HOME_UNIQ_STARTERS_OL = -1
                            HOME_UNIQ_STARTERS_DEFENSE = -1
                            HOME_UNIQ_STARTERS_SKILL = -1
                            HOME_UNIQ_STARTERS_QB = -1

                        else:
                            home_uniq_starters_ol_query = f"SELECT COUNT(*) AS ol_count FROM starters_{year} where Pos in ('OL','LT','LG','C','RG','RT','OT','OG') and Team = {home} group by Team;"
                            count_df = pd.read_sql_query(home_uniq_starters_ol_query, player_snaps_con)
                            HOME_UNIQ_STARTERS_OL = count_df['ol_count'].iloc[0]

                            home_uniq_starters_def_query = f"SELECT COUNT(*) AS def_count FROM starters_{year} where Pos in ('DE','DT','NT','DL','EDGE','ILB','OLB','LB','S','SS','FS','CB','DB','RCB','LCB') and Team = {home} group by Team;"
                            count_df = pd.read_sql_query(home_uniq_starters_def_query, player_snaps_con)
                            HOME_UNIQ_STARTERS_DEFENSE = count_df['def_count'].iloc[0]

                            home_uniq_starters_skill_query = f"SELECT COUNT(*) AS skill_count FROM starters_{year} where Pos in ('WR','TE','RB') and Team = {home} group by Team;"
                            count_df = pd.read_sql_query(home_uniq_starters_skill_query, player_snaps_con)
                            HOME_UNIQ_STARTERS_SKILL = count_df['skill_count'].iloc[0]

                            home_uniq_starters_qb_query = f"SELECT COUNT(*) AS qb_count FROM starters_{year} where Pos in ('QB') and Team = {home} group by Team;"
                            count_df = pd.read_sql_query(home_uniq_starters_qb_query, player_snaps_con)
                            HOME_UNIQ_STARTERS_QB = count_df['qb_count'].iloc[0]

                        df.to_sql(f"starters_{year}", player_snaps_con, if_exists="append")

                    if "away_starters" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="away_starters")
                        away_starts_table = comment_soup.find('table', id="away_starters")

                        df = pd.read_html(str(away_starts_table))[0]
                        df['Team'] = vis
                        df['Week'] = week

                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"starters_{year}\"", player_snaps_con)
                        if table.empty:
                            AWAY_UNIQ_STARTERS_OL = -1
                            AWAY_UNIQ_STARTERS_DEFENSE = -1
                            AWAY_UNIQ_STARTERS_SKILL = -1
                            AWAY_UNIQ_STARTERS_QB = -1
                        else:
                            away_uniq_starters_ol_query = f"SELECT COUNT(*) AS ol_count FROM starters_{year} where Pos in ('OL','LT','LG','C','RG','RT','OT','OG') and Team = {vis} group by Team;"
                            count_df = pd.read_sql_query(away_uniq_starters_ol_query, player_snaps_con)
                            AWAY_UNIQ_STARTERS_OL = count_df['ol_count'].iloc[0]

                            away_uniq_starters_def_query = f"SELECT COUNT(*) AS def_count FROM starters_{year} where Pos in ('DE','DT','NT','DL','EDGE','ILB','OLB','LB','S','SS','FS','CB','DB','RCB','LCB') and Team = {vis} group by Team;"
                            count_df = pd.read_sql_query(away_uniq_starters_def_query, player_snaps_con)
                            AWAY_UNIQ_STARTERS_DEFENSE = count_df['def_count'].iloc[0]

                            away_uniq_starters_skill_query = f"SELECT COUNT(*) AS skill_count FROM starters_{year} where Pos in ('WR','TE','RB') and Team = {vis} group by Team;"
                            count_df = pd.read_sql_query(away_uniq_starters_skill_query, player_snaps_con)
                            AWAY_UNIQ_STARTERS_SKILL = count_df['skill_count'].iloc[0]

                            away_uniq_starters_qb_query = f"SELECT COUNT(*) AS qb_count FROM starters_{year} where Pos in ('QB') and Team = {vis} group by Team;"
                            count_df = pd.read_sql_query(away_uniq_starters_qb_query, player_snaps_con)
                            AWAY_UNIQ_STARTERS_QB = count_df['qb_count'].iloc[0]

                        df.to_sql(f"starters_{year}", player_snaps_con, if_exists="append")

                    #######################################################
                    #####   END COLLECTING DATA FOR SNAPS TABLES      #####
                    #######################################################

            except Exception as e: 
                print('could not get game data: ', e)
                

            season_data.append({
                'SEASON': year,
                'AWAY_TEAM_NAME': vis,
                'AWAY_TEAM_PREV_RANK': get_prev_year_rank(year, vis),
                'HOME_TEAM_NAME': home,
                'HOME_TEAM_PREV_RANK': get_prev_year_rank(year, home),
                'AWAY_SCORE': AWAY_SCORE,
                'HOME_SCORE': HOME_SCORE,
                'WEEK': week,
                'AWAY_SOS': AWAY_SOS, # NEED TO GET AVERAGE SOS ENTERING GAME
                'HOME_SOS': HOME_SOS,
                'AWAY_UNIQ_STARTERS_QB': AWAY_UNIQ_STARTERS_QB,
                'AWAY_UNIQ_STARTERS_DEFENSE': AWAY_UNIQ_STARTERS_DEFENSE,
                'AWAY_UNIQ_STARTERS_OL': AWAY_UNIQ_STARTERS_OL,
                'AWAY_UNIQ_STARTERS_SKILL': AWAY_UNIQ_STARTERS_SKILL,
                'HOME_UNIQ_STARTERS_QB': HOME_UNIQ_STARTERS_QB,
                'HOME_UNIQ_STARTERS_DEFENSE': HOME_UNIQ_STARTERS_DEFENSE,
                'HOME_UNIQ_STARTERS_OL': HOME_UNIQ_STARTERS_OL,
                'HOME_UNIQ_STARTERS_SKILL': HOME_UNIQ_STARTERS_SKILL,
                'AWAY_FD_MEAN': AWAY_FD_MEAN, 
                'AWAY_FD_STD': AWAY_FD_STD,
                'AWAY_FD_MAX': AWAY_FD_MAX,
                'AWAY_FD_MIN': AWAY_FD_MIN,
                'AWAY_FD_AGAINST_MEAN': AWAY_FD_AGAINST_MEAN,
                'AWAY_FD_AGAINST_STD': AWAY_FD_AGAINST_STD,
                'AWAY_FD_AGAINST_MAX': AWAY_FD_AGAINST_MAX,
                'AWAY_FD_AGAINST_MIN': AWAY_FD_AGAINST_MIN,
                'AWAY_SACKS_MEAN': AWAY_SACKS_MEAN, 
                'AWAY_SACKS_STD': AWAY_SACKS_STD,
                'AWAY_SACKS_MAX': AWAY_SACKS_MAX,
                'AWAY_SACKS_MIN': AWAY_SACKS_MIN,
                'AWAY_SACKS_AGAINST_MEAN': AWAY_SACKS_AGAINST_MEAN, 
                'AWAY_SACKS_AGAINST_STD': AWAY_SACKS_AGAINST_STD,
                'AWAY_SACKS_AGAINST_MAX': AWAY_SACKS_AGAINST_MAX,
                'AWAY_SACKS_AGAINST_MIN': AWAY_SACKS_AGAINST_MIN,
                'AWAY_SACK_YDS_MEAN': AWAY_SACK_YDS_MEAN, 
                'AWAY_SACK_YDS_STD': AWAY_SACK_YDS_STD,
                'AWAY_SACK_YDS_MAX': AWAY_SACK_YDS_MAX,
                'AWAY_SACK_YDS_MIN': AWAY_SACK_YDS_MIN,
                'AWAY_SACK_YDS_AGAINST_MEAN': AWAY_SACK_YDS_AGAINST_MEAN, 
                'AWAY_SACK_YDS_AGAINST_STD': AWAY_SACK_YDS_AGAINST_STD,
                'AWAY_SACK_YDS_AGAINST_MAX': AWAY_SACK_YDS_AGAINST_MAX,
                'AWAY_SACK_YDS_AGAINST_MIN': AWAY_SACK_YDS_AGAINST_MIN,
                'AWAY_TOTAL_YDS_MEAN': AWAY_TOTAL_YDS_MEAN, 
                'AWAY_TOTAL_YDS_STD': AWAY_TOTAL_YDS_STD,
                'AWAY_TOTAL_YDS_MAX': AWAY_TOTAL_YDS_MAX,
                'AWAY_TOTAL_YDS_MIN': AWAY_TOTAL_YDS_MIN,
                'AWAY_FUMBLES_MEAN': AWAY_FUMBLES_MEAN, 
                'AWAY_FUMBLES_STD': AWAY_FUMBLES_STD,
                'AWAY_FUMBLES_MAX': AWAY_FUMBLES_MAX,
                'AWAY_FUMBLES_MIN': AWAY_FUMBLES_MIN,
                'AWAY_FUMBLES_LOST_MEAN': AWAY_FUMBLES_LOST_MEAN, 
                'AWAY_FUMBLES_LOST_STD': AWAY_FUMBLES_LOST_STD,
                'AWAY_FUMBLES_LOST_MAX': AWAY_FUMBLES_LOST_MAX,
                'AWAY_FUMBLES_LOST_MIN': AWAY_FUMBLES_LOST_MIN,
                'AWAY_TO_MEAN': AWAY_TO_MEAN, 
                'AWAY_TO_STD': AWAY_TO_STD,
                'AWAY_TO_MAX': AWAY_TO_MAX,
                'AWAY_TO_MIN': AWAY_TO_MIN,
                'AWAY_TO_AGAINST_MEAN': AWAY_TO_AGAINST_MEAN, 
                'AWAY_TO_AGAINST_STD': AWAY_TO_AGAINST_STD,
                'AWAY_TO_AGAINST_MAX': AWAY_TO_AGAINST_MAX,
                'AWAY_TO_AGAINST_MIN': AWAY_TO_AGAINST_MIN,
                'AWAY_PENALTIES_MEAN': AWAY_PENALTIES_MEAN, 
                'AWAY_PENALTIES_STD': AWAY_PENALTIES_STD,
                'AWAY_PENALTIES_MAX': AWAY_PENALTIES_MAX,
                'AWAY_PENALTIES_MIN': AWAY_PENALTIES_MIN,
                'AWAY_PENALTY_YARDS_MEAN': AWAY_PENALTY_YARDS_MEAN, 
                'AWAY_PENALTY_YARDS_STD': AWAY_PENALTY_YARDS_STD,
                'AWAY_PENALTY_YARDS_MAX': AWAY_PENALTY_YARDS_MAX,
                'AWAY_PENALTY_YARDS_MIN': AWAY_PENALTY_YARDS_MIN,
                'AWAY_3RD_DOWN_MEAN': AWAY_3RD_DOWN_MEAN, 
                'AWAY_3RD_DOWN_STD': AWAY_3RD_DOWN_STD,
                'AWAY_3RD_DOWN_MAX': AWAY_3RD_DOWN_MAX,
                'AWAY_3RD_DOWN_MIN': AWAY_3RD_DOWN_MIN,
                'AWAY_3RD_DOWN_ATT_MEAN': AWAY_3RD_DOWN_ATT_MEAN, 
                'AWAY_3RD_DOWN_ATT_STD': AWAY_3RD_DOWN_ATT_STD,
                'AWAY_3RD_DOWN_ATT_MAX': AWAY_3RD_DOWN_ATT_MAX,
                'AWAY_3RD_DOWN_ATT_MIN': AWAY_3RD_DOWN_ATT_MIN,
                'AWAY_3RD_DOWN_CONV_MEAN': AWAY_3RD_DOWN_CONV_MEAN, 
                'AWAY_3RD_DOWN_CONV_STD': AWAY_3RD_DOWN_CONV_STD,
                'AWAY_3RD_DOWN_CONV_MAX': AWAY_3RD_DOWN_CONV_MAX,
                'AWAY_3RD_DOWN_CONV_MIN': AWAY_3RD_DOWN_CONV_MIN,
                'AWAY_3RD_DOWN_AGAINST_MEAN': AWAY_3RD_DOWN_AGAINST_MEAN,
                'AWAY_3RD_DOWN_AGAINST_STD': AWAY_3RD_DOWN_AGAINST_STD,
                'AWAY_3RD_DOWN_AGAINST_MAX': AWAY_3RD_DOWN_AGAINST_MAX,
                'AWAY_3RD_DOWN_AGAINST_MIN': AWAY_3RD_DOWN_AGAINST_MIN,
                'AWAY_3RD_DOWN_ATT_AGAINST_MEAN': AWAY_3RD_DOWN_ATT_AGAINST_MEAN,
                'AWAY_3RD_DOWN_ATT_AGAINST_STD': AWAY_3RD_DOWN_ATT_AGAINST_STD,
                'AWAY_3RD_DOWN_ATT_AGAINST_MAX': AWAY_3RD_DOWN_ATT_AGAINST_MAX,
                'AWAY_3RD_DOWN_ATT_AGAINST_MIN': AWAY_3RD_DOWN_ATT_AGAINST_MIN,
                'AWAY_3RD_DOWN_CONV_AGAINST_MEAN': AWAY_3RD_DOWN_CONV_AGAINST_MEAN,
                'AWAY_3RD_DOWN_CONV_AGAINST_STD': AWAY_3RD_DOWN_CONV_AGAINST_STD,
                'AWAY_3RD_DOWN_CONV_AGAINST_MAX': AWAY_3RD_DOWN_CONV_AGAINST_MAX,
                'AWAY_3RD_DOWN_CONV_AGAINST_MIN': AWAY_3RD_DOWN_CONV_AGAINST_MIN,
                'AWAY_TOP_MEAN': AWAY_TOP_MEAN, 
                'AWAY_TOP_STD': AWAY_TOP_STD,
                'AWAY_TOP_MAX': AWAY_TOP_MAX,
                'AWAY_TOP_MIN': AWAY_TOP_MIN,
                'AWAY_PASS_COMP_MEAN': 0, 
                'AWAY_PASS_COMP_STD': 0,
                'AWAY_PASS_COMP_MAX': 0,
                'AWAY_PASS_COMP_MIN': 0,
                'AWAY_PASS_ATT_MEAN': 0, 
                'AWAY_PASS_ATT_STD': 0,
                'AWAY_PASS_ATT_MAX': 0,
                'AWAY_PASS_ATT_MIN': 0,
                'AWAY_PASS_YDS_MEAN': 0, 
                'AWAY_PASS_YDS_STD': 0,
                'AWAY_PASS_YDS_MAX': 0,
                'AWAY_PASS_YDS_MIN': 0,
                'AWAY_PASS_1D_MEAN': 0, 
                'AWAY_PASS_1D_STD': 0,
                'AWAY_PASS_1D_MAX': 0,
                'AWAY_PASS_1D_MIN': 0,
                'AWAY_PASS_1DPCT_MEAN': 0, 
                'AWAY_PASS_1DPCT_STD': 0,
                'AWAY_PASS_1DPCT_MAX': 0,
                'AWAY_PASS_1DPCT_MIN': 0,
                'AWAY_PASS_IAY_MEAN': 0, 
                'AWAY_PASS_IAY_STD': 0,
                'AWAY_PASS_IAY_MAX': 0,
                'AWAY_PASS_IAY_MIN': 0,
                'AWAY_PASS_IAYPA_MEAN': 0, 
                'AWAY_PASS_IAYPA_STD': 0,
                'AWAY_PASS_IAYPA_MAX': 0,
                'AWAY_PASS_IAYPA_MIN': 0,
                'AWAY_PASS_CAY_MEAN': 0, 
                'AWAY_PASS_CAY_STD': 0,
                'AWAY_PASS_CAY_MAX': 0,
                'AWAY_PASS_CAY_MIN': 0,
                'AWAY_PASS_CAYCMP_MEAN': 0, 
                'AWAY_PASS_CAYCMP_STD': 0,
                'AWAY_PASS_CAYCMP_MAX': 0,
                'AWAY_PASS_CAYCMP_MIN': 0,
                'AWAY_PASS_CAYPA_MEAN': 0, 
                'AWAY_PASS_CAYPA_STD': 0,
                'AWAY_PASS_CAYPA_MAX': 0,
                'AWAY_PASS_CAYPA_MIN': 0,
                'AWAY_PASS_YAC_MEAN': 0, 
                'AWAY_PASS_YAC_STD': 0,
                'AWAY_PASS_YAC_MAX': 0,
                'AWAY_PASS_YAC_MIN': 0,
                'AWAY_PASS_YACCMP_MEAN': 0, 
                'AWAY_PASS_YACCMP_STD': 0,
                'AWAY_PASS_YACCMP_MAX': 0,
                'AWAY_PASS_YACCMP_MIN': 0,
                'AWAY_PASS_DROPS_MEAN': 0, 
                'AWAY_PASS_DROPS_STD': 0,
                'AWAY_PASS_DROPS_MAX': 0,
                'AWAY_PASS_DROPS_MIN': 0,
                'AWAY_PASS_DROPPCT_MEAN': 0, 
                'AWAY_PASS_DROPPCT_STD': 0,
                'AWAY_PASS_DROPPCT_MAX': 0,
                'AWAY_PASS_DROPPCT_MIN': 0,
                'AWAY_PASS_BADTH_MEAN': 0, 
                'AWAY_PASS_BADTH_STD': 0,
                'AWAY_PASS_BADTH_MAX': 0,
                'AWAY_PASS_BADTH_MIN': 0,
                'AWAY_PASS_SK_MEAN': 0, 
                'AWAY_PASS_SK_STD': 0,
                'AWAY_PASS_SK_MAX': 0,
                'AWAY_PASS_SK_MIN': 0,
                'AWAY_PASS_BLITZ_MEAN': 0, 
                'AWAY_PASS_BLITZ_STD': 0,
                'AWAY_PASS_BLITZ_MAX': 0,
                'AWAY_PASS_BLITZ_MIN': 0,
                'AWAY_PASS_HRRY_MEAN': 0, 
                'AWAY_PASS_HRRY_STD': 0,
                'AWAY_PASS_HRRY_MAX': 0,
                'AWAY_PASS_HRRY_MIN': 0,
                'AWAY_PASS_HITS_MEAN': 0, 
                'AWAY_PASS_HITS_STD': 0,
                'AWAY_PASS_HITS_MAX': 0,
                'AWAY_PASS_HITS_MIN': 0,
                'AWAY_PASS_PRSS_MEAN': 0, 
                'AWAY_PASS_PRSS_STD': 0,
                'AWAY_PASS_PRSS_MAX': 0,
                'AWAY_PASS_PRSS_MIN': 0,
                'AWAY_PASS_PRSSPCT_MEAN': 0, 
                'AWAY_PASS_PRSSPCT_STD': 0,
                'AWAY_PASS_PRSSPCT_MAX': 0,
                'AWAY_PASS_PRSSPCT_MIN': 0,
                'AWAY_PASS_SCRM_MEAN': 0, 
                'AWAY_PASS_SCRM_STD': 0,
                'AWAY_PASS_SCRM_MAX': 0,
                'AWAY_PASS_SCRM_MIN': 0,
                'AWAY_PASS_YDSSCRM_MEAN': 0, 
                'AWAY_PASS_YDSSCRM_STD': 0,
                'AWAY_PASS_YDSSCRM_MAX': 0,
                'AWAY_PASS_YDSSCRM_MIN': 0,
                'AWAY_PASS_COMP_CAREER_MEAN': 0, 
                'AWAY_PASS_COMP_CAREER_STD': 0,
                'AWAY_PASS_COMP_CAREER_MAX': 0,
                'AWAY_PASS_COMP_CAREER_MIN': 0,
                'AWAY_PASS_ATT_CAREER_MEAN': 0, 
                'AWAY_PASS_ATT_CAREER_STD': 0,
                'AWAY_PASS_ATT_CAREER_MAX': 0,
                'AWAY_PASS_ATT_CAREER_MIN': 0,
                'AWAY_PASS_YDS_CAREER_MEAN': 0, 
                'AWAY_PASS_YDS_CAREER_STD': 0,
                'AWAY_PASS_YDS_CAREER_MAX': 0,
                'AWAY_PASS_YDS_CAREER_MIN': 0,
                'AWAY_PASS_1D_CAREER_MEAN': 0, 
                'AWAY_PASS_1D_CAREER_STD': 0,
                'AWAY_PASS_1D_CAREER_MAX': 0,
                'AWAY_PASS_1D_CAREER_MIN': 0,
                'AWAY_PASS_1DPCT_CAREER_MEAN': 0, 
                'AWAY_PASS_1DPCT_CAREER_STD': 0,
                'AWAY_PASS_1DPCT_CAREER_MAX': 0,
                'AWAY_PASS_1DPCT_CAREER_MIN': 0,
                'AWAY_PASS_IAY_CAREER_MEAN': 0, 
                'AWAY_PASS_IAY_CAREER_STD': 0,
                'AWAY_PASS_IAY_CAREER_MAX': 0,
                'AWAY_PASS_IAY_CAREER_MIN': 0,
                'AWAY_PASS_IAYPA_CAREER_MEAN': 0, 
                'AWAY_PASS_IAYPA_CAREER_STD': 0,
                'AWAY_PASS_IAYPA_CAREER_MAX': 0,
                'AWAY_PASS_IAYPA_CAREER_MIN': 0,
                'AWAY_PASS_CAY_CAREER_MEAN': 0, 
                'AWAY_PASS_CAY_CAREER_STD': 0,
                'AWAY_PASS_CAY_CAREER_MAX': 0,
                'AWAY_PASS_CAY_CAREER_MIN': 0,
                'AWAY_PASS_CAYCMP_CAREER_MEAN': 0, 
                'AWAY_PASS_CAYCMP_CAREER_STD': 0,
                'AWAY_PASS_CAYCMP_CAREER_MAX': 0,
                'AWAY_PASS_CAYCMP_CAREER_MIN': 0,
                'AWAY_PASS_CAYPA_CAREER_MEAN': 0, 
                'AWAY_PASS_CAYPA_CAREER_STD': 0,
                'AWAY_PASS_CAYPA_CAREER_MAX': 0,
                'AWAY_PASS_CAYPA_CAREER_MIN': 0,
                'AWAY_PASS_YAC_CAREER_MEAN': 0, 
                'AWAY_PASS_YAC_CAREER_STD': 0,
                'AWAY_PASS_YAC_CAREER_MAX': 0,
                'AWAY_PASS_YAC_CAREER_MIN': 0,
                'AWAY_PASS_YACCMP_CAREER_MEAN': 0, 
                'AWAY_PASS_YACCMP_CAREER_STD': 0,
                'AWAY_PASS_YACCMP_CAREER_MAX': 0,
                'AWAY_PASS_YACCMP_CAREER_MIN': 0,
                'AWAY_PASS_BADTH_CAREER_MEAN': 0, 
                'AWAY_PASS_BADTH_CAREER_STD': 0,
                'AWAY_PASS_BADTH_CAREER_MAX': 0,
                'AWAY_PASS_BADTH_CAREER_MIN': 0,
                'AWAY_PASS_SK_CAREER_MEAN': 0, 
                'AWAY_PASS_SK_CAREER_STD': 0,
                'AWAY_PASS_SK_CAREER_MAX': 0,
                'AWAY_PASS_SK_CAREER_MIN': 0,
                'AWAY_PASS_BLITZ_CAREER_MEAN': 0, 
                'AWAY_PASS_BLITZ_CAREER_STD': 0,
                'AWAY_PASS_BLITZ_CAREER_MAX': 0,
                'AWAY_PASS_BLITZ_CAREER_MIN': 0,
                'AWAY_PASS_HRRY_CAREER_MEAN': 0, 
                'AWAY_PASS_HRRY_CAREER_STD': 0,
                'AWAY_PASS_HRRY_CAREER_MAX': 0,
                'AWAY_PASS_HRRY_CAREER_MIN': 0,
                'AWAY_PASS_HITS_CAREER_MEAN': 0, 
                'AWAY_PASS_HITS_CAREER_STD': 0,
                'AWAY_PASS_HITS_CAREER_MAX': 0,
                'AWAY_PASS_HITS_CAREER_MIN': 0,
                'AWAY_PASS_PRSS_CAREER_MEAN': 0, 
                'AWAY_PASS_PRSS_CAREER_STD': 0,
                'AWAY_PASS_PRSS_CAREER_MAX': 0,
                'AWAY_PASS_PRSS_CAREER_MIN': 0,
                'AWAY_PASS_PRSSPCT_CAREER_MEAN': 0, 
                'AWAY_PASS_PRSSPCT_CAREER_STD': 0,
                'AWAY_PASS_PRSSPCT_CAREER_MAX': 0,
                'AWAY_PASS_PRSSPCT_CAREER_MIN': 0,
                'AWAY_PASS_BIG_GAME_W': 0,
                'AWAY_PASS_BIG_GAME_L': 0,
                'AWAY_PASS_PLAYOFF_W': 0,
                'AWAY_PASS_PLAYOFF_L': 0,
                'AWAY_PASS_CHAMP_W': 0,
                'AWAY_PASS_CHAMP_L': 0,
                'HOME_FD_MEAN': HOME_FD_MEAN, 
                'HOME_FD_STD': HOME_FD_STD,
                'HOME_FD_MAX': HOME_FD_MAX,
                'HOME_FD_MIN': HOME_FD_MIN,
                'HOME_FD_AGAINST_MEAN': HOME_FD_AGAINST_MEAN,
                'HOME_FD_AGAINST_STD': HOME_FD_AGAINST_STD,
                'HOME_FD_AGAINST_MAX': HOME_FD_AGAINST_MAX,
                'HOME_FD_AGAINST_MIN': HOME_FD_AGAINST_MIN,
                'HOME_SACKS_MEAN': HOME_SACKS_MEAN, 
                'HOME_SACKS_STD': HOME_SACKS_STD,
                'HOME_SACKS_MAX': HOME_SACKS_MAX,
                'HOME_SACKS_MIN': HOME_SACKS_MIN,
                'HOME_SACKS_AGAINST_MEAN': HOME_SACKS_AGAINST_MEAN, 
                'HOME_SACKS_AGAINST_STD': HOME_SACKS_AGAINST_STD,
                'HOME_SACKS_AGAINST_MAX': HOME_SACKS_AGAINST_MAX,
                'HOME_SACKS_AGAINST_MIN': HOME_SACKS_AGAINST_MIN,
                'HOME_SACK_YDS_MEAN': HOME_SACK_YDS_MEAN, 
                'HOME_SACK_YDS_STD': HOME_SACK_YDS_STD,
                'HOME_SACK_YDS_MAX': HOME_SACK_YDS_MAX,
                'HOME_SACK_YDS_MIN': HOME_SACK_YDS_MIN,
                'HOME_SACK_YDS_AGAINST_MEAN': HOME_SACK_YDS_AGAINST_MEAN, 
                'HOME_SACK_YDS_AGAINST_STD': HOME_SACK_YDS_AGAINST_STD,
                'HOME_SACK_YDS_AGAINST_MAX': HOME_SACK_YDS_AGAINST_MAX,
                'HOME_SACK_YDS_AGAINST_MIN': HOME_SACK_YDS_AGAINST_MIN,
                'HOME_TOTAL_YDS_MEAN': HOME_TOTAL_YDS_MEAN, 
                'HOME_TOTAL_YDS_STD': HOME_TOTAL_YDS_STD,
                'HOME_TOTAL_YDS_MAX': HOME_TOTAL_YDS_MAX,
                'HOME_TOTAL_YDS_MIN': HOME_TOTAL_YDS_MIN,
                'HOME_FUMBLES_MEAN': HOME_FUMBLES_MEAN, 
                'HOME_FUMBLES_STD': HOME_FUMBLES_STD,
                'HOME_FUMBLES_MAX': HOME_FUMBLES_MAX,
                'HOME_FUMBLES_MIN': HOME_FUMBLES_MIN,
                'HOME_FUMBLES_LOST_MEAN': HOME_FUMBLES_LOST_MEAN, 
                'HOME_FUMBLES_LOST_STD': HOME_FUMBLES_LOST_STD,
                'HOME_FUMBLES_LOST_MAX': HOME_FUMBLES_LOST_MAX,
                'HOME_FUMBLES_LOST_MIN': HOME_FUMBLES_LOST_MIN,
                'HOME_TO_MEAN': HOME_TO_MEAN, 
                'HOME_TO_STD': HOME_TO_STD,
                'HOME_TO_MAX': HOME_TO_MAX,
                'HOME_TO_MIN': HOME_TO_MIN,
                'HOME_TO_AGAINST_MEAN': HOME_TO_AGAINST_MEAN, 
                'HOME_TO_AGAINST_STD': HOME_TO_AGAINST_STD,
                'HOME_TO_AGAINST_MAX': HOME_TO_AGAINST_MAX,
                'HOME_TO_AGAINST_MIN': HOME_TO_AGAINST_MIN,
                'HOME_PENALTIES_MEAN': HOME_PENALTIES_MEAN, 
                'HOME_PENALTIES_STD': HOME_PENALTIES_STD,
                'HOME_PENALTIES_MAX': HOME_PENALTIES_MAX,
                'HOME_PENALTIES_MIN': HOME_PENALTIES_MIN,
                'HOME_PENALTY_YARDS_MEAN': HOME_PENALTY_YARDS_MEAN, 
                'HOME_PENALTY_YARDS_STD': HOME_PENALTY_YARDS_STD,
                'HOME_PENALTY_YARDS_MAX': HOME_PENALTY_YARDS_MAX,
                'HOME_PENALTY_YARDS_MIN': HOME_PENALTY_YARDS_MIN,
                'HOME_3RD_DOWN_MEAN': HOME_3RD_DOWN_MEAN, 
                'HOME_3RD_DOWN_STD': HOME_3RD_DOWN_STD,
                'HOME_3RD_DOWN_MAX': HOME_3RD_DOWN_MAX,
                'HOME_3RD_DOWN_MIN': HOME_3RD_DOWN_MIN,
                'HOME_3RD_DOWN_ATT_MEAN': HOME_3RD_DOWN_ATT_MEAN, 
                'HOME_3RD_DOWN_ATT_STD': HOME_3RD_DOWN_ATT_STD,
                'HOME_3RD_DOWN_ATT_MAX': HOME_3RD_DOWN_ATT_MAX,
                'HOME_3RD_DOWN_ATT_MIN': HOME_3RD_DOWN_ATT_MIN,
                'HOME_3RD_DOWN_CONV_MEAN': HOME_3RD_DOWN_CONV_MEAN, 
                'HOME_3RD_DOWN_CONV_STD': HOME_3RD_DOWN_CONV_STD,
                'HOME_3RD_DOWN_CONV_MAX': HOME_3RD_DOWN_CONV_MAX,
                'HOME_3RD_DOWN_CONV_MIN': HOME_3RD_DOWN_CONV_MIN,
                'HOME_3RD_DOWN_AGAINST_MEAN': HOME_3RD_DOWN_AGAINST_MEAN,
                'HOME_3RD_DOWN_AGAINST_STD': HOME_3RD_DOWN_AGAINST_STD,
                'HOME_3RD_DOWN_AGAINST_MAX': HOME_3RD_DOWN_AGAINST_MAX,
                'HOME_3RD_DOWN_AGAINST_MIN': HOME_3RD_DOWN_AGAINST_MIN,
                'HOME_3RD_DOWN_ATT_AGAINST_MEAN': HOME_3RD_DOWN_ATT_AGAINST_MEAN,
                'HOME_3RD_DOWN_ATT_AGAINST_STD': HOME_3RD_DOWN_ATT_AGAINST_STD,
                'HOME_3RD_DOWN_ATT_AGAINST_MAX': HOME_3RD_DOWN_ATT_AGAINST_MAX,
                'HOME_3RD_DOWN_ATT_AGAINST_MIN': HOME_3RD_DOWN_ATT_AGAINST_MIN,
                'HOME_3RD_DOWN_CONV_AGAINST_MEAN': HOME_3RD_DOWN_CONV_AGAINST_MEAN,
                'HOME_3RD_DOWN_CONV_AGAINST_STD': HOME_3RD_DOWN_CONV_AGAINST_STD,
                'HOME_3RD_DOWN_CONV_AGAINST_MAX': HOME_3RD_DOWN_CONV_AGAINST_MAX,
                'HOME_3RD_DOWN_CONV_AGAINST_MIN': HOME_3RD_DOWN_CONV_AGAINST_MIN,
                'HOME_TOP_MEAN': HOME_TOP_MEAN, 
                'HOME_TOP_STD': HOME_TOP_STD,
                'HOME_TOP_MAX': HOME_TOP_MAX,
                'HOME_TOP_MIN': HOME_TOP_MIN,
                'HOME_PASS_COMP_MEAN': 0, 
                'HOME_PASS_COMP_STD': 0,
                'HOME_PASS_COMP_MAX': 0,
                'HOME_PASS_COMP_MIN': 0,
                'HOME_PASS_ATT_MEAN': 0, 
                'HOME_PASS_ATT_STD': 0,
                'HOME_PASS_ATT_MAX': 0,
                'HOME_PASS_ATT_MIN': 0,
                'HOME_PASS_YDS_MEAN': 0, 
                'HOME_PASS_YDS_STD': 0,
                'HOME_PASS_YDS_MAX': 0,
                'HOME_PASS_YDS_MIN': 0,
                'HOME_PASS_1D_MEAN': 0, 
                'HOME_PASS_1D_STD': 0,
                'HOME_PASS_1D_MAX': 0,
                'HOME_PASS_1D_MIN': 0,
                'HOME_PASS_1DPCT_MEAN': 0, 
                'HOME_PASS_1DPCT_STD': 0,
                'HOME_PASS_1DPCT_MAX': 0,
                'HOME_PASS_1DPCT_MIN': 0,
                'HOME_PASS_IAY_MEAN': 0, 
                'HOME_PASS_IAY_STD': 0,
                'HOME_PASS_IAY_MAX': 0,
                'HOME_PASS_IAY_MIN': 0,
                'HOME_PASS_IAYPA_MEAN': 0, 
                'HOME_PASS_IAYPA_STD': 0,
                'HOME_PASS_IAYPA_MAX': 0,
                'HOME_PASS_IAYPA_MIN': 0,
                'HOME_PASS_CAY_MEAN': 0, 
                'HOME_PASS_CAY_STD': 0,
                'HOME_PASS_CAY_MAX': 0,
                'HOME_PASS_CAY_MIN': 0,
                'HOME_PASS_CAYCMP_MEAN': 0, 
                'HOME_PASS_CAYCMP_STD': 0,
                'HOME_PASS_CAYCMP_MAX': 0,
                'HOME_PASS_CAYCMP_MIN': 0,
                'HOME_PASS_CAYPA_MEAN': 0, 
                'HOME_PASS_CAYPA_STD': 0,
                'HOME_PASS_CAYPA_MAX': 0,
                'HOME_PASS_CAYPA_MIN': 0,
                'HOME_PASS_YAC_MEAN': 0, 
                'HOME_PASS_YAC_STD': 0,
                'HOME_PASS_YAC_MAX': 0,
                'HOME_PASS_YAC_MIN': 0,
                'HOME_PASS_YACCMP_MEAN': 0, 
                'HOME_PASS_YACCMP_STD': 0,
                'HOME_PASS_YACCMP_MAX': 0,
                'HOME_PASS_YACCMP_MIN': 0,
                'HOME_PASS_DROPS_MEAN': 0, 
                'HOME_PASS_DROPS_STD': 0,
                'HOME_PASS_DROPS_MAX': 0,
                'HOME_PASS_DROPS_MIN': 0,
                'HOME_PASS_DROPPCT_MEAN': 0, 
                'HOME_PASS_DROPPCT_STD': 0,
                'HOME_PASS_DROPPCT_MAX': 0,
                'HOME_PASS_DROPPCT_MIN': 0,
                'HOME_PASS_BADTH_MEAN': 0, 
                'HOME_PASS_BADTH_STD': 0,
                'HOME_PASS_BADTH_MAX': 0,
                'HOME_PASS_BADTH_MIN': 0,
                'HOME_PASS_SK_MEAN': 0, 
                'HOME_PASS_SK_STD': 0,
                'HOME_PASS_SK_MAX': 0,
                'HOME_PASS_SK_MIN': 0,
                'HOME_PASS_BLITZ_MEAN': 0, 
                'HOME_PASS_BLITZ_STD': 0,
                'HOME_PASS_BLITZ_MAX': 0,
                'HOME_PASS_BLITZ_MIN': 0,
                'HOME_PASS_HRRY_MEAN': 0, 
                'HOME_PASS_HRRY_STD': 0,
                'HOME_PASS_HRRY_MAX': 0,
                'HOME_PASS_HRRY_MIN': 0,
                'HOME_PASS_HITS_MEAN': 0, 
                'HOME_PASS_HITS_STD': 0,
                'HOME_PASS_HITS_MAX': 0,
                'HOME_PASS_HITS_MIN': 0,
                'HOME_PASS_PRSS_MEAN': 0, 
                'HOME_PASS_PRSS_STD': 0,
                'HOME_PASS_PRSS_MAX': 0,
                'HOME_PASS_PRSS_MIN': 0,
                'HOME_PASS_PRSSPCT_MEAN': 0, 
                'HOME_PASS_PRSSPCT_STD': 0,
                'HOME_PASS_PRSSPCT_MAX': 0,
                'HOME_PASS_PRSSPCT_MIN': 0,
                'HOME_PASS_SCRM_MEAN': 0, 
                'HOME_PASS_SCRM_STD': 0,
                'HOME_PASS_SCRM_MAX': 0,
                'HOME_PASS_SCRM_MIN': 0,
                'HOME_PASS_YDSSCRM_MEAN': 0, 
                'HOME_PASS_YDSSCRM_STD': 0,
                'HOME_PASS_YDSSCRM_MAX': 0,
                'HOME_PASS_YDSSCRM_MIN': 0,
                'HOME_PASS_COMP_CAREER_MEAN': 0, 
                'HOME_PASS_COMP_CAREER_STD': 0,
                'HOME_PASS_COMP_CAREER_MAX': 0,
                'HOME_PASS_COMP_CAREER_MIN': 0,
                'HOME_PASS_ATT_CAREER_MEAN': 0, 
                'HOME_PASS_ATT_CAREER_STD': 0,
                'HOME_PASS_ATT_CAREER_MAX': 0,
                'HOME_PASS_ATT_CAREER_MIN': 0,
                'HOME_PASS_YDS_CAREER_MEAN': 0, 
                'HOME_PASS_YDS_CAREER_STD': 0,
                'HOME_PASS_YDS_CAREER_MAX': 0,
                'HOME_PASS_YDS_CAREER_MIN': 0,
                'HOME_PASS_1D_CAREER_MEAN': 0, 
                'HOME_PASS_1D_CAREER_STD': 0,
                'HOME_PASS_1D_CAREER_MAX': 0,
                'HOME_PASS_1D_CAREER_MIN': 0,
                'HOME_PASS_1DPCT_CAREER_MEAN': 0, 
                'HOME_PASS_1DPCT_CAREER_STD': 0,
                'HOME_PASS_1DPCT_CAREER_MAX': 0,
                'HOME_PASS_1DPCT_CAREER_MIN': 0,
                'HOME_PASS_IAY_CAREER_MEAN': 0, 
                'HOME_PASS_IAY_CAREER_STD': 0,
                'HOME_PASS_IAY_CAREER_MAX': 0,
                'HOME_PASS_IAY_CAREER_MIN': 0,
                'HOME_PASS_IAYPA_CAREER_MEAN': 0, 
                'HOME_PASS_IAYPA_CAREER_STD': 0,
                'HOME_PASS_IAYPA_CAREER_MAX': 0,
                'HOME_PASS_IAYPA_CAREER_MIN': 0,
                'HOME_PASS_CAY_CAREER_MEAN': 0, 
                'HOME_PASS_CAY_CAREER_STD': 0,
                'HOME_PASS_CAY_CAREER_MAX': 0,
                'HOME_PASS_CAY_CAREER_MIN': 0,
                'HOME_PASS_CAYCMP_CAREER_MEAN': 0, 
                'HOME_PASS_CAYCMP_CAREER_STD': 0,
                'HOME_PASS_CAYCMP_CAREER_MAX': 0,
                'HOME_PASS_CAYCMP_CAREER_MIN': 0,
                'HOME_PASS_CAYPA_CAREER_MEAN': 0, 
                'HOME_PASS_CAYPA_CAREER_STD': 0,
                'HOME_PASS_CAYPA_CAREER_MAX': 0,
                'HOME_PASS_CAYPA_CAREER_MIN': 0,
                'HOME_PASS_YAC_CAREER_MEAN': 0, 
                'HOME_PASS_YAC_CAREER_STD': 0,
                'HOME_PASS_YAC_CAREER_MAX': 0,
                'HOME_PASS_YAC_CAREER_MIN': 0,
                'HOME_PASS_YACCMP_CAREER_MEAN': 0, 
                'HOME_PASS_YACCMP_CAREER_STD': 0,
                'HOME_PASS_YACCMP_CAREER_MAX': 0,
                'HOME_PASS_YACCMP_CAREER_MIN': 0,
                'HOME_PASS_BADTH_CAREER_MEAN': 0, 
                'HOME_PASS_BADTH_CAREER_STD': 0,
                'HOME_PASS_BADTH_CAREER_MAX': 0,
                'HOME_PASS_BADTH_CAREER_MIN': 0,
                'HOME_PASS_SK_CAREER_MEAN': 0, 
                'HOME_PASS_SK_CAREER_STD': 0,
                'HOME_PASS_SK_CAREER_MAX': 0,
                'HOME_PASS_SK_CAREER_MIN': 0,
                'HOME_PASS_BLITZ_CAREER_MEAN': 0, 
                'HOME_PASS_BLITZ_CAREER_STD': 0,
                'HOME_PASS_BLITZ_CAREER_MAX': 0,
                'HOME_PASS_BLITZ_CAREER_MIN': 0,
                'HOME_PASS_HRRY_CAREER_MEAN': 0, 
                'HOME_PASS_HRRY_CAREER_STD': 0,
                'HOME_PASS_HRRY_CAREER_MAX': 0,
                'HOME_PASS_HRRY_CAREER_MIN': 0,
                'HOME_PASS_HITS_CAREER_MEAN': 0, 
                'HOME_PASS_HITS_CAREER_STD': 0,
                'HOME_PASS_HITS_CAREER_MAX': 0,
                'HOME_PASS_HITS_CAREER_MIN': 0,
                'HOME_PASS_PRSS_CAREER_MEAN': 0, 
                'HOME_PASS_PRSS_CAREER_STD': 0,
                'HOME_PASS_PRSS_CAREER_MAX': 0,
                'HOME_PASS_PRSS_CAREER_MIN': 0,
                'HOME_PASS_PRSSPCT_CAREER_MEAN': 0, 
                'HOME_PASS_PRSSPCT_CAREER_STD': 0,
                'HOME_PASS_PRSSPCT_CAREER_MAX': 0,
                'HOME_PASS_PRSSPCT_CAREER_MIN': 0,
                'HOME_PASS_BIG_GAME_W': 0,
                'HOME_PASS_BIG_GAME_L': 0,
                'HOME_PASS_PLAYOFF_W': 0,
                'HOME_PASS_PLAYOFF_L': 0,
                'HOME_PASS_CHAMP_W': 0,
                'HOME_PASS_CHAMP_L': 0,
                'Home-Team-Win': home_team_win,
                'DIV_MATCH': get_div_match(vis, home),
                'SCORE': int(AWAY_SCORE)+int(HOME_SCORE),
                'OU': float(OU),
                'OU_COVER': OU_COVER,
                'SPREAD': abs(float(SPREAD)),
                'SPREAD_COVER': SPREAD_COVER,
                'WIND_SPEED': WIND_SPEED,
                'TEMP': TEMP,
                'SURFACE': SURFACE,
                'ROOF': ROOF
            })

    season_data = pd.DataFrame(season_data, columns=game_table_headers)
    season_data.to_sql(f"games_{year}", games_con, if_exists="replace")
