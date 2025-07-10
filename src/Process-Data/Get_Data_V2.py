## V2 -> 
#   USE ADVANCED PASSING STATS AND SAVE AS PASSER ROW FOR INDIVIDUAL QB. USE AGGREGATE PASSER DATA TO FILL GAME DF FOR TRAINING
#   DO THE SAME FOR ADVANCED RUSHING, BUT NO NEED FOR INDIVIUDAL RUSHERS
#   GATHER PUNTING stats and save to table. like passer, use this to populate game dataframe up front
#   ASSESS STRENGTH OF SCHEDULE in games df; use teams db table to record this 
#   USE PLAYER SNAPS TABLE TO ASSESS THE NUMBER OF OFFENSIVE LINEMAN WHO HAVE PLAYED AT LEAST 50% OF SNAPS IN A SINGLE GAME
#   record spread, weather, ROOF, AND surface 

import traceback
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

import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")

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
years = [2023,2024]
# RANKS THROUGH 18 WEEKS; 18th is WC round until 2021
# weeks = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]

# games_con = sqlite3.connect("../../Data/v2/games.sqlite")
# teams_con = sqlite3.connect("../../Data/v2/teams.sqlite")
# teams_cursor = teams_con.cursor()
# passers_con = sqlite3.connect("../../Data/v2/passers.sqlite")
# player_snaps_con = sqlite3.connect("../../Data/v2/player_snaps.sqlite")
# player_snaps_cursor = player_snaps_con.cursor()

con = sqlite3.connect("../../Data/v2.sqlite")
cursor = con.cursor()

for year in years:
    if year > 2020:
        weeks = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]
    else:
        weeks = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
    for week in weeks:
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
                                AWAY_FD = int(vis_stat)
                                HOME_FD = int(home_stat)
                            elif column_name == "Rush-Yds-TDs":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                AWAY_RUSH_ATT = int(vis_split[0])
                                AWAY_RUSH_YDS = int(vis_split[1])
                                AWAY_RUSH_TD = int(vis_split[2])
                                HOME_RUSH_ATT = int(home_split[0])
                                HOME_RUSH_YDS = int(home_split[1])
                                HOME_RUSH_TD = int(home_split[2])
                            elif column_name == "Sacked-Yards":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                AWAY_SACKED = int(vis_split[0])
                                AWAY_SACKED_YDS = int(vis_split[1])
                                HOME_SACKED = int(home_split[0])
                                HOME_SACKED_YDS = int(home_split[1])

                            elif column_name == "Total Yards":
                                AWAY_TOTAL_YDS = int(vis_stat)
                                HOME_TOTAL_YDS = int(home_stat)
                            elif column_name == "Fumbles-Lost":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                AWAY_FUMBLES = int(vis_split[0])
                                AWAY_FUMBLES_LOST = int(vis_split[1])
                                HOME_FUMBLES = int(home_split[0])
                                HOME_FUMBLES_LOST = int(home_split[1])

                            elif column_name == "Turnovers":
                                AWAY_TO = int(vis_stat)
                                HOME_TO = int(home_stat)
                            elif column_name == "Penalties-Yards":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                AWAY_PENALTIES = int(vis_split[0])
                                AWAY_PENALTY_YARDS = int(vis_split[1])
                                HOME_PENALTIES = int(home_split[0])
                                HOME_PENALTY_YARDS = int(home_split[1])
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
                                print('column invalid, OR NOT USED: ', column_name)

                        if int(HOME_SCORE) > int(AWAY_SCORE):
                            home_team_win = 1.0 
                        else:
                            home_team_win = 0.0

                        
                        #####################################################
                        #####   BEGIN COLLECTING DATA FOR TEAM TABLES   #####
                        #####################################################

                        # to get sos, get the win pct of each team and use it for sos of the other team
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"teams_{year}\"", con)
                        if table.empty:
                            # if this table is empty, this data will never be available to any team
                            AWAY_FD_MEAN = -1 
                            AWAY_FD_STD = -1
                            AWAY_FD_MAX = -1
                            AWAY_FD_MIN = -1
                            AWAY_FD_AGAINST_MEAN = -1
                            AWAY_FD_AGAINST_STD = -1
                            AWAY_FD_AGAINST_MAX = -1
                            AWAY_FD_AGAINST_MIN = -1
                            AWAY_RUSH_ATT_MEAN = -1
                            AWAY_RUSH_ATT_STD = -1
                            AWAY_RUSH_ATT_MAX = -1
                            AWAY_RUSH_ATT_MIN = -1
                            AWAY_RUSH_YDS_MEAN = -1
                            AWAY_RUSH_YDS_STD = -1
                            AWAY_RUSH_YDS_MAX = -1
                            AWAY_RUSH_YDS_MIN = -1
                            AWAY_RUSH_TD_MEAN = -1
                            AWAY_RUSH_TD_STD = -1
                            AWAY_RUSH_TD_MAX = -1
                            AWAY_RUSH_TD_MIN = -1
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
                            HOME_RUSH_ATT_MEAN = -1
                            HOME_RUSH_ATT_STD = -1
                            HOME_RUSH_ATT_MAX = -1
                            HOME_RUSH_ATT_MIN = -1
                            HOME_RUSH_YDS_MEAN = -1
                            HOME_RUSH_YDS_STD = -1
                            HOME_RUSH_YDS_MAX = -1
                            HOME_RUSH_YDS_MIN = -1
                            HOME_RUSH_TD_MEAN = -1
                            HOME_RUSH_TD_STD = -1
                            HOME_RUSH_TD_MAX = -1
                            HOME_RUSH_TD_MIN = -1
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
                            away_df = pd.read_sql_query(f"SELECT * FROM teams_{year} WHERE (WEEK < {week}) and TEAM_NAME = '{vis}' order by WEEK desc;", con)
                            home_df = pd.read_sql_query(f"SELECT * FROM teams_{year} WHERE (WEEK < {week}) and TEAM_NAME = '{home}' order by WEEK desc;", con)

                            AWAY_FD_MEAN = away_df['FD'].astype(float).mean()
                            AWAY_FD_STD = away_df['FD'].astype(float).std()
                            AWAY_FD_MIN = away_df['FD'].astype(float).min()
                            AWAY_FD_MAX = away_df['FD'].astype(float).max()
                            AWAY_FD_AGAINST_MEAN = away_df['FD_AGAINST'].astype(float).mean()
                            AWAY_FD_AGAINST_STD = away_df['FD_AGAINST'].astype(float).std()
                            AWAY_FD_AGAINST_MAX = away_df['FD_AGAINST'].astype(float).max()
                            AWAY_FD_AGAINST_MIN = away_df['FD_AGAINST'].astype(float).min()
                            AWAY_RUSH_ATT_MEAN = away_df['RUSH_ATT'].astype(float).mean()
                            AWAY_RUSH_ATT_STD = away_df['RUSH_ATT'].astype(float).std()
                            AWAY_RUSH_ATT_MAX = away_df['RUSH_ATT'].astype(float).max()
                            AWAY_RUSH_ATT_MIN = away_df['RUSH_ATT'].astype(float).min()
                            AWAY_RUSH_YDS_MEAN = away_df['RUSH_YDS'].astype(float).mean()
                            AWAY_RUSH_YDS_STD = away_df['RUSH_YDS'].astype(float).std()
                            AWAY_RUSH_YDS_MAX = away_df['RUSH_YDS'].astype(float).max()
                            AWAY_RUSH_YDS_MIN = away_df['RUSH_YDS'].astype(float).min()
                            AWAY_RUSH_TD_MEAN = away_df['RUSH_TD'].astype(float).mean()
                            AWAY_RUSH_TD_STD = away_df['RUSH_TD'].astype(float).std()
                            AWAY_RUSH_TD_MAX = away_df['RUSH_TD'].astype(float).max()
                            AWAY_RUSH_TD_MIN = away_df['RUSH_TD'].astype(float).min()
                            AWAY_SACKS_MEAN = away_df['SACKS'].astype(float).mean() 
                            AWAY_SACKS_STD = away_df['SACKS'].astype(float).std()
                            AWAY_SACKS_MAX = away_df['SACKS'].astype(float).max()
                            AWAY_SACKS_MIN = away_df['SACKS'].astype(float).min()
                            AWAY_SACKS_AGAINST_MEAN = away_df['SACKS_AGAINST'].astype(float).mean() 
                            AWAY_SACKS_AGAINST_STD = away_df['SACKS_AGAINST'].astype(float).std()
                            AWAY_SACKS_AGAINST_MAX = away_df['SACKS_AGAINST'].astype(float).max()
                            AWAY_SACKS_AGAINST_MIN = away_df['SACKS_AGAINST'].astype(float).min()
                            AWAY_SACK_YDS_MEAN = away_df['SACK_YDS'].astype(float).mean() 
                            AWAY_SACK_YDS_STD = away_df['SACK_YDS'].astype(float).std()
                            AWAY_SACK_YDS_MAX = away_df['SACK_YDS'].astype(float).max()
                            AWAY_SACK_YDS_MIN = away_df['SACK_YDS'].astype(float).min()
                            AWAY_SACK_YDS_AGAINST_MEAN = away_df['SACK_YDS_AGAINST'].astype(float).mean() 
                            AWAY_SACK_YDS_AGAINST_STD = away_df['SACK_YDS_AGAINST'].astype(float).std()
                            AWAY_SACK_YDS_AGAINST_MAX = away_df['SACK_YDS_AGAINST'].astype(float).max()
                            AWAY_SACK_YDS_AGAINST_MIN = away_df['SACK_YDS_AGAINST'].astype(float).min()
                            AWAY_TOTAL_YDS_MEAN = away_df['TOTAL_YDS'].astype(float).mean() 
                            AWAY_TOTAL_YDS_STD = away_df['TOTAL_YDS'].astype(float).std()
                            AWAY_TOTAL_YDS_MAX = away_df['TOTAL_YDS'].astype(float).max()
                            AWAY_TOTAL_YDS_MIN = away_df['TOTAL_YDS'].astype(float).min()
                            AWAY_FUMBLES_MEAN = away_df['FUMBLES'].astype(float).mean() 
                            AWAY_FUMBLES_STD = away_df['FUMBLES'].astype(float).std()
                            AWAY_FUMBLES_MAX = away_df['FUMBLES'].astype(float).max()
                            AWAY_FUMBLES_MIN = away_df['FUMBLES'].astype(float).min()
                            AWAY_FUMBLES_LOST_MEAN = away_df['FUMBLES_LOST'].astype(float).mean() 
                            AWAY_FUMBLES_LOST_STD = away_df['FUMBLES_LOST'].astype(float).std()
                            AWAY_FUMBLES_LOST_MAX = away_df['FUMBLES_LOST'].astype(float).max()
                            AWAY_FUMBLES_LOST_MIN = away_df['FUMBLES_LOST'].astype(float).min()
                            AWAY_TO_MEAN = away_df['TO'].astype(float).mean() 
                            AWAY_TO_STD = away_df['TO'].astype(float).std()
                            AWAY_TO_MAX = away_df['TO'].astype(float).max()
                            AWAY_TO_MIN = away_df['TO'].astype(float).min()
                            AWAY_TO_AGAINST_MEAN = away_df['TO_AGAINST'].astype(float).mean() 
                            AWAY_TO_AGAINST_STD = away_df['TO_AGAINST'].astype(float).std()
                            AWAY_TO_AGAINST_MAX = away_df['TO_AGAINST'].astype(float).max()
                            AWAY_TO_AGAINST_MIN = away_df['TO_AGAINST'].astype(float).min()
                            AWAY_PENALTIES_MEAN = away_df['PENALTIES'].astype(float).mean() 
                            AWAY_PENALTIES_STD = away_df['PENALTIES'].astype(float).std()
                            AWAY_PENALTIES_MAX = away_df['PENALTIES'].astype(float).max()
                            AWAY_PENALTIES_MIN = away_df['PENALTIES'].astype(float).min()
                            AWAY_PENALTY_YARDS_MEAN = away_df['PENALTY_YARDS'].astype(float).mean() 
                            AWAY_PENALTY_YARDS_STD = away_df['PENALTY_YARDS'].astype(float).std()
                            AWAY_PENALTY_YARDS_MAX = away_df['PENALTY_YARDS'].astype(float).max()
                            AWAY_PENALTY_YARDS_MIN = away_df['PENALTY_YARDS'].astype(float).min()
                            AWAY_3RD_DOWN_MEAN = away_df['3RD_DOWN'].astype(float).mean() 
                            AWAY_3RD_DOWN_STD = away_df['3RD_DOWN'].astype(float).std()
                            AWAY_3RD_DOWN_MAX = away_df['3RD_DOWN'].astype(float).max()
                            AWAY_3RD_DOWN_MIN = away_df['3RD_DOWN'].astype(float).min()
                            AWAY_3RD_DOWN_ATT_MEAN = away_df['3RD_DOWN_ATT'].astype(float).mean() 
                            AWAY_3RD_DOWN_ATT_STD = away_df['3RD_DOWN_ATT'].astype(float).std()
                            AWAY_3RD_DOWN_ATT_MAX = away_df['3RD_DOWN_ATT'].astype(float).max()
                            AWAY_3RD_DOWN_ATT_MIN = away_df['3RD_DOWN_ATT'].astype(float).min()
                            AWAY_3RD_DOWN_CONV_MEAN = away_df['3RD_CONV'].astype(float).mean() 
                            AWAY_3RD_DOWN_CONV_STD = away_df['3RD_CONV'].astype(float).std()
                            AWAY_3RD_DOWN_CONV_MAX = away_df['3RD_CONV'].astype(float).max()
                            AWAY_3RD_DOWN_CONV_MIN = away_df['3RD_CONV'].astype(float).min()
                            AWAY_3RD_DOWN_AGAINST_MEAN = away_df['3RD_DOWN_AGAINST'].astype(float).mean()
                            AWAY_3RD_DOWN_AGAINST_STD = away_df['3RD_DOWN_AGAINST'].astype(float).std()
                            AWAY_3RD_DOWN_AGAINST_MAX = away_df['3RD_DOWN_AGAINST'].astype(float).max()
                            AWAY_3RD_DOWN_AGAINST_MIN = away_df['3RD_DOWN_AGAINST'].astype(float).min()
                            AWAY_3RD_DOWN_ATT_AGAINST_MEAN = away_df['3RD_DOWN_ATT_AGAINST'].astype(float).mean()
                            AWAY_3RD_DOWN_ATT_AGAINST_STD = away_df['3RD_DOWN_ATT_AGAINST'].astype(float).std()
                            AWAY_3RD_DOWN_ATT_AGAINST_MAX = away_df['3RD_DOWN_ATT_AGAINST'].astype(float).max()
                            AWAY_3RD_DOWN_ATT_AGAINST_MIN = away_df['3RD_DOWN_ATT_AGAINST'].astype(float).min()
                            AWAY_3RD_DOWN_CONV_AGAINST_MEAN = away_df['3RD_CONV_AGAINST'].astype(float).mean()
                            AWAY_3RD_DOWN_CONV_AGAINST_STD = away_df['3RD_CONV_AGAINST'].astype(float).std()
                            AWAY_3RD_DOWN_CONV_AGAINST_MAX = away_df['3RD_CONV_AGAINST'].astype(float).max()
                            AWAY_3RD_DOWN_CONV_AGAINST_MIN = away_df['3RD_CONV_AGAINST'].astype(float).min()
                            AWAY_TOP_MEAN = away_df['TOP'].astype(float).mean() 
                            AWAY_TOP_STD = away_df['TOP'].astype(float).std()
                            AWAY_TOP_MAX = away_df['TOP'].astype(float).max()
                            AWAY_TOP_MIN = away_df['TOP'].astype(float).min() 
                            HOME_FD_MEAN = home_df['FD'].astype(float).mean()
                            HOME_FD_STD = home_df['FD'].astype(float).std()
                            HOME_FD_MIN = home_df['FD'].astype(float).min()
                            HOME_FD_MAX = home_df['FD'].astype(float).max()
                            HOME_FD_AGAINST_MEAN = home_df['FD_AGAINST'].astype(float).mean()
                            HOME_FD_AGAINST_STD = home_df['FD_AGAINST'].astype(float).std()
                            HOME_FD_AGAINST_MAX = home_df['FD_AGAINST'].astype(float).max()
                            HOME_FD_AGAINST_MIN = home_df['FD_AGAINST'].astype(float).min()
                            HOME_RUSH_ATT_MEAN = home_df['RUSH_ATT'].astype(float).mean()
                            HOME_RUSH_ATT_STD = home_df['RUSH_ATT'].astype(float).std()
                            HOME_RUSH_ATT_MAX = home_df['RUSH_ATT'].astype(float).max()
                            HOME_RUSH_ATT_MIN = home_df['RUSH_ATT'].astype(float).min()
                            HOME_RUSH_YDS_MEAN = home_df['RUSH_YDS'].astype(float).mean()
                            HOME_RUSH_YDS_STD = home_df['RUSH_YDS'].astype(float).std()
                            HOME_RUSH_YDS_MAX = home_df['RUSH_YDS'].astype(float).max()
                            HOME_RUSH_YDS_MIN = home_df['RUSH_YDS'].astype(float).min()
                            HOME_RUSH_TD_MEAN = home_df['RUSH_TD'].astype(float).mean()
                            HOME_RUSH_TD_STD = home_df['RUSH_TD'].astype(float).std()
                            HOME_RUSH_TD_MAX = home_df['RUSH_TD'].astype(float).max()
                            HOME_RUSH_TD_MIN = home_df['RUSH_TD'].astype(float).min()
                            HOME_SACKS_MEAN = home_df['SACKS'].astype(float).mean() 
                            HOME_SACKS_STD = home_df['SACKS'].astype(float).std()
                            HOME_SACKS_MAX = home_df['SACKS'].astype(float).max()
                            HOME_SACKS_MIN = home_df['SACKS'].astype(float).min()
                            HOME_SACKS_AGAINST_MEAN = home_df['SACKS_AGAINST'].astype(float).mean() 
                            HOME_SACKS_AGAINST_STD = home_df['SACKS_AGAINST'].astype(float).std()
                            HOME_SACKS_AGAINST_MAX = home_df['SACKS_AGAINST'].astype(float).max()
                            HOME_SACKS_AGAINST_MIN = home_df['SACKS_AGAINST'].astype(float).min()
                            HOME_SACK_YDS_MEAN = home_df['SACK_YDS'].astype(float).mean() 
                            HOME_SACK_YDS_STD = home_df['SACK_YDS'].astype(float).std()
                            HOME_SACK_YDS_MAX = home_df['SACK_YDS'].astype(float).max()
                            HOME_SACK_YDS_MIN = home_df['SACK_YDS'].astype(float).min()
                            HOME_SACK_YDS_AGAINST_MEAN = home_df['SACK_YDS_AGAINST'].astype(float).mean() 
                            HOME_SACK_YDS_AGAINST_STD = home_df['SACK_YDS_AGAINST'].astype(float).std()
                            HOME_SACK_YDS_AGAINST_MAX = home_df['SACK_YDS_AGAINST'].astype(float).max()
                            HOME_SACK_YDS_AGAINST_MIN = home_df['SACK_YDS_AGAINST'].astype(float).min()
                            HOME_TOTAL_YDS_MEAN = home_df['TOTAL_YDS'].astype(float).mean() 
                            HOME_TOTAL_YDS_STD = home_df['TOTAL_YDS'].astype(float).std()
                            HOME_TOTAL_YDS_MAX = home_df['TOTAL_YDS'].astype(float).max()
                            HOME_TOTAL_YDS_MIN = home_df['TOTAL_YDS'].astype(float).min()
                            HOME_FUMBLES_MEAN = home_df['FUMBLES'].astype(float).mean() 
                            HOME_FUMBLES_STD = home_df['FUMBLES'].astype(float).std()
                            HOME_FUMBLES_MAX = home_df['FUMBLES'].astype(float).max()
                            HOME_FUMBLES_MIN = home_df['FUMBLES'].astype(float).min()
                            HOME_FUMBLES_LOST_MEAN = home_df['FUMBLES_LOST'].astype(float).mean() 
                            HOME_FUMBLES_LOST_STD = home_df['FUMBLES_LOST'].astype(float).std()
                            HOME_FUMBLES_LOST_MAX = home_df['FUMBLES_LOST'].astype(float).max()
                            HOME_FUMBLES_LOST_MIN = home_df['FUMBLES_LOST'].astype(float).min()
                            HOME_TO_MEAN = home_df['TO'].astype(float).mean() 
                            HOME_TO_STD = home_df['TO'].astype(float).std()
                            HOME_TO_MAX = home_df['TO'].astype(float).max()
                            HOME_TO_MIN = home_df['TO'].astype(float).min()
                            HOME_TO_AGAINST_MEAN = home_df['TO_AGAINST'].astype(float).mean() 
                            HOME_TO_AGAINST_STD = home_df['TO_AGAINST'].astype(float).std()
                            HOME_TO_AGAINST_MAX = home_df['TO_AGAINST'].astype(float).max()
                            HOME_TO_AGAINST_MIN = home_df['TO_AGAINST'].astype(float).min()
                            HOME_PENALTIES_MEAN = home_df['PENALTIES'].astype(float).mean() 
                            HOME_PENALTIES_STD = home_df['PENALTIES'].astype(float).std()
                            HOME_PENALTIES_MAX = home_df['PENALTIES'].astype(float).max()
                            HOME_PENALTIES_MIN = home_df['PENALTIES'].astype(float).min()
                            HOME_PENALTY_YARDS_MEAN = home_df['PENALTY_YARDS'].astype(float).mean() 
                            HOME_PENALTY_YARDS_STD = home_df['PENALTY_YARDS'].astype(float).std()
                            HOME_PENALTY_YARDS_MAX = home_df['PENALTY_YARDS'].astype(float).max()
                            HOME_PENALTY_YARDS_MIN = home_df['PENALTY_YARDS'].astype(float).min()
                            HOME_3RD_DOWN_MEAN = home_df['3RD_DOWN'].astype(float).mean() 
                            HOME_3RD_DOWN_STD = home_df['3RD_DOWN'].astype(float).std()
                            HOME_3RD_DOWN_MAX = home_df['3RD_DOWN'].astype(float).max()
                            HOME_3RD_DOWN_MIN = home_df['3RD_DOWN'].astype(float).min()
                            HOME_3RD_DOWN_ATT_MEAN = home_df['3RD_DOWN_ATT'].astype(float).mean() 
                            HOME_3RD_DOWN_ATT_STD = home_df['3RD_DOWN_ATT'].astype(float).std()
                            HOME_3RD_DOWN_ATT_MAX = home_df['3RD_DOWN_ATT'].astype(float).max()
                            HOME_3RD_DOWN_ATT_MIN = home_df['3RD_DOWN_ATT'].astype(float).min()
                            HOME_3RD_DOWN_CONV_MEAN = home_df['3RD_CONV'].astype(float).mean() 
                            HOME_3RD_DOWN_CONV_STD = home_df['3RD_CONV'].astype(float).std()
                            HOME_3RD_DOWN_CONV_MAX = home_df['3RD_CONV'].astype(float).max()
                            HOME_3RD_DOWN_CONV_MIN = home_df['3RD_CONV'].astype(float).min()
                            HOME_3RD_DOWN_AGAINST_MEAN = home_df['3RD_DOWN_AGAINST'].astype(float).mean()
                            HOME_3RD_DOWN_AGAINST_STD = home_df['3RD_DOWN_AGAINST'].astype(float).std()
                            HOME_3RD_DOWN_AGAINST_MAX = home_df['3RD_DOWN_AGAINST'].astype(float).max()
                            HOME_3RD_DOWN_AGAINST_MIN = home_df['3RD_DOWN_AGAINST'].astype(float).min()
                            HOME_3RD_DOWN_ATT_AGAINST_MEAN = home_df['3RD_DOWN_ATT_AGAINST'].astype(float).mean()
                            HOME_3RD_DOWN_ATT_AGAINST_STD = home_df['3RD_DOWN_ATT_AGAINST'].astype(float).std()
                            HOME_3RD_DOWN_ATT_AGAINST_MAX = home_df['3RD_DOWN_ATT_AGAINST'].astype(float).max()
                            HOME_3RD_DOWN_ATT_AGAINST_MIN = home_df['3RD_DOWN_ATT_AGAINST'].astype(float).min()
                            HOME_3RD_DOWN_CONV_AGAINST_MEAN = home_df['3RD_CONV_AGAINST'].astype(float).mean()
                            HOME_3RD_DOWN_CONV_AGAINST_STD = home_df['3RD_CONV_AGAINST'].astype(float).std()
                            HOME_3RD_DOWN_CONV_AGAINST_MAX = home_df['3RD_CONV_AGAINST'].astype(float).max()
                            HOME_3RD_DOWN_CONV_AGAINST_MIN = home_df['3RD_CONV_AGAINST'].astype(float).min()
                            HOME_TOP_MEAN = home_df['TOP'].astype(float).mean() 
                            HOME_TOP_STD = home_df['TOP'].astype(float).std()
                            HOME_TOP_MAX = home_df['TOP'].astype(float).max()
                            HOME_TOP_MIN = home_df['TOP'].astype(float).min()

                        # now append the data, so it doesn't mess up the historical load
                        vis_team_data = [{
                            'SEASON':year,
                            'WEEK':week,
                            'WIN': 0.0 if home_team_win else 1.0,
                            'TEAM_NAME':vis,
                            'SCORE':AWAY_SCORE,
                            'TOP':AWAY_TOP, 
                            'FD':AWAY_FD,
                            'RUSH_ATT': AWAY_RUSH_ATT,
                            'RUSH_YDS': AWAY_RUSH_YDS,
                            'RUSH_TD': AWAY_RUSH_TD,
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
                        }]
                        vis_team_data = pd.DataFrame(vis_team_data, columns=team_table_headers)
                        vis_team_data.to_sql(f"teams_{year}", con, if_exists="append")

                        home_team_data = [{
                            'SEASON':year,
                            'WEEK':week,
                            'WIN':home_team_win,
                            'TEAM_NAME':home,
                            'SCORE':HOME_SCORE,
                            'TOP':HOME_TOP, 
                            'FD':HOME_FD,
                            'RUSH_ATT': HOME_RUSH_ATT,
                            'RUSH_YDS': HOME_RUSH_YDS,
                            'RUSH_TD': HOME_RUSH_TD,
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
                        }]

                        home_team_data = pd.DataFrame(home_team_data, columns=team_table_headers)
                        home_team_data.to_sql(f"teams_{year}", con, if_exists="append")

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

                        # If season passing table is empty, we want to pass -1 into each variable for the game stats
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"passers_{year}\"", con)
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
                            HOME_PASS_COMP_MEAN = -1
                            HOME_PASS_COMP_STD = -1
                            HOME_PASS_COMP_MAX = -1
                            HOME_PASS_COMP_MIN = -1
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
                            
                        else:
                            # if the table is not empty, take the mean, std, max and min from each of the RELEVANT VALues
                            vis_pass_q = visiting_passer['Player']
                            home_pass_q = home_passer['Player']

                            # if vis_pass_q == "Aidan O'Connell":
                            #     vis_pass_q = "Aidan O'\Connell"
                            # if home_pass_q == "Aidan O'Connell":
                            #     home_pass_q = "Aidan O'\Connell"

                            away_passer_df = pd.read_sql_query(f'SELECT * FROM passers_{year} WHERE (WEEK < {week}) and Player = "{vis_pass_q}" order by WEEK desc;', con)
                            home_passer_df = pd.read_sql_query(f'SELECT * FROM passers_{year} WHERE (WEEK < {week}) and Player = "{home_pass_q}" order by WEEK desc;', con)

                            AWAY_PASS_COMP_MEAN = away_passer_df['Cmp'].astype(float).mean()
                            AWAY_PASS_COMP_STD = away_passer_df['Cmp'].astype(float).std()
                            AWAY_PASS_COMP_MAX = away_passer_df['Cmp'].astype(float).max()
                            AWAY_PASS_COMP_MIN = away_passer_df['Cmp'].astype(float).min()
                            AWAY_PASS_ATT_MEAN = away_passer_df['Att'].astype(float).mean() 
                            AWAY_PASS_ATT_STD = away_passer_df['Att'].astype(float).std()
                            AWAY_PASS_ATT_MAX = away_passer_df['Att'].astype(float).max()
                            AWAY_PASS_ATT_MIN = away_passer_df['Att'].astype(float).min()
                            AWAY_PASS_YDS_MEAN = away_passer_df['Yds'].astype(float).mean() 
                            AWAY_PASS_YDS_STD = away_passer_df['Yds'].astype(float).std()
                            AWAY_PASS_YDS_MAX = away_passer_df['Yds'].astype(float).max()
                            AWAY_PASS_YDS_MIN = away_passer_df['Yds'].astype(float).min()
                            AWAY_PASS_1D_MEAN = away_passer_df['1D'].astype(float).mean() 
                            AWAY_PASS_1D_STD = away_passer_df['1D'].astype(float).std()
                            AWAY_PASS_1D_MAX = away_passer_df['1D'].astype(float).max()
                            AWAY_PASS_1D_MIN = away_passer_df['1D'].astype(float).min()
                            AWAY_PASS_1DPCT_MEAN = away_passer_df['1D%'].astype(float).mean() 
                            AWAY_PASS_1DPCT_STD = away_passer_df['1D%'].astype(float).std()
                            AWAY_PASS_1DPCT_MAX = away_passer_df['1D%'].astype(float).max()
                            AWAY_PASS_1DPCT_MIN = away_passer_df['1D%'].astype(float).min()
                            AWAY_PASS_IAY_MEAN = away_passer_df['IAY'].astype(float).mean() 
                            AWAY_PASS_IAY_STD = away_passer_df['IAY'].astype(float).std()
                            AWAY_PASS_IAY_MAX = away_passer_df['IAY'].astype(float).max()
                            AWAY_PASS_IAY_MIN = away_passer_df['IAY'].astype(float).min()
                            AWAY_PASS_IAYPA_MEAN = away_passer_df['IAY/PA'].astype(float).mean() 
                            AWAY_PASS_IAYPA_STD = away_passer_df['IAY/PA'].astype(float).std()
                            AWAY_PASS_IAYPA_MAX = away_passer_df['IAY/PA'].astype(float).max()
                            AWAY_PASS_IAYPA_MIN = away_passer_df['IAY/PA'].astype(float).min()
                            AWAY_PASS_CAY_MEAN = away_passer_df['CAY'].astype(float).mean() 
                            AWAY_PASS_CAY_STD = away_passer_df['CAY'].astype(float).std()
                            AWAY_PASS_CAY_MAX = away_passer_df['CAY'].astype(float).max()
                            AWAY_PASS_CAY_MIN = away_passer_df['CAY'].astype(float).min()
                            AWAY_PASS_CAYCMP_MEAN = away_passer_df['CAY/Cmp'].astype(float).mean() 
                            AWAY_PASS_CAYCMP_STD = away_passer_df['CAY/Cmp'].astype(float).std()
                            AWAY_PASS_CAYCMP_MAX = away_passer_df['CAY/Cmp'].astype(float).max()
                            AWAY_PASS_CAYCMP_MIN = away_passer_df['CAY/Cmp'].astype(float).min()
                            AWAY_PASS_CAYPA_MEAN = away_passer_df['CAY/PA'].astype(float).mean() 
                            AWAY_PASS_CAYPA_STD = away_passer_df['CAY/PA'].astype(float).std()
                            AWAY_PASS_CAYPA_MAX = away_passer_df['CAY/PA'].astype(float).max()
                            AWAY_PASS_CAYPA_MIN = away_passer_df['CAY/PA'].astype(float).min()
                            AWAY_PASS_YAC_MEAN = away_passer_df['YAC'].astype(float).mean() 
                            AWAY_PASS_YAC_STD = away_passer_df['YAC'].astype(float).std()
                            AWAY_PASS_YAC_MAX = away_passer_df['YAC'].astype(float).max()
                            AWAY_PASS_YAC_MIN = away_passer_df['YAC'].astype(float).min()
                            AWAY_PASS_YACCMP_MEAN = away_passer_df['YAC/Cmp'].astype(float).mean() 
                            AWAY_PASS_YACCMP_STD = away_passer_df['YAC/Cmp'].astype(float).std()
                            AWAY_PASS_YACCMP_MAX = away_passer_df['YAC/Cmp'].astype(float).max()
                            AWAY_PASS_YACCMP_MIN = away_passer_df['YAC/Cmp'].astype(float).min()
                            AWAY_PASS_DROPS_MEAN = away_passer_df['Drops'].astype(float).mean() 
                            AWAY_PASS_DROPS_STD = away_passer_df['Drops'].astype(float).std()
                            AWAY_PASS_DROPS_MAX = away_passer_df['Drops'].astype(float).max()
                            AWAY_PASS_DROPS_MIN = away_passer_df['Drops'].astype(float).min()
                            AWAY_PASS_DROPPCT_MEAN = away_passer_df['Drop%'].astype(float).mean() 
                            AWAY_PASS_DROPPCT_STD = away_passer_df['Drop%'].astype(float).std()
                            AWAY_PASS_DROPPCT_MAX = away_passer_df['Drop%'].astype(float).max()
                            AWAY_PASS_DROPPCT_MIN = away_passer_df['Drop%'].astype(float).min()
                            AWAY_PASS_BADTH_MEAN = away_passer_df['BadTh'].astype(float).mean() 
                            AWAY_PASS_BADTH_STD = away_passer_df['BadTh'].astype(float).std()
                            AWAY_PASS_BADTH_MAX = away_passer_df['BadTh'].astype(float).max()
                            AWAY_PASS_BADTH_MIN = away_passer_df['BadTh'].astype(float).min()
                            AWAY_PASS_SK_MEAN = away_passer_df['Sk'].astype(float).mean() 
                            AWAY_PASS_SK_STD = away_passer_df['Sk'].astype(float).std()
                            AWAY_PASS_SK_MAX = away_passer_df['Sk'].astype(float).max()
                            AWAY_PASS_SK_MIN = away_passer_df['Sk'].astype(float).min()
                            AWAY_PASS_BLITZ_MEAN = away_passer_df['Bltz'].astype(float).mean() 
                            AWAY_PASS_BLITZ_STD = away_passer_df['Bltz'].astype(float).std()
                            AWAY_PASS_BLITZ_MAX = away_passer_df['Bltz'].astype(float).max()
                            AWAY_PASS_BLITZ_MIN = away_passer_df['Bltz'].astype(float).min()
                            AWAY_PASS_HRRY_MEAN = away_passer_df['Hrry'].astype(float).mean() 
                            AWAY_PASS_HRRY_STD = away_passer_df['Hrry'].astype(float).std()
                            AWAY_PASS_HRRY_MAX = away_passer_df['Hrry'].astype(float).max()
                            AWAY_PASS_HRRY_MIN = away_passer_df['Hrry'].astype(float).min()
                            AWAY_PASS_HITS_MEAN = away_passer_df['Hits'].astype(float).mean() 
                            AWAY_PASS_HITS_STD = away_passer_df['Hits'].astype(float).std()
                            AWAY_PASS_HITS_MAX = away_passer_df['Hits'].astype(float).max()
                            AWAY_PASS_HITS_MIN = away_passer_df['Hits'].astype(float).min()
                            AWAY_PASS_PRSS_MEAN = away_passer_df['Prss'].astype(float).mean() 
                            AWAY_PASS_PRSS_STD = away_passer_df['Prss'].astype(float).std()
                            AWAY_PASS_PRSS_MAX = away_passer_df['Prss'].astype(float).max()
                            AWAY_PASS_PRSS_MIN = away_passer_df['Prss'].astype(float).min()
                            AWAY_PASS_PRSSPCT_MEAN = away_passer_df['Prss%'].astype(float).mean() 
                            AWAY_PASS_PRSSPCT_STD = away_passer_df['Prss%'].astype(float).std()
                            AWAY_PASS_PRSSPCT_MAX = away_passer_df['Prss%'].astype(float).max()
                            AWAY_PASS_PRSSPCT_MIN = away_passer_df['Prss%'].astype(float).min()
                            AWAY_PASS_SCRM_MEAN = away_passer_df['Scrm'].astype(float).mean() 
                            AWAY_PASS_SCRM_STD = away_passer_df['Scrm'].astype(float).std()
                            AWAY_PASS_SCRM_MAX = away_passer_df['Scrm'].astype(float).max()
                            AWAY_PASS_SCRM_MIN = away_passer_df['Scrm'].astype(float).min()
                            AWAY_PASS_YDSSCRM_MEAN = away_passer_df['Yds/Scr'].astype(float).mean() 
                            AWAY_PASS_YDSSCRM_STD = away_passer_df['Yds/Scr'].astype(float).std()
                            AWAY_PASS_YDSSCRM_MAX = away_passer_df['Yds/Scr'].astype(float).max()
                            AWAY_PASS_YDSSCRM_MIN = away_passer_df['Yds/Scr'].astype(float).min()
                            HOME_PASS_COMP_MEAN = home_passer_df['Cmp'].astype(float).mean()
                            HOME_PASS_COMP_STD = home_passer_df['Cmp'].astype(float).std()
                            HOME_PASS_COMP_MAX = home_passer_df['Cmp'].astype(float).max()
                            HOME_PASS_COMP_MIN = home_passer_df['Cmp'].astype(float).min()
                            HOME_PASS_ATT_MEAN = home_passer_df['Att'].astype(float).mean() 
                            HOME_PASS_ATT_STD = home_passer_df['Att'].astype(float).std()
                            HOME_PASS_ATT_MAX = home_passer_df['Att'].astype(float).max()
                            HOME_PASS_ATT_MIN = home_passer_df['Att'].astype(float).min()
                            HOME_PASS_YDS_MEAN = home_passer_df['Yds'].astype(float).mean() 
                            HOME_PASS_YDS_STD = home_passer_df['Yds'].astype(float).std()
                            HOME_PASS_YDS_MAX = home_passer_df['Yds'].astype(float).max()
                            HOME_PASS_YDS_MIN = home_passer_df['Yds'].astype(float).min()
                            HOME_PASS_1D_MEAN = home_passer_df['1D'].astype(float).mean() 
                            HOME_PASS_1D_STD = home_passer_df['1D'].astype(float).std()
                            HOME_PASS_1D_MAX = home_passer_df['1D'].astype(float).max()
                            HOME_PASS_1D_MIN = home_passer_df['1D'].astype(float).min()
                            HOME_PASS_1DPCT_MEAN = home_passer_df['1D%'].astype(float).mean() 
                            HOME_PASS_1DPCT_STD = home_passer_df['1D%'].astype(float).std()
                            HOME_PASS_1DPCT_MAX = home_passer_df['1D%'].astype(float).max()
                            HOME_PASS_1DPCT_MIN = home_passer_df['1D%'].astype(float).min()
                            HOME_PASS_IAY_MEAN = home_passer_df['IAY'].astype(float).mean() 
                            HOME_PASS_IAY_STD = home_passer_df['IAY'].astype(float).std()
                            HOME_PASS_IAY_MAX = home_passer_df['IAY'].astype(float).max()
                            HOME_PASS_IAY_MIN = home_passer_df['IAY'].astype(float).min()
                            HOME_PASS_IAYPA_MEAN = home_passer_df['IAY/PA'].astype(float).mean() 
                            HOME_PASS_IAYPA_STD = home_passer_df['IAY/PA'].astype(float).std()
                            HOME_PASS_IAYPA_MAX = home_passer_df['IAY/PA'].astype(float).max()
                            HOME_PASS_IAYPA_MIN = home_passer_df['IAY/PA'].astype(float).min()
                            HOME_PASS_CAY_MEAN = home_passer_df['CAY'].astype(float).mean() 
                            HOME_PASS_CAY_STD = home_passer_df['CAY'].astype(float).std()
                            HOME_PASS_CAY_MAX = home_passer_df['CAY'].astype(float).max()
                            HOME_PASS_CAY_MIN = home_passer_df['CAY'].astype(float).min()
                            HOME_PASS_CAYCMP_MEAN = home_passer_df['CAY/Cmp'].astype(float).mean() 
                            HOME_PASS_CAYCMP_STD = home_passer_df['CAY/Cmp'].astype(float).std()
                            HOME_PASS_CAYCMP_MAX = home_passer_df['CAY/Cmp'].astype(float).max()
                            HOME_PASS_CAYCMP_MIN = home_passer_df['CAY/Cmp'].astype(float).min()
                            HOME_PASS_CAYPA_MEAN = home_passer_df['CAY/PA'].astype(float).mean() 
                            HOME_PASS_CAYPA_STD = home_passer_df['CAY/PA'].astype(float).std()
                            HOME_PASS_CAYPA_MAX = home_passer_df['CAY/PA'].astype(float).max()
                            HOME_PASS_CAYPA_MIN = home_passer_df['CAY/PA'].astype(float).min()
                            HOME_PASS_YAC_MEAN = home_passer_df['YAC'].astype(float).mean() 
                            HOME_PASS_YAC_STD = home_passer_df['YAC'].astype(float).std()
                            HOME_PASS_YAC_MAX = home_passer_df['YAC'].astype(float).max()
                            HOME_PASS_YAC_MIN = home_passer_df['YAC'].astype(float).min()
                            HOME_PASS_YACCMP_MEAN = home_passer_df['YAC/Cmp'].astype(float).mean() 
                            HOME_PASS_YACCMP_STD = home_passer_df['YAC/Cmp'].astype(float).std()
                            HOME_PASS_YACCMP_MAX = home_passer_df['YAC/Cmp'].astype(float).max()
                            HOME_PASS_YACCMP_MIN = home_passer_df['YAC/Cmp'].astype(float).min()
                            HOME_PASS_DROPS_MEAN = home_passer_df['Drops'].astype(float).mean() 
                            HOME_PASS_DROPS_STD = home_passer_df['Drops'].astype(float).std()
                            HOME_PASS_DROPS_MAX = home_passer_df['Drops'].astype(float).max()
                            HOME_PASS_DROPS_MIN = home_passer_df['Drops'].astype(float).min()
                            HOME_PASS_DROPPCT_MEAN = home_passer_df['Drop%'].astype(float).mean() 
                            HOME_PASS_DROPPCT_STD = home_passer_df['Drop%'].astype(float).std()
                            HOME_PASS_DROPPCT_MAX = home_passer_df['Drop%'].astype(float).max()
                            HOME_PASS_DROPPCT_MIN = home_passer_df['Drop%'].astype(float).min()
                            HOME_PASS_BADTH_MEAN = home_passer_df['BadTh'].astype(float).mean() 
                            HOME_PASS_BADTH_STD = home_passer_df['BadTh'].astype(float).std()
                            HOME_PASS_BADTH_MAX = home_passer_df['BadTh'].astype(float).max()
                            HOME_PASS_BADTH_MIN = home_passer_df['BadTh'].astype(float).min()
                            HOME_PASS_SK_MEAN = home_passer_df['Sk'].astype(float).mean() 
                            HOME_PASS_SK_STD = home_passer_df['Sk'].astype(float).std()
                            HOME_PASS_SK_MAX = home_passer_df['Sk'].astype(float).max()
                            HOME_PASS_SK_MIN = home_passer_df['Sk'].astype(float).min()
                            HOME_PASS_BLITZ_MEAN = home_passer_df['Bltz'].astype(float).mean() 
                            HOME_PASS_BLITZ_STD = home_passer_df['Bltz'].astype(float).std()
                            HOME_PASS_BLITZ_MAX = home_passer_df['Bltz'].astype(float).max()
                            HOME_PASS_BLITZ_MIN = home_passer_df['Bltz'].astype(float).min()
                            HOME_PASS_HRRY_MEAN = home_passer_df['Hrry'].astype(float).mean() 
                            HOME_PASS_HRRY_STD = home_passer_df['Hrry'].astype(float).std()
                            HOME_PASS_HRRY_MAX = home_passer_df['Hrry'].astype(float).max()
                            HOME_PASS_HRRY_MIN = home_passer_df['Hrry'].astype(float).min()
                            HOME_PASS_HITS_MEAN = home_passer_df['Hits'].astype(float).mean() 
                            HOME_PASS_HITS_STD = home_passer_df['Hits'].astype(float).std()
                            HOME_PASS_HITS_MAX = home_passer_df['Hits'].astype(float).max()
                            HOME_PASS_HITS_MIN = home_passer_df['Hits'].astype(float).min()
                            HOME_PASS_PRSS_MEAN = home_passer_df['Prss'].astype(float).mean() 
                            HOME_PASS_PRSS_STD = home_passer_df['Prss'].astype(float).std()
                            HOME_PASS_PRSS_MAX = home_passer_df['Prss'].astype(float).max()
                            HOME_PASS_PRSS_MIN = home_passer_df['Prss'].astype(float).min()
                            HOME_PASS_PRSSPCT_MEAN = home_passer_df['Prss%'].astype(float).mean() 
                            HOME_PASS_PRSSPCT_STD = home_passer_df['Prss%'].astype(float).std()
                            HOME_PASS_PRSSPCT_MAX = home_passer_df['Prss%'].astype(float).max()
                            HOME_PASS_PRSSPCT_MIN = home_passer_df['Prss%'].astype(float).min()
                            HOME_PASS_SCRM_MEAN = home_passer_df['Scrm'].astype(float).mean() 
                            HOME_PASS_SCRM_STD = home_passer_df['Scrm'].astype(float).std()
                            HOME_PASS_SCRM_MAX = home_passer_df['Scrm'].astype(float).max()
                            HOME_PASS_SCRM_MIN = home_passer_df['Scrm'].astype(float).min()
                            HOME_PASS_YDSSCRM_MEAN = home_passer_df['Yds/Scr'].astype(float).mean() 
                            HOME_PASS_YDSSCRM_STD = home_passer_df['Yds/Scr'].astype(float).std()
                            HOME_PASS_YDSSCRM_MAX = home_passer_df['Yds/Scr'].astype(float).max()
                            HOME_PASS_YDSSCRM_MIN = home_passer_df['Yds/Scr'].astype(float).min()

                        # If CAREER passing table is empty, we want to pass passer's career stats from 2018 to present day
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='passers_2018-present'", con)
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
                            HOME_PASS_COMP_CAREER_MEAN = -1
                            HOME_PASS_COMP_CAREER_STD = -1
                            HOME_PASS_COMP_CAREER_MAX = -1
                            HOME_PASS_COMP_CAREER_MIN = -1
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
                            vis_pass_q = visiting_passer['Player']
                            home_pass_q = home_passer['Player']

                            # if vis_pass_q == "Aidan O'Connell":
                            #     vis_pass_q = "Aidan O'\Connell"
                            # if home_pass_q == "Aidan O'Connell":
                            #     home_pass_q = "Aidan O'\Connell"
                            # if the table is not empty, take the mean, std, max and min from each of the RELEVANT VALues
                            away_passer_career_df = pd.read_sql_query(f'SELECT * FROM `passers_2018-present` WHERE Player = "{vis_pass_q}";', con)
                            home_passer_career_df = pd.read_sql_query(f'SELECT * FROM `passers_2018-present` WHERE Player = "{home_pass_q}";', con)

                            AWAY_PASS_COMP_CAREER_MEAN = away_passer_career_df['Cmp'].astype(float).mean()
                            AWAY_PASS_COMP_CAREER_STD = away_passer_career_df['Cmp'].astype(float).std()
                            AWAY_PASS_COMP_CAREER_MAX = away_passer_career_df['Cmp'].astype(float).max()
                            AWAY_PASS_COMP_CAREER_MIN = away_passer_career_df['Cmp'].astype(float).min()
                            AWAY_PASS_ATT_CAREER_MEAN = away_passer_career_df['Att'].astype(float).mean() 
                            AWAY_PASS_ATT_CAREER_STD = away_passer_career_df['Att'].astype(float).std()
                            AWAY_PASS_ATT_CAREER_MAX = away_passer_career_df['Att'].astype(float).max()
                            AWAY_PASS_ATT_CAREER_MIN = away_passer_career_df['Att'].astype(float).min()
                            AWAY_PASS_YDS_CAREER_MEAN = away_passer_career_df['Yds'].astype(float).mean() 
                            AWAY_PASS_YDS_CAREER_STD = away_passer_career_df['Yds'].astype(float).std()
                            AWAY_PASS_YDS_CAREER_MAX = away_passer_career_df['Yds'].astype(float).max()
                            AWAY_PASS_YDS_CAREER_MIN = away_passer_career_df['Yds'].astype(float).min()
                            AWAY_PASS_1D_CAREER_MEAN = away_passer_career_df['1D'].astype(float).mean() 
                            AWAY_PASS_1D_CAREER_STD = away_passer_career_df['1D'].astype(float).std()
                            AWAY_PASS_1D_CAREER_MAX = away_passer_career_df['1D'].astype(float).max()
                            AWAY_PASS_1D_CAREER_MIN = away_passer_career_df['1D'].astype(float).min()
                            AWAY_PASS_1DPCT_CAREER_MEAN = away_passer_career_df['1D%'].astype(float).mean() 
                            AWAY_PASS_1DPCT_CAREER_STD = away_passer_career_df['1D%'].astype(float).std()
                            AWAY_PASS_1DPCT_CAREER_MAX = away_passer_career_df['1D%'].astype(float).max()
                            AWAY_PASS_1DPCT_CAREER_MIN = away_passer_career_df['1D%'].astype(float).min()
                            AWAY_PASS_IAY_CAREER_MEAN = away_passer_career_df['IAY'].astype(float).mean() 
                            AWAY_PASS_IAY_CAREER_STD = away_passer_career_df['IAY'].astype(float).std()
                            AWAY_PASS_IAY_CAREER_MAX = away_passer_career_df['IAY'].astype(float).max()
                            AWAY_PASS_IAY_CAREER_MIN = away_passer_career_df['IAY'].astype(float).min()
                            AWAY_PASS_IAYPA_CAREER_MEAN = away_passer_career_df['IAY/PA'].astype(float).mean() 
                            AWAY_PASS_IAYPA_CAREER_STD = away_passer_career_df['IAY/PA'].astype(float).std()
                            AWAY_PASS_IAYPA_CAREER_MAX = away_passer_career_df['IAY/PA'].astype(float).max()
                            AWAY_PASS_IAYPA_CAREER_MIN = away_passer_career_df['IAY/PA'].astype(float).min()
                            AWAY_PASS_CAY_CAREER_MEAN = away_passer_career_df['CAY'].astype(float).mean() 
                            AWAY_PASS_CAY_CAREER_STD = away_passer_career_df['CAY'].astype(float).std()
                            AWAY_PASS_CAY_CAREER_MAX = away_passer_career_df['CAY'].astype(float).max()
                            AWAY_PASS_CAY_CAREER_MIN = away_passer_career_df['CAY'].astype(float).min()
                            AWAY_PASS_CAYCMP_CAREER_MEAN = away_passer_career_df['CAY/Cmp'].astype(float).mean() 
                            AWAY_PASS_CAYCMP_CAREER_STD = away_passer_career_df['CAY/Cmp'].astype(float).std()
                            AWAY_PASS_CAYCMP_CAREER_MAX = away_passer_career_df['CAY/Cmp'].astype(float).max()
                            AWAY_PASS_CAYCMP_CAREER_MIN = away_passer_career_df['CAY/Cmp'].astype(float).min()
                            AWAY_PASS_CAYPA_CAREER_MEAN = away_passer_career_df['CAY/PA'].astype(float).mean() 
                            AWAY_PASS_CAYPA_CAREER_STD = away_passer_career_df['CAY/PA'].astype(float).std()
                            AWAY_PASS_CAYPA_CAREER_MAX = away_passer_career_df['CAY/PA'].astype(float).max()
                            AWAY_PASS_CAYPA_CAREER_MIN = away_passer_career_df['CAY/PA'].astype(float).min()
                            AWAY_PASS_YAC_CAREER_MEAN = away_passer_career_df['YAC'].astype(float).mean() 
                            AWAY_PASS_YAC_CAREER_STD = away_passer_career_df['YAC'].astype(float).std()
                            AWAY_PASS_YAC_CAREER_MAX = away_passer_career_df['YAC'].astype(float).max()
                            AWAY_PASS_YAC_CAREER_MIN = away_passer_career_df['YAC'].astype(float).min()
                            AWAY_PASS_YACCMP_CAREER_MEAN = away_passer_career_df['YAC/Cmp'].astype(float).mean() 
                            AWAY_PASS_YACCMP_CAREER_STD = away_passer_career_df['YAC/Cmp'].astype(float).std()
                            AWAY_PASS_YACCMP_CAREER_MAX = away_passer_career_df['YAC/Cmp'].astype(float).max()
                            AWAY_PASS_YACCMP_CAREER_MIN = away_passer_career_df['YAC/Cmp'].astype(float).min()
                            AWAY_PASS_BADTH_CAREER_MEAN = away_passer_career_df['BadTh'].astype(float).mean() 
                            AWAY_PASS_BADTH_CAREER_STD = away_passer_career_df['BadTh'].astype(float).std()
                            AWAY_PASS_BADTH_CAREER_MAX = away_passer_career_df['BadTh'].astype(float).max()
                            AWAY_PASS_BADTH_CAREER_MIN = away_passer_career_df['BadTh'].astype(float).min()
                            AWAY_PASS_SK_CAREER_MEAN = away_passer_career_df['Sk'].astype(float).mean() 
                            AWAY_PASS_SK_CAREER_STD = away_passer_career_df['Sk'].astype(float).std()
                            AWAY_PASS_SK_CAREER_MAX = away_passer_career_df['Sk'].astype(float).max()
                            AWAY_PASS_SK_CAREER_MIN = away_passer_career_df['Sk'].astype(float).min()
                            AWAY_PASS_BLITZ_CAREER_MEAN = away_passer_career_df['Bltz'].astype(float).mean() 
                            AWAY_PASS_BLITZ_CAREER_STD = away_passer_career_df['Bltz'].astype(float).std()
                            AWAY_PASS_BLITZ_CAREER_MAX = away_passer_career_df['Bltz'].astype(float).max()
                            AWAY_PASS_BLITZ_CAREER_MIN = away_passer_career_df['Bltz'].astype(float).min()
                            AWAY_PASS_HRRY_CAREER_MEAN = away_passer_career_df['Hrry'].astype(float).mean() 
                            AWAY_PASS_HRRY_CAREER_STD = away_passer_career_df['Hrry'].astype(float).std()
                            AWAY_PASS_HRRY_CAREER_MAX = away_passer_career_df['Hrry'].astype(float).max()
                            AWAY_PASS_HRRY_CAREER_MIN = away_passer_career_df['Hrry'].astype(float).min()
                            AWAY_PASS_HITS_CAREER_MEAN = away_passer_career_df['Hits'].astype(float).mean() 
                            AWAY_PASS_HITS_CAREER_STD = away_passer_career_df['Hits'].astype(float).std()
                            AWAY_PASS_HITS_CAREER_MAX = away_passer_career_df['Hits'].astype(float).max()
                            AWAY_PASS_HITS_CAREER_MIN = away_passer_career_df['Hits'].astype(float).min()
                            AWAY_PASS_PRSS_CAREER_MEAN = away_passer_career_df['Prss'].astype(float).mean() 
                            AWAY_PASS_PRSS_CAREER_STD = away_passer_career_df['Prss'].astype(float).std()
                            AWAY_PASS_PRSS_CAREER_MAX = away_passer_career_df['Prss'].astype(float).max()
                            AWAY_PASS_PRSS_CAREER_MIN = away_passer_career_df['Prss'].astype(float).min()
                            AWAY_PASS_PRSSPCT_CAREER_MEAN = away_passer_career_df['Prss%'].astype(float).mean() 
                            AWAY_PASS_PRSSPCT_CAREER_STD = away_passer_career_df['Prss%'].astype(float).std()
                            AWAY_PASS_PRSSPCT_CAREER_MAX = away_passer_career_df['Prss%'].astype(float).max()
                            AWAY_PASS_PRSSPCT_CAREER_MIN = away_passer_career_df['Prss%'].astype(float).min()

                            AWAY_PASS_BIG_GAME_W = away_passer_career_df['BIG_GAME_W'].sum()
                            AWAY_PASS_BIG_GAME_L = away_passer_career_df['BIG_GAME_L'].sum()
                            AWAY_PASS_PLAYOFF_W = away_passer_career_df['PLAYOFF_W'].sum()
                            AWAY_PASS_PLAYOFF_L = away_passer_career_df['PLAYOFF_L'].sum()
                            AWAY_PASS_CHAMP_W = away_passer_career_df['CHAMP_W'].sum()
                            AWAY_PASS_CHAMP_L = away_passer_career_df['CHAMP_L'].sum()

                            HOME_PASS_COMP_CAREER_MEAN = home_passer_career_df['Cmp'].astype(float).mean()
                            HOME_PASS_COMP_CAREER_STD = home_passer_career_df['Cmp'].astype(float).std()
                            HOME_PASS_COMP_CAREER_MAX = home_passer_career_df['Cmp'].astype(float).min()
                            HOME_PASS_COMP_CAREER_MIN = home_passer_career_df['Cmp'].astype(float).max()
                            HOME_PASS_ATT_CAREER_MEAN = home_passer_career_df['Att'].astype(float).mean() 
                            HOME_PASS_ATT_CAREER_STD = home_passer_career_df['Att'].astype(float).std()
                            HOME_PASS_ATT_CAREER_MAX = home_passer_career_df['Att'].astype(float).max()
                            HOME_PASS_ATT_CAREER_MIN = home_passer_career_df['Att'].astype(float).min()
                            HOME_PASS_YDS_CAREER_MEAN = home_passer_career_df['Yds'].astype(float).mean() 
                            HOME_PASS_YDS_CAREER_STD = home_passer_career_df['Yds'].astype(float).std()
                            HOME_PASS_YDS_CAREER_MAX = home_passer_career_df['Yds'].astype(float).max()
                            HOME_PASS_YDS_CAREER_MIN = home_passer_career_df['Yds'].astype(float).min()
                            HOME_PASS_1D_CAREER_MEAN = home_passer_career_df['1D'].astype(float).mean() 
                            HOME_PASS_1D_CAREER_STD = home_passer_career_df['1D'].astype(float).std()
                            HOME_PASS_1D_CAREER_MAX = home_passer_career_df['1D'].astype(float).max()
                            HOME_PASS_1D_CAREER_MIN = home_passer_career_df['1D'].astype(float).min()
                            HOME_PASS_1DPCT_CAREER_MEAN = home_passer_career_df['1D%'].astype(float).mean() 
                            HOME_PASS_1DPCT_CAREER_STD = home_passer_career_df['1D%'].astype(float).std()
                            HOME_PASS_1DPCT_CAREER_MAX = home_passer_career_df['1D%'].astype(float).max()
                            HOME_PASS_1DPCT_CAREER_MIN = home_passer_career_df['1D%'].astype(float).min()
                            HOME_PASS_IAY_CAREER_MEAN = home_passer_career_df['IAY'].astype(float).mean() 
                            HOME_PASS_IAY_CAREER_STD = home_passer_career_df['IAY'].astype(float).std()
                            HOME_PASS_IAY_CAREER_MAX = home_passer_career_df['IAY'].astype(float).max()
                            HOME_PASS_IAY_CAREER_MIN = home_passer_career_df['IAY'].astype(float).min()
                            HOME_PASS_IAYPA_CAREER_MEAN = home_passer_career_df['IAY/PA'].astype(float).mean() 
                            HOME_PASS_IAYPA_CAREER_STD = home_passer_career_df['IAY/PA'].astype(float).std()
                            HOME_PASS_IAYPA_CAREER_MAX = home_passer_career_df['IAY/PA'].astype(float).max()
                            HOME_PASS_IAYPA_CAREER_MIN = home_passer_career_df['IAY/PA'].astype(float).min()
                            HOME_PASS_CAY_CAREER_MEAN = home_passer_career_df['CAY'].astype(float).mean() 
                            HOME_PASS_CAY_CAREER_STD = home_passer_career_df['CAY'].astype(float).std()
                            HOME_PASS_CAY_CAREER_MAX = home_passer_career_df['CAY'].astype(float).max()
                            HOME_PASS_CAY_CAREER_MIN = home_passer_career_df['CAY'].astype(float).min()
                            HOME_PASS_CAYCMP_CAREER_MEAN = home_passer_career_df['CAY/Cmp'].astype(float).mean() 
                            HOME_PASS_CAYCMP_CAREER_STD = home_passer_career_df['CAY/Cmp'].astype(float).std()
                            HOME_PASS_CAYCMP_CAREER_MAX = home_passer_career_df['CAY/Cmp'].astype(float).max()
                            HOME_PASS_CAYCMP_CAREER_MIN = home_passer_career_df['CAY/Cmp'].astype(float).min()
                            HOME_PASS_CAYPA_CAREER_MEAN = home_passer_career_df['CAY/PA'].astype(float).mean() 
                            HOME_PASS_CAYPA_CAREER_STD = home_passer_career_df['CAY/PA'].astype(float).std()
                            HOME_PASS_CAYPA_CAREER_MAX = home_passer_career_df['CAY/PA'].astype(float).max()
                            HOME_PASS_CAYPA_CAREER_MIN = home_passer_career_df['CAY/PA'].astype(float).min()
                            HOME_PASS_YAC_CAREER_MEAN = home_passer_career_df['YAC'].astype(float).mean() 
                            HOME_PASS_YAC_CAREER_STD = home_passer_career_df['YAC'].astype(float).std()
                            HOME_PASS_YAC_CAREER_MAX = home_passer_career_df['YAC'].astype(float).max()
                            HOME_PASS_YAC_CAREER_MIN = home_passer_career_df['YAC'].astype(float).min()
                            HOME_PASS_YACCMP_CAREER_MEAN = home_passer_career_df['YAC/Cmp'].astype(float).mean() 
                            HOME_PASS_YACCMP_CAREER_STD = home_passer_career_df['YAC/Cmp'].astype(float).std()
                            HOME_PASS_YACCMP_CAREER_MAX = home_passer_career_df['YAC/Cmp'].astype(float).max()
                            HOME_PASS_YACCMP_CAREER_MIN = home_passer_career_df['YAC/Cmp'].astype(float).min()
                            HOME_PASS_BADTH_CAREER_MEAN = home_passer_career_df['BadTh'].astype(float).mean() 
                            HOME_PASS_BADTH_CAREER_STD = home_passer_career_df['BadTh'].astype(float).std()
                            HOME_PASS_BADTH_CAREER_MAX = home_passer_career_df['BadTh'].astype(float).max()
                            HOME_PASS_BADTH_CAREER_MIN = home_passer_career_df['BadTh'].astype(float).min()
                            HOME_PASS_SK_CAREER_MEAN = home_passer_career_df['Sk'].astype(float).mean() 
                            HOME_PASS_SK_CAREER_STD = home_passer_career_df['Sk'].astype(float).std()
                            HOME_PASS_SK_CAREER_MAX = home_passer_career_df['Sk'].astype(float).max()
                            HOME_PASS_SK_CAREER_MIN = home_passer_career_df['Sk'].astype(float).min()
                            HOME_PASS_BLITZ_CAREER_MEAN = home_passer_career_df['Bltz'].astype(float).mean() 
                            HOME_PASS_BLITZ_CAREER_STD = home_passer_career_df['Bltz'].astype(float).std()
                            HOME_PASS_BLITZ_CAREER_MAX = home_passer_career_df['Bltz'].astype(float).max()
                            HOME_PASS_BLITZ_CAREER_MIN = home_passer_career_df['Bltz'].astype(float).min()
                            HOME_PASS_HRRY_CAREER_MEAN = home_passer_career_df['Hrry'].astype(float).mean() 
                            HOME_PASS_HRRY_CAREER_STD = home_passer_career_df['Hrry'].astype(float).std()
                            HOME_PASS_HRRY_CAREER_MAX = home_passer_career_df['Hrry'].astype(float).max()
                            HOME_PASS_HRRY_CAREER_MIN = home_passer_career_df['Hrry'].astype(float).min()
                            HOME_PASS_HITS_CAREER_MEAN = home_passer_career_df['Hits'].astype(float).mean() 
                            HOME_PASS_HITS_CAREER_STD = home_passer_career_df['Hits'].astype(float).std()
                            HOME_PASS_HITS_CAREER_MAX = home_passer_career_df['Hits'].astype(float).max()
                            HOME_PASS_HITS_CAREER_MIN = home_passer_career_df['Hits'].astype(float).min()
                            HOME_PASS_PRSS_CAREER_MEAN = home_passer_career_df['Prss'].astype(float).mean() 
                            HOME_PASS_PRSS_CAREER_STD = home_passer_career_df['Prss'].astype(float).std()
                            HOME_PASS_PRSS_CAREER_MAX = home_passer_career_df['Prss'].astype(float).max()
                            HOME_PASS_PRSS_CAREER_MIN = home_passer_career_df['Prss'].astype(float).min()
                            HOME_PASS_PRSSPCT_CAREER_MEAN = home_passer_career_df['Prss%'].astype(float).mean() 
                            HOME_PASS_PRSSPCT_CAREER_STD = home_passer_career_df['Prss%'].astype(float).std()
                            HOME_PASS_PRSSPCT_CAREER_MAX = home_passer_career_df['Prss%'].astype(float).max()
                            HOME_PASS_PRSSPCT_CAREER_MIN = home_passer_career_df['Prss%'].astype(float).min()

                            HOME_PASS_BIG_GAME_W = home_passer_career_df['BIG_GAME_W'].sum()
                            HOME_PASS_BIG_GAME_L = home_passer_career_df['BIG_GAME_L'].sum()
                            HOME_PASS_PLAYOFF_W = home_passer_career_df['PLAYOFF_W'].sum()
                            HOME_PASS_PLAYOFF_L = home_passer_career_df['PLAYOFF_L'].sum()
                            HOME_PASS_CHAMP_W = home_passer_career_df['CHAMP_W'].sum()
                            HOME_PASS_CHAMP_L = home_passer_career_df['CHAMP_L'].sum()

                        # at any rate: append the flat passer values to passers_{year}, as well as passers_2018-present, after adding 'big game' wins
                        passers_frame.to_sql(f"passers_{year}", con, if_exists="append")
                        passers_frame.to_sql(f"passers_2018-present", con, if_exists="append")

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

                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"starters_{year}\"", con)
                        if table.empty:
                            HOME_UNIQ_STARTERS_OL = -1
                            HOME_UNIQ_STARTERS_DEFENSE = -1
                            HOME_UNIQ_STARTERS_SKILL = -1
                            HOME_UNIQ_STARTERS_QB = -1

                        else:
                            try:
                                home_uniq_starters_ol_query = f"SELECT COUNT(distinct Player) AS ol_count FROM starters_{year} where Pos in ('OL','LT','LG','C','RG','RT','OT','OG','G/C','G/T','T/G','T/C','C/G','C/T','T','G') and Team = '{home}';"
                                count_df = pd.read_sql_query(home_uniq_starters_ol_query, con)
                                count = count_df['ol_count'].iloc[0]
                                if count > 0:
                                    HOME_UNIQ_STARTERS_OL = count
                                else:
                                    HOME_UNIQ_STARTERS_OL = -1
                            except Exception as e:
                                HOME_UNIQ_STARTERS_OL = -1

                            try:
                                home_uniq_starters_def_query = f"SELECT COUNT(distinct Player) AS def_count FROM starters_{year} where Pos in ('DE','DT','NT','DL','EDGE','ILB','OLB','LB','S','SS','FS','CB','DB','RCB','LCB','WLB','RDE','LDE','SLB','ROLB','LOLB','RILB','LILB','WILL','MIKE','SAM','RDT','LDT','RLB','DE/DT','DE/NT', 'SAF') and Team = '{home}';"
                                count_df = pd.read_sql_query(home_uniq_starters_def_query, con)
                                count = count_df['def_count'].iloc[0]
                                if count > 0:
                                    HOME_UNIQ_STARTERS_DEFENSE = count
                                else:
                                    HOME_UNIQ_STARTERS_DEFENSE = -1
                            except Exception as e:
                                HOME_UNIQ_STARTERS_DEFENSE = -1

                            try:
                                home_uniq_starters_skill_query = f"SELECT COUNT(distinct Player) AS skill_count FROM starters_{year} where Pos in ('WR','TE','RB','HB','FB','FB/DL','WR/PR') and Team = '{home}';"
                                count_df = pd.read_sql_query(home_uniq_starters_skill_query, con)
                                count = count_df['skill_count'].iloc[0]
                                if count > 0:
                                    HOME_UNIQ_STARTERS_SKILL = count
                                else:
                                    HOME_UNIQ_STARTERS_SKILL = -1
                            except Exception as e:
                                HOME_UNIQ_STARTERS_SKILL = -1

                            try:
                                home_uniq_starters_qb_query = f"SELECT COUNT(distinct Player) AS qb_count FROM starters_{year} where Pos in ('QB') and Team = '{home}';"
                                count_df = pd.read_sql_query(home_uniq_starters_qb_query, con)
                                count = count_df['qb_count'].iloc[0]
                                if count > 0:
                                    HOME_UNIQ_STARTERS_QB = count
                                else:
                                    HOME_UNIQ_STARTERS_QB = -1
                            except Exception as e:
                                HOME_UNIQ_STARTERS_QB = -1

                        df.to_sql(f"starters_{year}", con, if_exists="append")

                    if "vis_starters" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="vis_starters")
                        away_starts_table = comment_soup.find('table', id="vis_starters")

                        df = pd.read_html(str(away_starts_table))[0]
                        df['Team'] = vis
                        df['Week'] = week

                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"starters_{year}\"", con)
                        if table.empty:
                            AWAY_UNIQ_STARTERS_OL = -1
                            AWAY_UNIQ_STARTERS_DEFENSE = -1
                            AWAY_UNIQ_STARTERS_SKILL = -1
                            AWAY_UNIQ_STARTERS_QB = -1

                        else:
                            try:
                                away_uniq_starters_ol_query = f"SELECT COUNT(distinct Player) AS ol_count FROM starters_{year} where Pos in ('OL','LT','LG','C','RG','RT','OT','OG','G/C','G/T','T/G','T/C','C/G','C/T','T','G') and Team = '{vis}';"
                                count_df = pd.read_sql_query(away_uniq_starters_ol_query, con)
                                count = count_df['ol_count'].iloc[0]
                                if count > 0:
                                    AWAY_UNIQ_STARTERS_OL = count
                                else:
                                    AWAY_UNIQ_STARTERS_OL = -1
                            except Exception as e:
                                AWAY_UNIQ_STARTERS_OL = -1

                            try:
                                away_uniq_starters_def_query = f"SELECT COUNT(distinct Player) AS def_count FROM starters_{year} where Pos in ('DE','DT','NT','DL','EDGE','ILB','OLB','MLB','LB','S','SS','FS','CB','DB','RCB','LCB','WLB','RDE','LDE','SLB','ROLB','LOLB','RILB','LILB','WILL','MIKE','SAM','RDT','LDT','RLB','DE/DT','DE/NT', 'SAF') and Team = '{vis}';"
                                count_df = pd.read_sql_query(away_uniq_starters_def_query, con)
                                count = count_df['def_count'].iloc[0]
                                if count > 0:
                                    AWAY_UNIQ_STARTERS_DEFENSE = count
                                else:
                                    AWAY_UNIQ_STARTERS_DEFENSE = -1
                            except Exception as e:
                                AWAY_UNIQ_STARTERS_DEFENSE = -1

                            try:
                                away_uniq_starters_skill_query = f"SELECT COUNT(distinct Player) AS skill_count FROM starters_{year} where Pos in ('WR','TE','RB','HB','FB','FB/DL','WR/PR') and Team = '{vis}';"
                                count_df = pd.read_sql_query(away_uniq_starters_skill_query, con)
                                count = count_df['skill_count'].iloc[0]
                                if count > 0:
                                    AWAY_UNIQ_STARTERS_SKILL = count
                                else:
                                    AWAY_UNIQ_STARTERS_SKILL = -1
                            except Exception as e:
                                AWAY_UNIQ_STARTERS_SKILL = -1

                            try:
                                away_uniq_starters_qb_query = f"SELECT COUNT(distinct Player) AS qb_count FROM starters_{year} where Pos in ('QB') and Team = '{vis}';"
                                count_df = pd.read_sql_query(away_uniq_starters_qb_query, con)
                                count = count_df['qb_count'].iloc[0]
                                if count > 0:
                                    AWAY_UNIQ_STARTERS_QB = count
                                else:
                                    AWAY_UNIQ_STARTERS_QB = -1
                            except Exception as e:
                                AWAY_UNIQ_STARTERS_QB = -1

                        df.to_sql(f"starters_{year}", con, if_exists="append")

                    #######################################################
                    #####   END COLLECTING DATA FOR SNAPS TABLES      #####
                    #######################################################

            except Exception as e: 
                print('could not get game data: ', e)
                tb = traceback.extract_tb(e.__traceback__)
                print("Exception occurred at line:", tb[-1].lineno)

            game_data = [{
                'SEASON': year,
                'AWAY_TEAM_NAME': vis,
                'AWAY_TEAM_PREV_RANK': get_prev_year_rank(year, vis),
                'HOME_TEAM_NAME': home,
                'HOME_TEAM_PREV_RANK': get_prev_year_rank(year, home),
                'AWAY_SCORE': AWAY_SCORE,
                'HOME_SCORE': HOME_SCORE,
                'WEEK': week,
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
                'AWAY_RUSH_ATT_MEAN': AWAY_RUSH_ATT_MEAN,
                'AWAY_RUSH_ATT_STD': AWAY_RUSH_ATT_STD,
                'AWAY_RUSH_ATT_MAX':AWAY_RUSH_ATT_MAX,
                'AWAY_RUSH_ATT_MIN': AWAY_RUSH_ATT_MIN,
                'AWAY_RUSH_YDS_MEAN': AWAY_RUSH_YDS_MEAN,
                'AWAY_RUSH_YDS_STD': AWAY_RUSH_YDS_STD,
                'AWAY_RUSH_YDS_MAX': AWAY_RUSH_YDS_MAX,
                'AWAY_RUSH_YDS_MIN': AWAY_RUSH_YDS_MIN,
                'AWAY_RUSH_TD_MEAN': AWAY_RUSH_TD_MEAN,
                'AWAY_RUSH_TD_STD': AWAY_RUSH_TD_STD,
                'AWAY_RUSH_TD_MAX': AWAY_RUSH_TD_MAX,
                'AWAY_RUSH_TD_MIN': AWAY_RUSH_TD_MIN,
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
                'AWAY_PASS_1DPCT_MAX': AWAY_PASS_1DPCT_MAX,
                'AWAY_PASS_1DPCT_MEAN': AWAY_PASS_1DPCT_MEAN,
                'AWAY_PASS_1DPCT_MIN': AWAY_PASS_1DPCT_MIN,
                'AWAY_PASS_1DPCT_STD': AWAY_PASS_1DPCT_STD,
                'AWAY_PASS_1D_MAX': AWAY_PASS_1D_MAX,
                'AWAY_PASS_1D_MEAN': AWAY_PASS_1D_MEAN,
                'AWAY_PASS_1D_MIN': AWAY_PASS_1D_MIN,
                'AWAY_PASS_1D_STD': AWAY_PASS_1D_STD,
                'AWAY_PASS_ATT_MAX': AWAY_PASS_ATT_MAX,
                'AWAY_PASS_ATT_MEAN': AWAY_PASS_ATT_MEAN,
                'AWAY_PASS_ATT_MIN': AWAY_PASS_ATT_MIN,
                'AWAY_PASS_ATT_STD': AWAY_PASS_ATT_STD,
                'AWAY_PASS_CAYCMP_MAX': AWAY_PASS_CAYCMP_MAX,
                'AWAY_PASS_CAYCMP_MEAN': AWAY_PASS_CAYCMP_MEAN,
                'AWAY_PASS_CAYCMP_MIN': AWAY_PASS_CAYCMP_MIN,
                'AWAY_PASS_CAYCMP_STD': AWAY_PASS_CAYCMP_STD,
                'AWAY_PASS_CAY_MAX': AWAY_PASS_CAY_MAX,
                'AWAY_PASS_CAY_MEAN': AWAY_PASS_CAY_MEAN,
                'AWAY_PASS_CAY_MIN': AWAY_PASS_CAY_MIN,
                'AWAY_PASS_CAY_STD': AWAY_PASS_CAY_STD,
                'AWAY_PASS_COMP_MAX': AWAY_PASS_COMP_MAX,
                'AWAY_PASS_COMP_MEAN': AWAY_PASS_COMP_MEAN,
                'AWAY_PASS_COMP_MIN': AWAY_PASS_COMP_MIN,
                'AWAY_PASS_COMP_STD': AWAY_PASS_COMP_STD,
                'AWAY_PASS_IAYPA_MAX': AWAY_PASS_IAYPA_MAX,
                'AWAY_PASS_IAYPA_MEAN': AWAY_PASS_IAYPA_MEAN,
                'AWAY_PASS_IAYPA_MIN': AWAY_PASS_IAYPA_MIN,
                'AWAY_PASS_IAYPA_STD': AWAY_PASS_IAYPA_STD,
                'AWAY_PASS_IAY_MAX': AWAY_PASS_IAY_MAX,
                'AWAY_PASS_IAY_MEAN': AWAY_PASS_IAY_MEAN,
                'AWAY_PASS_IAY_MIN': AWAY_PASS_IAY_MIN,
                'AWAY_PASS_IAY_STD': AWAY_PASS_IAY_STD,
                'AWAY_PASS_YDS_MAX': AWAY_PASS_YDS_MAX,
                'AWAY_PASS_YDS_MEAN': AWAY_PASS_YDS_MEAN,
                'AWAY_PASS_YDS_MIN': AWAY_PASS_YDS_MIN,
                'AWAY_PASS_YDS_STD': AWAY_PASS_YDS_STD,
                'AWAY_PASS_1DPCT_CAREER_MAX': AWAY_PASS_1DPCT_CAREER_MAX,
                'AWAY_PASS_1DPCT_CAREER_MEAN': AWAY_PASS_1DPCT_CAREER_MEAN,
                'AWAY_PASS_1DPCT_CAREER_MIN': AWAY_PASS_1DPCT_CAREER_MIN,
                'AWAY_PASS_1DPCT_CAREER_STD': AWAY_PASS_1DPCT_CAREER_STD,
                'AWAY_PASS_1D_CAREER_MAX': AWAY_PASS_1D_CAREER_MAX,
                'AWAY_PASS_1D_CAREER_MEAN': AWAY_PASS_1D_CAREER_MEAN,
                'AWAY_PASS_1D_CAREER_MIN': AWAY_PASS_1D_CAREER_MIN,
                'AWAY_PASS_1D_CAREER_STD': AWAY_PASS_1D_CAREER_STD,
                'AWAY_PASS_ATT_CAREER_MAX': AWAY_PASS_ATT_CAREER_MAX,
                'AWAY_PASS_ATT_CAREER_MEAN': AWAY_PASS_ATT_CAREER_MEAN,
                'AWAY_PASS_ATT_CAREER_MIN': AWAY_PASS_ATT_CAREER_MIN,
                'AWAY_PASS_ATT_CAREER_STD': AWAY_PASS_ATT_CAREER_STD,
                'AWAY_PASS_BADTH_CAREER_MAX': AWAY_PASS_BADTH_CAREER_MAX,
                'AWAY_PASS_BADTH_CAREER_MEAN': AWAY_PASS_BADTH_CAREER_MEAN,
                'AWAY_PASS_BADTH_CAREER_MIN': AWAY_PASS_BADTH_CAREER_MIN,
                'AWAY_PASS_BADTH_CAREER_STD': AWAY_PASS_BADTH_CAREER_STD,
                'AWAY_PASS_BADTH_MAX': AWAY_PASS_BADTH_MAX,
                'AWAY_PASS_BADTH_MEAN': AWAY_PASS_BADTH_MEAN,
                'AWAY_PASS_BADTH_MIN': AWAY_PASS_BADTH_MIN,
                'AWAY_PASS_BADTH_STD': AWAY_PASS_BADTH_STD,
                'AWAY_PASS_BIG_GAME_L': AWAY_PASS_BIG_GAME_L,
                'AWAY_PASS_BIG_GAME_W': AWAY_PASS_BIG_GAME_W,
                'AWAY_PASS_BLITZ_CAREER_MAX': AWAY_PASS_BLITZ_CAREER_MAX,
                'AWAY_PASS_BLITZ_CAREER_MEAN': AWAY_PASS_BLITZ_CAREER_MEAN,
                'AWAY_PASS_BLITZ_CAREER_MIN': AWAY_PASS_BLITZ_CAREER_MIN,
                'AWAY_PASS_BLITZ_CAREER_STD': AWAY_PASS_BLITZ_CAREER_STD,
                'AWAY_PASS_BLITZ_MAX': AWAY_PASS_BLITZ_MAX,
                'AWAY_PASS_BLITZ_MEAN': AWAY_PASS_BLITZ_MEAN,
                'AWAY_PASS_BLITZ_MIN': AWAY_PASS_BLITZ_MIN,
                'AWAY_PASS_BLITZ_STD': AWAY_PASS_BLITZ_STD,
                'AWAY_PASS_CAYCMP_CAREER_MAX': AWAY_PASS_CAYCMP_CAREER_MAX,
                'AWAY_PASS_CAYCMP_CAREER_MEAN': AWAY_PASS_CAYCMP_CAREER_MEAN,
                'AWAY_PASS_CAYCMP_CAREER_MIN': AWAY_PASS_CAYCMP_CAREER_MIN,
                'AWAY_PASS_CAYCMP_CAREER_STD': AWAY_PASS_CAYCMP_CAREER_STD,
                'AWAY_PASS_CAYPA_CAREER_MAX': AWAY_PASS_CAYPA_CAREER_MAX,
                'AWAY_PASS_CAYPA_CAREER_MEAN': AWAY_PASS_CAYPA_CAREER_MEAN,
                'AWAY_PASS_CAYPA_CAREER_MIN': AWAY_PASS_CAYPA_CAREER_MIN,
                'AWAY_PASS_CAYPA_CAREER_STD': AWAY_PASS_CAYPA_CAREER_STD,
                'AWAY_PASS_CAYPA_MAX': AWAY_PASS_CAYPA_MAX,
                'AWAY_PASS_CAYPA_MEAN': AWAY_PASS_CAYPA_MEAN,
                'AWAY_PASS_CAYPA_MIN': AWAY_PASS_CAYPA_MIN,
                'AWAY_PASS_CAYPA_STD': AWAY_PASS_CAYPA_STD,
                'AWAY_PASS_CAY_CAREER_MAX': AWAY_PASS_CAY_CAREER_MAX,
                'AWAY_PASS_CAY_CAREER_MEAN': AWAY_PASS_CAY_CAREER_MEAN,
                'AWAY_PASS_CAY_CAREER_MIN': AWAY_PASS_CAY_CAREER_MIN,
                'AWAY_PASS_CAY_CAREER_STD': AWAY_PASS_CAY_CAREER_STD,
                'AWAY_PASS_CHAMP_L': AWAY_PASS_CHAMP_L,
                'AWAY_PASS_CHAMP_W': AWAY_PASS_CHAMP_W,
                'AWAY_PASS_COMP_CAREER_MAX': AWAY_PASS_COMP_CAREER_MAX,
                'AWAY_PASS_COMP_CAREER_MEAN': AWAY_PASS_COMP_CAREER_MEAN,
                'AWAY_PASS_COMP_CAREER_MIN': AWAY_PASS_COMP_CAREER_MIN,
                'AWAY_PASS_COMP_CAREER_STD': AWAY_PASS_COMP_CAREER_STD,
                'AWAY_PASS_DROPPCT_MAX': AWAY_PASS_DROPPCT_MAX,
                'AWAY_PASS_DROPPCT_MEAN': AWAY_PASS_DROPPCT_MEAN,
                'AWAY_PASS_DROPPCT_MIN': AWAY_PASS_DROPPCT_MIN,
                'AWAY_PASS_DROPPCT_STD': AWAY_PASS_DROPPCT_STD,
                'AWAY_PASS_DROPS_MAX': AWAY_PASS_DROPS_MAX,
                'AWAY_PASS_DROPS_MEAN': AWAY_PASS_DROPS_MEAN,
                'AWAY_PASS_DROPS_MIN': AWAY_PASS_DROPS_MIN,
                'AWAY_PASS_DROPS_STD': AWAY_PASS_DROPS_STD,
                'AWAY_PASS_HITS_CAREER_MAX': AWAY_PASS_HITS_CAREER_MAX,
                'AWAY_PASS_HITS_CAREER_MEAN': AWAY_PASS_HITS_CAREER_MEAN,
                'AWAY_PASS_HITS_CAREER_MIN': AWAY_PASS_HITS_CAREER_MIN,
                'AWAY_PASS_HITS_CAREER_STD': AWAY_PASS_HITS_CAREER_STD,
                'AWAY_PASS_HITS_MAX': AWAY_PASS_HITS_MAX,
                'AWAY_PASS_HITS_MEAN': AWAY_PASS_HITS_MEAN,
                'AWAY_PASS_HITS_MIN': AWAY_PASS_HITS_MIN,
                'AWAY_PASS_HITS_STD': AWAY_PASS_HITS_STD,
                'AWAY_PASS_HRRY_CAREER_MAX': AWAY_PASS_HRRY_CAREER_MAX,
                'AWAY_PASS_HRRY_CAREER_MEAN': AWAY_PASS_HRRY_CAREER_MEAN,
                'AWAY_PASS_HRRY_CAREER_MIN': AWAY_PASS_HRRY_CAREER_MIN,
                'AWAY_PASS_HRRY_CAREER_STD': AWAY_PASS_HRRY_CAREER_STD,
                'AWAY_PASS_HRRY_MAX': AWAY_PASS_HRRY_MAX,
                'AWAY_PASS_HRRY_MEAN': AWAY_PASS_HRRY_MEAN,
                'AWAY_PASS_HRRY_MIN': AWAY_PASS_HRRY_MIN,
                'AWAY_PASS_HRRY_STD': AWAY_PASS_HRRY_STD,
                'AWAY_PASS_IAYPA_CAREER_MAX': AWAY_PASS_IAYPA_CAREER_MAX,
                'AWAY_PASS_IAYPA_CAREER_MEAN': AWAY_PASS_IAYPA_CAREER_MEAN,
                'AWAY_PASS_IAYPA_CAREER_MIN': AWAY_PASS_IAYPA_CAREER_MIN,
                'AWAY_PASS_IAYPA_CAREER_STD': AWAY_PASS_IAYPA_CAREER_STD,
                'AWAY_PASS_IAY_CAREER_MAX': AWAY_PASS_IAY_CAREER_MAX,
                'AWAY_PASS_IAY_CAREER_MEAN': AWAY_PASS_IAY_CAREER_MEAN,
                'AWAY_PASS_IAY_CAREER_MIN': AWAY_PASS_IAY_CAREER_MIN,
                'AWAY_PASS_IAY_CAREER_STD': AWAY_PASS_IAY_CAREER_STD,
                'AWAY_PASS_PLAYOFF_L': AWAY_PASS_PLAYOFF_L,
                'AWAY_PASS_PLAYOFF_W': AWAY_PASS_PLAYOFF_W,
                'AWAY_PASS_PRSSPCT_CAREER_MAX': AWAY_PASS_PRSSPCT_CAREER_MAX,
                'AWAY_PASS_PRSSPCT_CAREER_MEAN': AWAY_PASS_PRSSPCT_CAREER_MEAN,
                'AWAY_PASS_PRSSPCT_CAREER_MIN': AWAY_PASS_PRSSPCT_CAREER_MIN,
                'AWAY_PASS_PRSSPCT_CAREER_STD': AWAY_PASS_PRSSPCT_CAREER_STD,
                'AWAY_PASS_PRSSPCT_MAX': AWAY_PASS_PRSSPCT_MAX,
                'AWAY_PASS_PRSSPCT_MEAN': AWAY_PASS_PRSSPCT_MEAN,
                'AWAY_PASS_PRSSPCT_MIN': AWAY_PASS_PRSSPCT_MIN,
                'AWAY_PASS_PRSSPCT_STD': AWAY_PASS_PRSSPCT_STD,
                'AWAY_PASS_PRSS_CAREER_MAX': AWAY_PASS_PRSS_CAREER_MAX,
                'AWAY_PASS_PRSS_CAREER_MEAN': AWAY_PASS_PRSS_CAREER_MEAN,
                'AWAY_PASS_PRSS_CAREER_MIN': AWAY_PASS_PRSS_CAREER_MIN,
                'AWAY_PASS_PRSS_CAREER_STD': AWAY_PASS_PRSS_CAREER_STD,
                'AWAY_PASS_PRSS_MAX': AWAY_PASS_PRSS_MAX,
                'AWAY_PASS_PRSS_MEAN': AWAY_PASS_PRSS_MEAN,
                'AWAY_PASS_PRSS_MIN': AWAY_PASS_PRSS_MIN,
                'AWAY_PASS_PRSS_STD': AWAY_PASS_PRSS_STD,
                'AWAY_PASS_SCRM_MAX': AWAY_PASS_SCRM_MAX,
                'AWAY_PASS_SCRM_MEAN': AWAY_PASS_SCRM_MEAN,
                'AWAY_PASS_SCRM_MIN': AWAY_PASS_SCRM_MIN,
                'AWAY_PASS_SCRM_STD': AWAY_PASS_SCRM_STD,
                'AWAY_PASS_SK_CAREER_MAX': AWAY_PASS_SK_CAREER_MAX,
                'AWAY_PASS_SK_CAREER_MEAN': AWAY_PASS_SK_CAREER_MEAN,
                'AWAY_PASS_SK_CAREER_MIN': AWAY_PASS_SK_CAREER_MIN,
                'AWAY_PASS_SK_CAREER_STD': AWAY_PASS_SK_CAREER_STD,
                'AWAY_PASS_SK_MAX': AWAY_PASS_SK_MAX,
                'AWAY_PASS_SK_MEAN': AWAY_PASS_SK_MEAN,
                'AWAY_PASS_SK_MIN': AWAY_PASS_SK_MIN,
                'AWAY_PASS_SK_STD': AWAY_PASS_SK_STD,
                'AWAY_PASS_YACCMP_CAREER_MAX': AWAY_PASS_YACCMP_CAREER_MAX,
                'AWAY_PASS_YACCMP_CAREER_MEAN': AWAY_PASS_YACCMP_CAREER_MEAN,
                'AWAY_PASS_YACCMP_CAREER_MIN': AWAY_PASS_YACCMP_CAREER_MIN,
                'AWAY_PASS_YACCMP_CAREER_STD': AWAY_PASS_YACCMP_CAREER_STD,
                'AWAY_PASS_YACCMP_MAX': AWAY_PASS_YACCMP_MAX,
                'AWAY_PASS_YACCMP_MEAN': AWAY_PASS_YACCMP_MEAN,
                'AWAY_PASS_YACCMP_MIN': AWAY_PASS_YACCMP_MIN,
                'AWAY_PASS_YACCMP_STD': AWAY_PASS_YACCMP_STD,
                'AWAY_PASS_YAC_CAREER_MAX': AWAY_PASS_YAC_CAREER_MAX,
                'AWAY_PASS_YAC_CAREER_MEAN': AWAY_PASS_YAC_CAREER_MEAN,
                'AWAY_PASS_YAC_CAREER_MIN': AWAY_PASS_YAC_CAREER_MIN,
                'AWAY_PASS_YAC_CAREER_STD': AWAY_PASS_YAC_CAREER_STD,
                'AWAY_PASS_YAC_MAX': AWAY_PASS_YAC_MAX,
                'AWAY_PASS_YAC_MEAN': AWAY_PASS_YAC_MEAN,
                'AWAY_PASS_YAC_MIN': AWAY_PASS_YAC_MIN,
                'AWAY_PASS_YAC_STD': AWAY_PASS_YAC_STD,
                'AWAY_PASS_YDSSCRM_MAX': AWAY_PASS_YDSSCRM_MAX,
                'AWAY_PASS_YDSSCRM_MEAN': AWAY_PASS_YDSSCRM_MEAN,
                'AWAY_PASS_YDSSCRM_MIN': AWAY_PASS_YDSSCRM_MIN,
                'AWAY_PASS_YDSSCRM_STD': AWAY_PASS_YDSSCRM_STD,
                'AWAY_PASS_YDS_CAREER_MAX': AWAY_PASS_YDS_CAREER_MAX,
                'AWAY_PASS_YDS_CAREER_MEAN': AWAY_PASS_YDS_CAREER_MEAN,
                'AWAY_PASS_YDS_CAREER_MIN': AWAY_PASS_YDS_CAREER_MIN,
                'AWAY_PASS_YDS_CAREER_STD': AWAY_PASS_YDS_CAREER_STD,
                'HOME_FD_MEAN': HOME_FD_MEAN, 
                'HOME_FD_STD': HOME_FD_STD,
                'HOME_FD_MAX': HOME_FD_MAX,
                'HOME_FD_MIN': HOME_FD_MIN,
                'HOME_FD_AGAINST_MEAN': HOME_FD_AGAINST_MEAN,
                'HOME_FD_AGAINST_STD': HOME_FD_AGAINST_STD,
                'HOME_FD_AGAINST_MAX': HOME_FD_AGAINST_MAX,
                'HOME_FD_AGAINST_MIN': HOME_FD_AGAINST_MIN,
                'HOME_RUSH_ATT_MEAN': HOME_RUSH_ATT_MEAN,
                'HOME_RUSH_ATT_STD': HOME_RUSH_ATT_STD,
                'HOME_RUSH_ATT_MAX': HOME_RUSH_ATT_MAX,
                'HOME_RUSH_ATT_MIN': HOME_RUSH_ATT_MIN,
                'HOME_RUSH_YDS_MEAN': HOME_RUSH_YDS_MEAN,
                'HOME_RUSH_YDS_STD': HOME_RUSH_YDS_STD,
                'HOME_RUSH_YDS_MAX': HOME_RUSH_YDS_MAX,
                'HOME_RUSH_YDS_MIN': HOME_RUSH_YDS_MIN,
                'HOME_RUSH_TD_MEAN': HOME_RUSH_TD_MEAN,
                'HOME_RUSH_TD_STD': HOME_RUSH_TD_STD,
                'HOME_RUSH_TD_MAX': HOME_RUSH_TD_MAX,
                'HOME_RUSH_TD_MIN': HOME_RUSH_TD_MIN,
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
                'HOME_PASS_1DPCT_CAREER_MAX': HOME_PASS_1DPCT_CAREER_MAX,
                'HOME_PASS_1DPCT_CAREER_MEAN': HOME_PASS_1DPCT_CAREER_MEAN,
                'HOME_PASS_1DPCT_CAREER_MIN': HOME_PASS_1DPCT_CAREER_MIN,
                'HOME_PASS_1DPCT_CAREER_STD': HOME_PASS_1DPCT_CAREER_STD,
                'HOME_PASS_1DPCT_MAX': HOME_PASS_1DPCT_MAX,
                'HOME_PASS_1DPCT_MEAN': HOME_PASS_1DPCT_MEAN,
                'HOME_PASS_1DPCT_MIN': HOME_PASS_1DPCT_MIN,
                'HOME_PASS_1DPCT_STD': HOME_PASS_1DPCT_STD,
                'HOME_PASS_1D_CAREER_MAX': HOME_PASS_1D_CAREER_MAX,
                'HOME_PASS_1D_CAREER_MEAN': HOME_PASS_1D_CAREER_MEAN,
                'HOME_PASS_1D_CAREER_MIN': HOME_PASS_1D_CAREER_MIN,
                'HOME_PASS_1D_CAREER_STD': HOME_PASS_1D_CAREER_STD,
                'HOME_PASS_1D_MAX': HOME_PASS_1D_MAX,
                'HOME_PASS_1D_MEAN': HOME_PASS_1D_MEAN,
                'HOME_PASS_1D_MIN': HOME_PASS_1D_MIN,
                'HOME_PASS_1D_STD': HOME_PASS_1D_STD,
                'HOME_PASS_ATT_CAREER_MAX': HOME_PASS_ATT_CAREER_MAX,
                'HOME_PASS_ATT_CAREER_MEAN': HOME_PASS_ATT_CAREER_MEAN,
                'HOME_PASS_ATT_CAREER_MIN': HOME_PASS_ATT_CAREER_MIN,
                'HOME_PASS_ATT_CAREER_STD': HOME_PASS_ATT_CAREER_STD,
                'HOME_PASS_ATT_MAX': HOME_PASS_ATT_MAX,
                'HOME_PASS_ATT_MEAN': HOME_PASS_ATT_MEAN,
                'HOME_PASS_ATT_MIN': HOME_PASS_ATT_MIN,
                'HOME_PASS_ATT_STD': HOME_PASS_ATT_STD,
                'HOME_PASS_BADTH_CAREER_MAX': HOME_PASS_BADTH_CAREER_MAX,
                'HOME_PASS_BADTH_CAREER_MEAN': HOME_PASS_BADTH_CAREER_MEAN,
                'HOME_PASS_BADTH_CAREER_MIN': HOME_PASS_BADTH_CAREER_MIN,
                'HOME_PASS_BADTH_CAREER_STD': HOME_PASS_BADTH_CAREER_STD,
                'HOME_PASS_BADTH_MAX': HOME_PASS_BADTH_MAX,
                'HOME_PASS_BADTH_MEAN': HOME_PASS_BADTH_MEAN,
                'HOME_PASS_BADTH_MIN': HOME_PASS_BADTH_MIN,
                'HOME_PASS_BADTH_STD': HOME_PASS_BADTH_STD,
                'HOME_PASS_BIG_GAME_L': HOME_PASS_BIG_GAME_L,
                'HOME_PASS_BIG_GAME_W': HOME_PASS_BIG_GAME_W,
                'HOME_PASS_BLITZ_CAREER_MAX': HOME_PASS_BLITZ_CAREER_MAX,
                'HOME_PASS_BLITZ_CAREER_MEAN': HOME_PASS_BLITZ_CAREER_MEAN,
                'HOME_PASS_BLITZ_CAREER_MIN': HOME_PASS_BLITZ_CAREER_MIN,
                'HOME_PASS_BLITZ_CAREER_STD': HOME_PASS_BLITZ_CAREER_STD,
                'HOME_PASS_BLITZ_MAX': HOME_PASS_BLITZ_MAX,
                'HOME_PASS_BLITZ_MEAN': HOME_PASS_BLITZ_MEAN,
                'HOME_PASS_BLITZ_MIN': HOME_PASS_BLITZ_MIN,
                'HOME_PASS_BLITZ_STD': HOME_PASS_BLITZ_STD,
                'HOME_PASS_CAYCMP_CAREER_MAX': HOME_PASS_CAYCMP_CAREER_MAX,
                'HOME_PASS_CAYCMP_CAREER_MEAN': HOME_PASS_CAYCMP_CAREER_MEAN,
                'HOME_PASS_CAYCMP_CAREER_MIN': HOME_PASS_CAYCMP_CAREER_MIN,
                'HOME_PASS_CAYCMP_CAREER_STD': HOME_PASS_CAYCMP_CAREER_STD,
                'HOME_PASS_CAYCMP_MAX': HOME_PASS_CAYCMP_MAX,
                'HOME_PASS_CAYCMP_MEAN': HOME_PASS_CAYCMP_MEAN,
                'HOME_PASS_CAYCMP_MIN': HOME_PASS_CAYCMP_MIN,
                'HOME_PASS_CAYCMP_STD': HOME_PASS_CAYCMP_STD,
                'HOME_PASS_CAYPA_CAREER_MAX': HOME_PASS_CAYPA_CAREER_MAX,
                'HOME_PASS_CAYPA_CAREER_MEAN': HOME_PASS_CAYPA_CAREER_MEAN,
                'HOME_PASS_CAYPA_CAREER_MIN': HOME_PASS_CAYPA_CAREER_MIN,
                'HOME_PASS_CAYPA_CAREER_STD': HOME_PASS_CAYPA_CAREER_STD,
                'HOME_PASS_CAYPA_MAX': HOME_PASS_CAYPA_MAX,
                'HOME_PASS_CAYPA_MEAN': HOME_PASS_CAYPA_MEAN,
                'HOME_PASS_CAYPA_MIN': HOME_PASS_CAYPA_MIN,
                'HOME_PASS_CAYPA_STD': HOME_PASS_CAYPA_STD,
                'HOME_PASS_CAY_CAREER_MAX': HOME_PASS_CAY_CAREER_MAX,
                'HOME_PASS_CAY_CAREER_MEAN': HOME_PASS_CAY_CAREER_MEAN,
                'HOME_PASS_CAY_CAREER_MIN': HOME_PASS_CAY_CAREER_MIN,
                'HOME_PASS_CAY_CAREER_STD': HOME_PASS_CAY_CAREER_STD,
                'HOME_PASS_CAY_MAX': HOME_PASS_CAY_MAX,
                'HOME_PASS_CAY_MEAN': HOME_PASS_CAY_MEAN,
                'HOME_PASS_CAY_MIN': HOME_PASS_CAY_MIN,
                'HOME_PASS_CAY_STD': HOME_PASS_CAY_STD,
                'HOME_PASS_CHAMP_L': HOME_PASS_CHAMP_L,
                'HOME_PASS_CHAMP_W': HOME_PASS_CHAMP_W,
                'HOME_PASS_COMP_CAREER_MAX': HOME_PASS_COMP_CAREER_MAX,
                'HOME_PASS_COMP_CAREER_MEAN': HOME_PASS_COMP_CAREER_MEAN,
                'HOME_PASS_COMP_CAREER_MIN': HOME_PASS_COMP_CAREER_MIN,
                'HOME_PASS_COMP_CAREER_STD': HOME_PASS_COMP_CAREER_STD,
                'HOME_PASS_COMP_MAX': HOME_PASS_COMP_MAX,
                'HOME_PASS_COMP_MEAN': HOME_PASS_COMP_MEAN,
                'HOME_PASS_COMP_MIN': HOME_PASS_COMP_MIN,
                'HOME_PASS_COMP_STD': HOME_PASS_COMP_STD,
                'HOME_PASS_DROPPCT_MAX': HOME_PASS_DROPPCT_MAX,
                'HOME_PASS_DROPPCT_MEAN': HOME_PASS_DROPPCT_MEAN,
                'HOME_PASS_DROPPCT_MIN': HOME_PASS_DROPPCT_MIN,
                'HOME_PASS_DROPPCT_STD': HOME_PASS_DROPPCT_STD,
                'HOME_PASS_DROPS_MAX': HOME_PASS_DROPS_MAX,
                'HOME_PASS_DROPS_MEAN': HOME_PASS_DROPS_MEAN,
                'HOME_PASS_DROPS_MIN': HOME_PASS_DROPS_MIN,
                'HOME_PASS_DROPS_STD': HOME_PASS_DROPS_STD,
                'HOME_PASS_HITS_CAREER_MAX': HOME_PASS_HITS_CAREER_MAX,
                'HOME_PASS_HITS_CAREER_MEAN': HOME_PASS_HITS_CAREER_MEAN,
                'HOME_PASS_HITS_CAREER_MIN': HOME_PASS_HITS_CAREER_MIN,
                'HOME_PASS_HITS_CAREER_STD': HOME_PASS_HITS_CAREER_STD,
                'HOME_PASS_HITS_MAX': HOME_PASS_HITS_MAX,
                'HOME_PASS_HITS_MEAN': HOME_PASS_HITS_MEAN,
                'HOME_PASS_HITS_MIN': HOME_PASS_HITS_MIN,
                'HOME_PASS_HITS_STD': HOME_PASS_HITS_STD,
                'HOME_PASS_HRRY_CAREER_MAX': HOME_PASS_HRRY_CAREER_MAX,
                'HOME_PASS_HRRY_CAREER_MEAN': HOME_PASS_HRRY_CAREER_MEAN,
                'HOME_PASS_HRRY_CAREER_MIN': HOME_PASS_HRRY_CAREER_MIN,
                'HOME_PASS_HRRY_CAREER_STD': HOME_PASS_HRRY_CAREER_STD,
                'HOME_PASS_HRRY_MAX': HOME_PASS_HRRY_MAX,
                'HOME_PASS_HRRY_MEAN': HOME_PASS_HRRY_MEAN,
                'HOME_PASS_HRRY_MIN': HOME_PASS_HRRY_MIN,
                'HOME_PASS_HRRY_STD': HOME_PASS_HRRY_STD,
                'HOME_PASS_IAYPA_CAREER_MAX': HOME_PASS_IAYPA_CAREER_MAX,
                'HOME_PASS_IAYPA_CAREER_MEAN': HOME_PASS_IAYPA_CAREER_MEAN,
                'HOME_PASS_IAYPA_CAREER_MIN': HOME_PASS_IAYPA_CAREER_MIN,
                'HOME_PASS_IAYPA_CAREER_STD': HOME_PASS_IAYPA_CAREER_STD,
                'HOME_PASS_IAYPA_MAX': HOME_PASS_IAYPA_MAX,
                'HOME_PASS_IAYPA_MEAN': HOME_PASS_IAYPA_MEAN,
                'HOME_PASS_IAYPA_MIN': HOME_PASS_IAYPA_MIN,
                'HOME_PASS_IAYPA_STD': HOME_PASS_IAYPA_STD,
                'HOME_PASS_IAY_CAREER_MAX': HOME_PASS_IAY_CAREER_MAX,
                'HOME_PASS_IAY_CAREER_MEAN': HOME_PASS_IAY_CAREER_MEAN,
                'HOME_PASS_IAY_CAREER_MIN': HOME_PASS_IAY_CAREER_MIN,
                'HOME_PASS_IAY_CAREER_STD': HOME_PASS_IAY_CAREER_STD,
                'HOME_PASS_IAY_MAX': HOME_PASS_IAY_MAX,
                'HOME_PASS_IAY_MEAN': HOME_PASS_IAY_MEAN,
                'HOME_PASS_IAY_MIN': HOME_PASS_IAY_MIN,
                'HOME_PASS_IAY_STD': HOME_PASS_IAY_STD,
                'HOME_PASS_PLAYOFF_L': HOME_PASS_PLAYOFF_L,
                'HOME_PASS_PLAYOFF_W': HOME_PASS_PLAYOFF_W,
                'HOME_PASS_PRSSPCT_CAREER_MAX': HOME_PASS_PRSSPCT_CAREER_MAX,
                'HOME_PASS_PRSSPCT_CAREER_MEAN': HOME_PASS_PRSSPCT_CAREER_MEAN,
                'HOME_PASS_PRSSPCT_CAREER_MIN': HOME_PASS_PRSSPCT_CAREER_MIN,
                'HOME_PASS_PRSSPCT_CAREER_STD': HOME_PASS_PRSSPCT_CAREER_STD,
                'HOME_PASS_PRSSPCT_MAX': HOME_PASS_PRSSPCT_MAX,
                'HOME_PASS_PRSSPCT_MEAN': HOME_PASS_PRSSPCT_MEAN,
                'HOME_PASS_PRSSPCT_MIN': HOME_PASS_PRSSPCT_MIN,
                'HOME_PASS_PRSSPCT_STD': HOME_PASS_PRSSPCT_STD,
                'HOME_PASS_PRSS_CAREER_MAX': HOME_PASS_PRSS_CAREER_MAX,
                'HOME_PASS_PRSS_CAREER_MEAN': HOME_PASS_PRSS_CAREER_MEAN,
                'HOME_PASS_PRSS_CAREER_MIN': HOME_PASS_PRSS_CAREER_MIN,
                'HOME_PASS_PRSS_CAREER_STD': HOME_PASS_PRSS_CAREER_STD,
                'HOME_PASS_PRSS_MAX': HOME_PASS_PRSS_MAX,
                'HOME_PASS_PRSS_MEAN': HOME_PASS_PRSS_MEAN,
                'HOME_PASS_PRSS_MIN': HOME_PASS_PRSS_MIN,
                'HOME_PASS_PRSS_STD': HOME_PASS_PRSS_STD,
                'HOME_PASS_SCRM_MAX': HOME_PASS_SCRM_MAX,
                'HOME_PASS_SCRM_MEAN': HOME_PASS_SCRM_MEAN,
                'HOME_PASS_SCRM_MIN': HOME_PASS_SCRM_MIN,
                'HOME_PASS_SCRM_STD': HOME_PASS_SCRM_STD,
                'HOME_PASS_SK_CAREER_MAX': HOME_PASS_SK_CAREER_MAX,
                'HOME_PASS_SK_CAREER_MEAN': HOME_PASS_SK_CAREER_MEAN,
                'HOME_PASS_SK_CAREER_MIN': HOME_PASS_SK_CAREER_MIN,
                'HOME_PASS_SK_CAREER_STD': HOME_PASS_SK_CAREER_STD,
                'HOME_PASS_SK_MAX': HOME_PASS_SK_MAX,
                'HOME_PASS_SK_MEAN': HOME_PASS_SK_MEAN,
                'HOME_PASS_SK_MIN': HOME_PASS_SK_MIN,
                'HOME_PASS_SK_STD': HOME_PASS_SK_STD,
                'HOME_PASS_YACCMP_CAREER_MAX': HOME_PASS_YACCMP_CAREER_MAX,
                'HOME_PASS_YACCMP_CAREER_MEAN': HOME_PASS_YACCMP_CAREER_MEAN,
                'HOME_PASS_YACCMP_CAREER_MIN': HOME_PASS_YACCMP_CAREER_MIN,
                'HOME_PASS_YACCMP_CAREER_STD': HOME_PASS_YACCMP_CAREER_STD,
                'HOME_PASS_YACCMP_MAX': HOME_PASS_YACCMP_MAX,
                'HOME_PASS_YACCMP_MEAN': HOME_PASS_YACCMP_MEAN,
                'HOME_PASS_YACCMP_MIN': HOME_PASS_YACCMP_MIN,
                'HOME_PASS_YACCMP_STD': HOME_PASS_YACCMP_STD,
                'HOME_PASS_YAC_CAREER_MAX': HOME_PASS_YAC_CAREER_MAX,
                'HOME_PASS_YAC_CAREER_MEAN': HOME_PASS_YAC_CAREER_MEAN,
                'HOME_PASS_YAC_CAREER_MIN': HOME_PASS_YAC_CAREER_MIN,
                'HOME_PASS_YAC_CAREER_STD': HOME_PASS_YAC_CAREER_STD,
                'HOME_PASS_YAC_MAX': HOME_PASS_YAC_MAX,
                'HOME_PASS_YAC_MEAN': HOME_PASS_YAC_MEAN,
                'HOME_PASS_YAC_MIN': HOME_PASS_YAC_MIN,
                'HOME_PASS_YAC_STD': HOME_PASS_YAC_STD,
                'HOME_PASS_YDSSCRM_MAX': HOME_PASS_YDSSCRM_MAX,
                'HOME_PASS_YDSSCRM_MEAN': HOME_PASS_YDSSCRM_MEAN,
                'HOME_PASS_YDSSCRM_MIN': HOME_PASS_YDSSCRM_MIN,
                'HOME_PASS_YDSSCRM_STD': HOME_PASS_YDSSCRM_STD,
                'HOME_PASS_YDS_CAREER_MAX': HOME_PASS_YDS_CAREER_MAX,
                'HOME_PASS_YDS_CAREER_MEAN': HOME_PASS_YDS_CAREER_MEAN,
                'HOME_PASS_YDS_CAREER_MIN': HOME_PASS_YDS_CAREER_MIN,
                'HOME_PASS_YDS_CAREER_STD': HOME_PASS_YDS_CAREER_STD,
                'HOME_PASS_YDS_MAX': HOME_PASS_YDS_MAX,
                'HOME_PASS_YDS_MEAN': HOME_PASS_YDS_MEAN,
                'HOME_PASS_YDS_MIN': HOME_PASS_YDS_MIN,
                'HOME_PASS_YDS_STD': HOME_PASS_YDS_STD,
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
            }]

            game_df = pd.DataFrame(game_data)
            game_df.to_sql(f"games_{year}", con, if_exists="append")

con.close()