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

years = [2018,2019,2020,2021,2022,2023,2024]
# RANKS THROUGH 18 WEEKS; 18th is WC round until 2021
weeks = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]

games_con = sqlite3.connect("../../Data/v2/games.sqlite")
teams_con = sqlite3.connect("../../Data/v2/teams.sqlite")
teams_cursor = teams_con.cursor()
passers_con = sqlite3.connect("../../Data/v2/passers.sqlite")
player_snaps_con = sqlite3.connect("../../Data/v2/player_snaps.sqlite")

for year in years:
    season_data = []
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
                        df['BIG_GAME_WIN'] = -1 # use as a default value: -1 means this was not a 'big game'

                        visiting_passer = df.loc[df['Tm'] == vis].iloc[0]
                        home_passer = df.loc[df['Tm'] == home].iloc[0]

                        SPREAD_ABS = abs(float(SPREAD))
                        if SPREAD_ABS <= 3.0:
                            if home_team_win > 0:
                                home_passer['BIG_GAME_WIN'] = 1.0
                                visiting_passer['BIG_GAME_WIN'] = 0.0
                            else:
                                visiting_passer['BIG_GAME_WIN'] = 1.0
                                home_passer['BIG_GAME_WIN'] = 0.0


                        passers_frame = pd.DataFrame([visiting_passer.to_dict(), home_passer.to_dict()])
                        passers_frame.reset_index(drop=True, inplace=True)

                        passers_frame['1D%'] = passers_frame['1D%'].str.replace('%', '', regex=False).astype(float) / 100
                        passers_frame['Drop%'] = passers_frame['Drop%'].str.replace('%', '', regex=False).astype(float) / 100
                        passers_frame['Prss%'] = passers_frame['Prss%'].str.replace('%', '', regex=False).astype(float) / 100
                        passers_frame['Bad%'] = passers_frame['Bad%'].str.replace('%', '', regex=False).astype(float) / 100
                        
                        print(passers_frame)
                        exit(0)
                        

                        # df = df[df['Att'] > 2]

                        # If season passing table is empty, we want to pass -1 into each variable for the game stats
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name=\"passers_{year}\"", passers_con)
                        if table.empty:
                            AWAY_PASS_COMP_MEAN = -1 
                            AWAY_PASS_COMP_STD = -1
                            AWAY_PASS_COMP_MAX = -1
                            AWAY_PASS_COMP_MIN = -1
                            # TODO ... ... ...
                        else:
                            # if the table is not empty, take the mean, std, max and min from each of the RELEVANT VALues
                            away_passer_df = pd.read_sql_query(f"SELECT * FROM passers_{year} WHERE (WEEK < {week}) and Player = {visiting_passer['Player']} order by WEEK desc;", passers_con)
                            home_passer_df = pd.read_sql_query(f"SELECT * FROM passers_{year} WHERE (WEEK < {week}) and Player = {home_passer['Player']} order by WEEK desc;", passers_con)

                            AWAY_PASS_COMP_MEAN = away_passer_df['AWAY_PASS_COMP'].mean()
                            AWAY_PASS_COMP_STD = away_passer_df['AWAY_PASS_COMP'].std()
                            AWAY_PASS_COMP_MAX = away_passer_df['AWAY_PASS_COMP'].min()
                            AWAY_PASS_COMP_MIN = away_passer_df['AWAY_PASS_COMP'].max()
                            # TODO ... ... ...

                        # If CAREER passing table is empty, we want to pass passer's career stats from 2018 to present day
                        table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='passers_2018-present'", passers_con)
                        if table.empty:
                            AWAY_PASS_COMP_CAREER = -1
                            AWAY_PASS_ATT_CAREER = -1
                            AWAY_PASS_YDS_CAREER = -1
                            # TODO ... ... ...
                        else:
                            # if the table is not empty, take the mean, std, max and min from each of the RELEVANT VALues
                            away_passer_career_df = pd.read_sql_query(f"SELECT * FROM passers_2018-present WHERE and Player = {visiting_passer['Player']};", passers_con)
                            home_passer_career_df = pd.read_sql_query(f"SELECT * FROM passers_2018-present WHERE and Player = {home_passer['Player']};", passers_con)

                            AWAY_PASS_COMP_CAREER_MEAN = away_passer_career_df['AWAY_PASS_COMP'].mean()
                            AWAY_PASS_COMP_CAREER_STD = away_passer_career_df['AWAY_PASS_COMP'].std()
                            AWAY_PASS_COMP_CAREER_MAX = away_passer_career_df['AWAY_PASS_COMP'].min()
                            AWAY_PASS_COMP_CAREER_MIN = away_passer_career_df['AWAY_PASS_COMP'].max()
                            # TODO ... ... ...

                        # at any rate: append the flat passer values to passers_{year}, as well as passers_2018-present, after adding 'big game' wins
                        passers_frame.to_sql(f"passers_{year}", passers_con, if_exists="append")
                        passers_frame.to_sql(f"passers_2018-present", passers_con, if_exists="append")

                        exit(0)
                        # visiting_passer = df.loc[df['Tm'] == vis].iloc[0]
                        # home_passer = df.loc[df['Tm'] == home].iloc[0]

                        # AWAY_PASS_COMP = visiting_passer["Cmp"]
                        # AWAY_PASS_ATT = visiting_passer["Att"]
                        # AWAY_PASS_YDS = visiting_passer["Yds"]
                        # AWAY_PASS_1D = visiting_passer["1D"]
                        # AWAY_PASS_1DPCT = visiting_passer["1D%"]
                        # AWAY_PASS_IAY = visiting_passer["IAY"]
                        # AWAY_PASS_IAYPA = visiting_passer["IAY/PA"]
                        # AWAY_PASS_CAY = visiting_passer["CAY"]
                        # AWAY_PASS_CAYCMP = visiting_passer["CAY/Cmp"]
                        # AWAY_PASS_CAYPA = visiting_passer['CAY/PA']
                        # AWAY_PASS_YAC = visiting_passer['YAC']
                        # AWAY_PASS_YACCMP = visiting_passer['YAC/Cmp']
                        # AWAY_PASS_DROPS = visiting_passer['Drops']
                        # AWAY_PASS_DROPPCT = visiting_passer["Drop%"]
                        # AWAY_PASS_BADTH = visiting_passer["BadTh"]
                        # AWAY_PASS_SK = visiting_passer["Sk"]
                        # AWAY_PASS_BLTZ = visiting_passer["Bltz"]
                        # AWAY_PASS_HRRY = visiting_passer["Hrry"]
                        # AWAY_PASS_HITS = visiting_passer["Hits"]
                        # AWAY_PASS_PRSS = visiting_passer["Prss"]
                        # AWAY_PASS_PRSSPCT = visiting_passer["Prss%"]
                        # AWAY_PASS_SCRM = visiting_passer["Scrm"]
                        # AWAY_PASS_YDSSCRM = visiting_passer["Yds/Scr"]
                        # HOME_PASS_COMP = home_passer["Cmp"]
                        # HOME_PASS_ATT = home_passer["Att"]
                        # HOME_PASS_YDS = home_passer["Yds"]
                        # HOME_PASS_1D = home_passer["1D"]
                        # HOME_PASS_1DPCT = home_passer["1D%"]
                        # HOME_PASS_IAY = home_passer["IAY"]
                        # HOME_PASS_IAYPA = home_passer["IAY/PA"]
                        # HOME_PASS_CAY = home_passer["CAY"]
                        # HOME_PASS_CAYCMP = home_passer["CAY/Cmp"]
                        # HOME_PASS_CAYPA = home_passer['CAY/PA']
                        # HOME_PASS_YAC = home_passer['YAC']
                        # HOME_PASS_YACCMP = home_passer['YAC/Cmp']
                        # HOME_PASS_DROPS = home_passer['Drops']
                        # HOME_PASS_DROPPCT = home_passer["Drop%"]
                        # HOME_PASS_BADTH = home_passer["BadTh"]
                        # HOME_PASS_SK = home_passer["Sk"]
                        # HOME_PASS_BLTZ = home_passer["Bltz"]
                        # HOME_PASS_HRRY = home_passer["Hrry"]
                        # HOME_PASS_HITS = home_passer["Hits"]
                        # HOME_PASS_PRSS = home_passer["Prss"]
                        # HOME_PASS_PRSSPCT = home_passer["Prss%"]
                        # HOME_PASS_SCRM = home_passer["Scrm"]
                        # HOME_PASS_YDSSCRM = home_passer["Yds/Scr"]

                    #######################################################
                    #####   END COLLECTING DATA FOR PASSER TABLES     #####
                    #######################################################

                    #########################################################
                    #####   BEGIN COLLECTING DATA FOR RUSHER TABLES     #####
                    #########################################################
                    if "rushing_advanced" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="rushing_advanced")
                        adv_rushing_table = comment_soup.find('table', id="rushing_advanced")

                        df = pd.read_html(str(adv_rushing_table))[0]
                        df.fillna(0.0, inplace=True)

                        df = df[df['Player'] != 'Player']

                        visiting_rushers = pd.DataFrame(df.loc[df['Tm'] == vis])
                        home_rushers = pd.DataFrame(df.loc[df['Tm'] == home])

                        AWAY_RUSH_ATT = visiting_rushers['Att'].apply(pd.to_numeric).sum()
                        AWAY_RUSH_YDS = visiting_rushers['Yds'].apply(pd.to_numeric).sum()
                        AWAY_RUSH_TD = visiting_rushers['TD'].apply(pd.to_numeric).sum()
                        AWAY_RUSH_1D = visiting_rushers['1D'].apply(pd.to_numeric).sum()
                        AWAY_RUSH_YBC = visiting_rushers['YBC'].apply(pd.to_numeric).sum()
                        AWAY_RUSH_YBCATT = visiting_rushers['YBC/Att'].apply(pd.to_numeric).mean()
                        AWAY_RUSH_YAC = visiting_rushers['YAC'].apply(pd.to_numeric).sum()
                        AWAY_RUSH_YACATT = visiting_rushers['YAC/Att'].apply(pd.to_numeric).mean()
                        AWAY_RUSH_BRKTKL = visiting_rushers['BrkTkl'].apply(pd.to_numeric).sum()
                        AWAY_RUSH_ATTBR = visiting_rushers['Att/Br'].apply(pd.to_numeric).mean()
                        HOME_RUSH_ATT = home_rushers['Att'].apply(pd.to_numeric).sum()
                        HOME_RUSH_YDS = home_rushers['Yds'].apply(pd.to_numeric).sum()
                        HOME_RUSH_TD = home_rushers['TD'].apply(pd.to_numeric).sum()
                        HOME_RUSH_1D = home_rushers['1D'].apply(pd.to_numeric).sum()
                        HOME_RUSH_YBC = home_rushers['YBC'].apply(pd.to_numeric).sum()
                        HOME_RUSH_YBCATT = home_rushers['YBC/Att'].apply(pd.to_numeric).mean()
                        HOME_RUSH_YAC = home_rushers['YAC'].apply(pd.to_numeric).sum()
                        HOME_RUSH_YACATT = home_rushers['YAC/Att'].apply(pd.to_numeric).mean()
                        HOME_RUSH_BRKTKL = home_rushers['BrkTkl'].apply(pd.to_numeric).sum()
                        HOME_RUSH_ATTBR = home_rushers['Att/Br'].apply(pd.to_numeric).mean()

                        # TODO: calculate the necessities of these stats like with advanced passing. Career stats are probably not necessary

                    #######################################################
                    #####   END COLLECTING DATA FOR RUSHER TABLES     #####
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

                        df.to_sql(f"starters_{year}", player_snaps_con, if_exists="replace")
                        print("\n")
                        print(df)

                    if "away_starters" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="away_starters")
                        away_starts_table = comment_soup.find('table', id="away_starters")

                        df = pd.read_html(str(away_starts_table))[0]
                        df['Team'] = vis
                        df['Week'] = week

                        df.to_sql(f"starters_{year}", player_snaps_con, if_exists="replace")
                        print("\n")
                        print(df)
                        exit(0)
                    #######################################################
                    #####   END COLLECTING DATA FOR SNAPS TABLES      #####
                    #######################################################

            except Exception as e: 
                print('could not get game data: ', e)
                

            season_data.append({
                'SEASON': year,
                'WEEK': week,
                'AWAY_TEAM_NAME': vis,
                'HOME_TEAM_NAME': home,
                'AWAY_SCORE': AWAY_SCORE,
                'HOME_SCORE': HOME_SCORE,
                'AWAY_SOS': AWAY_SOS, # NEED TO GET AVERAGE SOS ENTERING GAME
                'HOME_SOS': HOME_SOS,
                'AWAY_UNIQ_STARTERS_DEFENSE': 0,
                'AWAY_UNIQ_STARTERS_OL':0,
                'AWAY_UNIQ_STARTERS_WR':0,
                'HOME_UNIQ_STARTERS_DEFENSE': 0,
                'HOME_UNIQ_STARTERS_OL':0,
                'HOME_UNIQ_STARTERS_WR':0,
                # 'AWAY_DEFENSE_YRS_EXP': 0,
                # 'AWAY_OL_YRS_EXP': 0,
                # 'AWAY_WR_YRS_EXP': 0,
                # 'HOME_DEFENSE_YRS_EXP': 0, 
                # 'HOME_OL_YRS_EXP': 0,
                # 'HOME_WR_YRS_EXP': 0,
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
                'AWAY_PASS_BIG_GAME_W': 0, # spread of -2.5 or tighter
                'AWAY_PASS_BIG_GAME_L': 0,
                'AWAY_PASS_PLAYOFF_W': 0,
                'AWAY_PASS_PLAYOFF_L': 0,
                'AWAY_PASS_CHAMP_W': 0,
                'AWAY_PASS_CHAMP_L': 0,
                'AWAY_RUSH_ATT_MEAN': 0, 
                'AWAY_RUSH_ATT_STD': 0,
                'AWAY_RUSH_ATT_MAX': 0,
                'AWAY_RUSH_ATT_MIN': 0,
                'AWAY_RUSH_YDS_MEAN': 0, 
                'AWAY_RUSH_YDS_STD': 0,
                'AWAY_RUSH_YDS_MAX': 0,
                'AWAY_RUSH_YDS_MIN': 0,
                'AWAY_RUSH_TD_MEAN': 0, 
                'AWAY_RUSH_TD_STD': 0,
                'AWAY_RUSH_TD_MAX': 0,
                'AWAY_RUSH_TD_MIN': 0,
                'AWAY_RUSH_1D_MEAN': 0, 
                'AWAY_RUSH_1D_STD': 0,
                'AWAY_RUSH_1D_MAX': 0,
                'AWAY_RUSH_1D_MIN': 0,
                'AWAY_RUSH_YBC_MEAN': 0, 
                'AWAY_RUSH_YBC_STD': 0,
                'AWAY_RUSH_YBC_MAX': 0,
                'AWAY_RUSH_YBC_MIN': 0,
                'AWAY_RUSH_YBCATT_MEAN': 0, 
                'AWAY_RUSH_YBCATT_STD': 0,
                'AWAY_RUSH_YBCATT_MAX': 0,
                'AWAY_RUSH_YBCATT_MIN': 0,
                'AWAY_RUSH_YAC_MEAN': 0, 
                'AWAY_RUSH_YAC_STD': 0,
                'AWAY_RUSH_YAC_MAX': 0,
                'AWAY_RUSH_YAC_MIN': 0,
                'AWAY_RUSH_YACATT_MEAN': 0, 
                'AWAY_RUSH_YACATT_STD': 0,
                'AWAY_RUSH_YACATT_MAX': 0,
                'AWAY_RUSH_YACATT_MIN': 0,
                'AWAY_RUSH_BRKTKL_MEAN': 0, 
                'AWAY_RUSH_BRKTKL_STD': 0,
                'AWAY_RUSH_BRKTKL_MAX': 0,
                'AWAY_RUSH_BRKTKL_MIN': 0,
                'AWAY_RUSH_ATTBR_MEAN': 0, 
                'AWAY_RUSH_ATTBR_STD': 0,
                'AWAY_RUSH_ATTBR_MAX': 0,
                'AWAY_RUSH_ATTBR_MIN': 0,
                'HOME_FD_MEAN': 0, 
                'HOME_FD_STD': 0,
                'HOME_FD_MAX': 0,
                'HOME_FD_MIN': 0,
                'HOME_FD_AGAINST_MEAN': 0,
                'HOME_FD_AGAINST_STD': 0,
                'HOME_FD_AGAINST_MAX': 0,
                'HOME_FD_AGAINST_MIN': 0,
                'HOME_SACKS_MEAN': 0, 
                'HOME_SACKS_STD': 0,
                'HOME_SACKS_MAX': 0,
                'HOME_SACKS_MIN': 0,
                'HOME_SACKS_AGAINST_MEAN': 0, 
                'HOME_SACKS_AGAINST_STD': 0,
                'HOME_SACKS_AGAINST_MAX': 0,
                'HOME_SACKS_AGAINST_MIN': 0,
                'HOME_SACK_YDS_MEAN': 0, 
                'HOME_SACK_YDS_STD': 0,
                'HOME_SACK_YDS_MAX': 0,
                'HOME_SACK_YDS_MIN': 0,
                'HOME_SACK_YDS_AGAINST_MEAN': 0, 
                'HOME_SACK_YDS_AGAINST_STD': 0,
                'HOME_SACK_YDS_AGAINST_MAX': 0,
                'HOME_SACK_YDS_AGAINST_MIN': 0,
                'HOME_TOTAL_YDS_MEAN': 0, 
                'HOME_TOTAL_YDS_STD': 0,
                'HOME_TOTAL_YDS_MAX': 0,
                'HOME_TOTAL_YDS_MIN': 0,
                'HOME_FUMBLES_MEAN': 0, 
                'HOME_FUMBLES_STD': 0,
                'HOME_FUMBLES_MAX': 0,
                'HOME_FUMBLES_MIN': 0,
                'HOME_FUMBLES_LOST_MEAN': 0, 
                'HOME_FUMBLES_LOST_STD': 0,
                'HOME_FUMBLES_LOST_MAX': 0,
                'HOME_FUMBLES_LOST_MIN': 0,
                'HOME_TO_MEAN': 0, 
                'HOME_TO_STD': 0,
                'HOME_TO_MAX': 0,
                'HOME_TO_MIN': 0,
                'HOME_TO_AGAINST_MEAN': 0, 
                'HOME_TO_AGAINST_STD': 0,
                'HOME_TO_AGAINST_MAX': 0,
                'HOME_TO_AGAINST_MIN': 0,
                'HOME_PENALTIES_MEAN': 0, 
                'HOME_PENALTIES_STD': 0,
                'HOME_PENALTIES_MAX': 0,
                'HOME_PENALTIES_MIN': 0,
                'HOME_PENALTY_YARDS_MEAN': 0, 
                'HOME_PENALTY_YARDS_STD': 0,
                'HOME_PENALTY_YARDS_MAX': 0,
                'HOME_PENALTY_YARDS_MIN': 0,
                'HOME_3RD_DOWN_MEAN': 0, 
                'HOME_3RD_DOWN_STD': 0,
                'HOME_3RD_DOWN_MAX': 0,
                'HOME_3RD_DOWN_MIN': 0,
                'HOME_3RD_DOWN_ATT_MEAN': 0, 
                'HOME_3RD_DOWN_ATT_STD': 0,
                'HOME_3RD_DOWN_ATT_MAX': 0,
                'HOME_3RD_DOWN_ATT_MIN': 0,
                'HOME_3RD_DOWN_CONV_MEAN': 0, 
                'HOME_3RD_DOWN_CONV_STD': 0,
                'HOME_3RD_DOWN_CONV_MAX': 0,
                'HOME_3RD_DOWN_CONV_MIN': 0,
                'HOME_3RD_DOWN_AGAINST_MEAN': 0,
                'HOME_3RD_DOWN_AGAINST_STD': 0,
                'HOME_3RD_DOWN_AGAINST_MAX': 0,
                'HOME_3RD_DOWN_AGAINST_MIN': 0,
                'HOME_3RD_DOWN_ATT_AGAINST_MEAN': 0,
                'HOME_3RD_DOWN_ATT_AGAINST_STD': 0,
                'HOME_3RD_DOWN_ATT_AGAINST_MAX': 0,
                'HOME_3RD_DOWN_ATT_AGAINST_MIN': 0,
                'HOME_3RD_DOWN_CONV_AGAINST_MEAN': 0,
                'HOME_3RD_DOWN_CONV_AGAINST_STD': 0,
                'HOME_3RD_DOWN_CONV_AGAINST_MAX': 0,
                'HOME_3RD_DOWN_CONV_AGAINST_MIN': 0,
                'HOME_TOP_MEAN': 0, 
                'HOME_TOP_STD': 0,
                'HOME_TOP_MAX': 0,
                'HOME_TOP_MIN': 0,
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
                'HOME_PASS_BIG_GAME_W': 0, # spread of -2.5 or tighter
                'HOME_PASS_BIG_GAME_L': 0,
                'HOME_PASS_PLAYOFF_W': 0,
                'HOME_PASS_PLAYOFF_L': 0,
                'HOME_PASS_CHAMP_W': 0,
                'HOME_PASS_CHAMP_L': 0,
                'HOME_RUSH_ATT_MEAN': 0, 
                'HOME_RUSH_ATT_STD': 0,
                'HOME_RUSH_ATT_MAX': 0,
                'HOME_RUSH_ATT_MIN': 0,
                'HOME_RUSH_YDS_MEAN': 0, 
                'HOME_RUSH_YDS_STD': 0,
                'HOME_RUSH_YDS_MAX': 0,
                'HOME_RUSH_YDS_MIN': 0,
                'HOME_RUSH_TD_MEAN': 0, 
                'HOME_RUSH_TD_STD': 0,
                'HOME_RUSH_TD_MAX': 0,
                'HOME_RUSH_TD_MIN': 0,
                'HOME_RUSH_1D_MEAN': 0, 
                'HOME_RUSH_1D_STD': 0,
                'HOME_RUSH_1D_MAX': 0,
                'HOME_RUSH_1D_MIN': 0,
                'HOME_RUSH_YBC_MEAN': 0, 
                'HOME_RUSH_YBC_STD': 0,
                'HOME_RUSH_YBC_MAX': 0,
                'HOME_RUSH_YBC_MIN': 0,
                'HOME_RUSH_YBCATT_MEAN': 0, 
                'HOME_RUSH_YBCATT_STD': 0,
                'HOME_RUSH_YBCATT_MAX': 0,
                'HOME_RUSH_YBCATT_MIN': 0,
                'HOME_RUSH_YAC_MEAN': 0, 
                'HOME_RUSH_YAC_STD': 0,
                'HOME_RUSH_YAC_MAX': 0,
                'HOME_RUSH_YAC_MIN': 0,
                'HOME_RUSH_YACATT_MEAN': 0, 
                'HOME_RUSH_YACATT_STD': 0,
                'HOME_RUSH_YACATT_MAX': 0,
                'HOME_RUSH_YACATT_MIN': 0,
                'HOME_RUSH_BRKTKL_MEAN': 0, 
                'HOME_RUSH_BRKTKL_STD': 0,
                'HOME_RUSH_BRKTKL_MAX': 0,
                'HOME_RUSH_BRKTKL_MIN': 0,
                'HOME_RUSH_ATTBR_MEAN': 0, 
                'HOME_RUSH_ATTBR_STD': 0,
                'HOME_RUSH_ATTBR_MAX': 0,
                'HOME_RUSH_ATTBR_MIN': 0,
                'Home-Team-Win': home_team_win,
                'DIV_MATCH': 0,
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
