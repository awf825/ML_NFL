import argparse
import os
import random
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup, Comment
from selenium import webdriver

from abbrev import team_abbrev_index, team_abbrev_index_1996, team_abbrev_index_1987

parser = argparse.ArgumentParser(description='Model to Run')
parser.add_argument('-week', help="Most recent week of season (int)")
args = parser.parse_args()

# driver = webdriver.Chrome()

from tqdm import tqdm

sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.tools import get_json_data, to_data_frame

years = [2018,2019,2020,2021,2022,2023,2024]
weeks = [1,2,3,4]

games_con = sqlite3.connect("../../Data/games.sqlite")

table_headers = [
    'VIS_TEAM_NAME',
    'HOME_TEAM_NAME',
    'VIS_SCORE',
    'VIS_FD',
    'VIS_RUSH_ATT',
    'VIS_RUSH_YDS',
    'VIS_RUSH_TD',
    'VIS_PASS_COMP',
    'VIS_PASS_ATT',
    'VIS_PASS_YDS',
    'VIS_PASS_TD',
    'VIS_PASS_INT',
    'VIS_SACKED',
    'VIS_SACKED_YDS',
    'VIS_TOTAL_YDS',
    'VIS_FUMBLES',
    'VIS_FUMBLES_LOST',
    'VIS_TO',
    'VIS_PENALTIES',
    'VIS_PENALTY_YARDS',
    'VIS_3RD_CONV',
    'VIS_TOP',
    'HOME_SCORE',
    'HOME_FD',
    'HOME_RUSH_ATT',
    'HOME_RUSH_YDS',
    'HOME_RUSH_TD',
    'HOME_PASS_COMP',
    'HOME_PASS_ATT',
    'HOME_PASS_YDS',
    'HOME_PASS_TD',
    'HOME_PASS_INT',
    'HOME_SACKED',
    'HOME_SACKED_YDS',
    'HOME_TOTAL_YDS',
    'HOME_FUMBLES',
    'HOME_FUMBLES_LOST',
    'HOME_TO',
    'HOME_PENALTIES',
    'HOME_PENALTY_YARDS',
    'HOME_3RD_CONV',
    'HOME_TOP',
    'Home-Team-Win',
    'SCORE',
    'OU',
    'OU_COVER'
]

for year in years:
    df_data = []
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
                # page_source = driver.get(game_url)
                game_req = Request(game_url)
                game_response = urlopen(game_req)

                game_soup = BeautifulSoup(game_response, features="html.parser")

                scoring_table = game_soup.find('table', id="scoring")
                last_row = scoring_table("tr")[-1]
                VIS_SCORE = last_row.find('td', {'data-stat':'vis_team_score'}).get_text()
                HOME_SCORE = last_row.find('td', {'data-stat':'home_team_score'}).get_text()

                # could use more of this info in the future...

                scoring_table = game_soup.find('table', id="scoring")
                last_row = scoring_table("tr")[-1]

                # Find the commented-out HTML
                comments = game_soup.find_all(string=lambda text: isinstance(text, Comment))

                vis = ''
                home = ''
                OU = 0
                OU_COVER = 0
                VIS_FD = ""
                HOME_FD = ""
                VIS_RUSH_ATT = ""
                VIS_RUSH_YDS = ""
                VIS_RUSH_TD = ""
                HOME_RUSH_ATT = ""
                HOME_RUSH_YDS = ""
                HOME_RUSH_TD = ""
                VIS_PASS_COMP = ""
                VIS_PASS_ATT = ""
                VIS_PASS_YDS = ""
                VIS_PASS_TD = ""
                VIS_PASS_INT = ""
                HOME_PASS_COMP = ""
                HOME_PASS_ATT = ""
                HOME_PASS_YDS = ""
                HOME_PASS_TD = ""
                HOME_PASS_INT = ""
                VIS_SACKED = ""
                VIS_SACKED_YDS = ""
                HOME_SACKED = ""
                HOME_SACKED_YDS = ""
                VIS_TOTAL_YDS = ""
                HOME_TOTAL_YDS = ""
                VIS_FUMBLES = ""
                VIS_FUMBLES_LOST = ""
                HOME_FUMBLES = ""
                HOME_FUMBLES_LOST = ""
                VIS_TO = ""
                HOME_TO = ""
                VIS_PENALTIES = ""
                VIS_PENALTY_YARDS = ""
                HOME_PENALTIES = ""
                HOME_PENALTY_YARDS = ""
                VIS_3RD_CONV = 0
                HOME_3RD_CONV = 0
                VIS_TOP = 0
                HOME_TOP = 0
                OU = 0
                OU_COVER = 0
                home_team_win = 0
                for comment in comments:
                    x = comment.extract()
                    # # comment_soup =  BeautifulSoup(x, 'html.parser')
                    # # table = comment_soup.find('table', id="game_info")
                    # # last_row = table.findAll("tr")[-1]
                    if "game_info" in x:
                        try:
                            comment_soup =  BeautifulSoup(x, 'html.parser')
                            table = comment_soup.find('table', id="game_info")
                            last_row = table.findAll("tr")[-1]
                            # exit(0)
                            # could use more of this info in the future...
                            OU = last_row.find('td', {'data-stat':'stat'}).get_text().split(" ")[0]
                            if int(VIS_SCORE)+int(HOME_SCORE) > float(OU):
                                OU_COVER = 1.0
                            else:
                                OU_COVER = 0.0
                        except:
                            OU_COVER = 0.0
                            OU = 0

                    if "team_stats" in x:
                        comment_soup =  BeautifulSoup(x, 'html.parser')
                        table = comment_soup.find('table', id="team_stats")
                        rows = table.findAll("tr")

                        vis = rows[0].find('th', {'data-stat':'vis_stat'}).get_text()
                        home = rows[0].find('th', {'data-stat':'home_stat'}).get_text()

                        vis = team_abbrev_index.get(vis)
                        home = team_abbrev_index.get(home)

                        for row in rows[1:]:
                            table_row = []
                            scraped_rows = row.findAll(["td", "th"])
                            # triples of column name, visiting stat, home stat
                            column_name = scraped_rows[0].get_text()
                            vis_stat = scraped_rows[1].get_text()
                            home_stat = scraped_rows[2].get_text()
                            if column_name == "First Downs":
                                VIS_FD = vis_stat
                                HOME_FD = home_stat
                            elif column_name == "Rush-Yds-TDs":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                VIS_RUSH_ATT = vis_split[0]
                                VIS_RUSH_YDS = vis_split[1]
                                VIS_RUSH_TD = vis_split[2]
                                HOME_RUSH_ATT = home_split[0]
                                HOME_RUSH_YDS = home_split[1]
                                HOME_RUSH_TD = home_split[2]
                            elif column_name == "Cmp-Att-Yd-TD-INT":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                VIS_PASS_COMP = vis_split[0]
                                VIS_PASS_ATT = vis_split[1]
                                VIS_PASS_YDS = vis_split[2]
                                VIS_PASS_TD = vis_split[3]
                                VIS_PASS_INT = vis_split[4]
                                HOME_PASS_COMP = home_split[0]
                                HOME_PASS_ATT = home_split[1]
                                HOME_PASS_YDS = home_split[2]
                                HOME_PASS_TD = home_split[3]
                                HOME_PASS_INT = home_split[4]
                            elif column_name == "Sacked-Yards":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                VIS_SACKED = vis_split[0]
                                VIS_SACKED_YDS = vis_split[1]
                                HOME_SACKED = home_split[0]
                                HOME_SACKED_YDS = home_split[1]

                            elif column_name == "Total Yards":
                                VIS_TOTAL_YDS = vis_stat
                                HOME_TOTAL_YDS = home_stat
                            elif column_name == "Fumbles-Lost":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                VIS_FUMBLES = vis_split[0]
                                VIS_FUMBLES_LOST = vis_split[1]
                                HOME_FUMBLES = home_split[0]
                                HOME_FUMBLES_LOST = home_split[1]

                            elif column_name == "Turnovers":
                                VIS_TO = vis_stat
                                HOME_TO = home_stat
                            elif column_name == "Penalties-Yards":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                VIS_PENALTIES = vis_split[0]
                                VIS_PENALTY_YARDS = vis_split[1]
                                HOME_PENALTIES = home_split[0]
                                HOME_PENALTY_YARDS = home_split[1]
                            elif column_name == "Third Down Conv.":
                                vis_split = vis_stat.split("-")
                                home_split = home_stat.split("-")

                                if int(vis_split[1]) > 0:
                                    VIS_3RD_CONV = int(vis_split[0]) / int(vis_split[1])
                                else:
                                    VIS_3RD_CONV = 0.0

                                if int(home_split[1]) > 0:
                                    HOME_3RD_CONV = int(home_split[0]) / int(home_split[1])
                                else: 
                                    HOME_3RD_CONV = 0.0
                            elif column_name == "Time of Possession":
                                vis_split = vis_stat.split(":")
                                home_split = home_stat.split(":")

                                # round to nearest minute
                                if int(vis_split[1]) > 30:
                                    VIS_TOP = int(vis_split[0])+1
                                else:
                                    VIS_TOP = int(vis_split[0])

                                if int(home_split[1]) > 30:
                                    HOME_TOP = int(home_split[0])+1
                                else:
                                    HOME_TOP = int(home_split[0])
                            else: 
                                print('column invalid, OR NOT USED')

                            # print('column_name, vis_stat, home_state: ', column_name, vis_stat, home_stat)

                        if int(HOME_SCORE) > int(VIS_SCORE):
                            home_team_win = 1.0 
                        else:
                            home_team_win = 0.0
            except Exception: 
                print('could not get game data')
                
            df_data.append({
                'VIS_TEAM_NAME': vis,
                'HOME_TEAM_NAME': home,
                'VIS_SCORE': VIS_SCORE,
                'VIS_FD': VIS_FD,
                'VIS_RUSH_ATT': VIS_RUSH_ATT,
                'VIS_RUSH_YDS': VIS_RUSH_YDS,
                'VIS_RUSH_TD': VIS_RUSH_TD, 
                'VIS_PASS_COMP': VIS_PASS_COMP,
                'VIS_PASS_ATT': VIS_PASS_ATT,
                'VIS_PASS_YDS': VIS_PASS_YDS,
                'VIS_PASS_TD': VIS_PASS_TD,
                'VIS_PASS_INT': VIS_PASS_INT,
                'VIS_SACKED': VIS_SACKED,
                'VIS_SACKED_YDS': VIS_SACKED_YDS,
                'VIS_TOTAL_YDS': VIS_TOTAL_YDS,
                'VIS_FUMBLES': VIS_FUMBLES,
                'VIS_FUMBLES_LOST': VIS_FUMBLES_LOST,
                'VIS_TO': VIS_TO,
                'VIS_PENALTIES': VIS_PENALTIES,
                'VIS_PENALTY_YARDS': VIS_PENALTY_YARDS,
                'VIS_3RD_CONV': VIS_3RD_CONV,
                'VIS_TOP': VIS_TOP,
                'HOME_SCORE': HOME_SCORE,
                'HOME_FD': HOME_FD,
                'HOME_RUSH_ATT': HOME_RUSH_ATT,
                'HOME_RUSH_YDS': HOME_RUSH_YDS,
                'HOME_RUSH_TD': HOME_RUSH_TD,
                'HOME_PASS_COMP': HOME_PASS_COMP,
                'HOME_PASS_ATT': HOME_PASS_ATT,
                'HOME_PASS_YDS': HOME_PASS_YDS,
                'HOME_PASS_TD': HOME_PASS_TD,
                'HOME_PASS_INT': HOME_PASS_INT,
                'HOME_SACKED': HOME_SACKED,
                'HOME_SACKED_YDS': HOME_SACKED_YDS,
                'HOME_TOTAL_YDS': HOME_TOTAL_YDS,
                'HOME_FUMBLES': HOME_FUMBLES,
                'HOME_FUMBLES_LOST': HOME_FUMBLES_LOST,
                'HOME_TO': HOME_TO,
                'HOME_PENALTIES': HOME_PENALTIES,
                'HOME_PENALTY_YARDS': HOME_PENALTY_YARDS,
                'HOME_3RD_CONV': HOME_3RD_CONV,
                'HOME_TOP': HOME_TOP,
                'Home-Team-Win': home_team_win,
                'SCORE': int(VIS_SCORE)+int(HOME_SCORE),
                'OU': OU,
                'OU_COVER': OU_COVER
            })

    df = pd.DataFrame(df_data, columns=table_headers)
    df.to_sql("games_2024", games_con, if_exists="replace")
    # df.to_sql(f"games_1996-{year}", games_con, if_exists="append")
    # df.to_sql(f"games_1988-{year}", games_con, if_exists="append")
    # df.to_sql(f"games_1979-{year}", games_con, if_exists="append")

    df_data = []
