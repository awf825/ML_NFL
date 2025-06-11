from datetime import datetime
import json
import re
import numpy as np
import requests
import pandas as pd
from .Dictionaries import team_index_current

games_header = {
    'user-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/57.0.2987.133 Safari/537.36',
    'Dnt': '1',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en',
    'origin': 'http://stats.nba.com',
    'Referer': 'https://github.com'
}

data_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nba.com/',
    'Connection': 'keep-alive'
}

headers = [
    'TEAM_ID',
    'TEAM_NAME',
    'FD',
    'RUSH_ATT',
    'RUSH_YDS',
    'RUSH_TD',
    'PASS_COMP',
    'PASS_ATT',
    'PASS_YDS',
    'PASS_TD',
    'PASS_INT',
    'SACKED',
    'SACKED_YDS',
    'TOTAL_YDS',
    'FUMBLES',
    'FUMBLES_LOST',
    'TO',
    'PENALTIES',
    'PENALTY_YARDS',
    '3RD_CONV',
    'TOP',
]

data_url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/teams/{team}/statistics"

def find_object_by_key_value(array_of_objects, key, value):
    for obj in array_of_objects:
        if key in obj and obj[key] == value:
            return obj
    return None

def get_df_data(week):
    rows = []
    if week > 0:
        year = 2024
    else: 
        year = 2023
    for team,id in team_index_current.items():
        # row = []
        # print('team: ', team)
        # team_stats_json = get_json_data(f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/teams/{id}/statistics")
        team_stats_json = get_json_data(f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/2/teams/{id}/statistics")
        categories = team_stats_json['splits']['categories']

        divider = find_object_by_key_value(categories[0]['stats'], "name", "gamesPlayed")['value']

        # TODO: UPDATE STATS TO PER GAME INSTEAD OF TOTALS

        FD = 0
        RUSH_ATT = 0
        RUSH_YDS = 0
        RUSH_TD = 0
        PASS_COMP = 0
        PASS_ATT = 0
        PASS_YDS = 0
        PASS_TD = 0
        PASS_INT = 0
        SACKED = 0
        SACKED_YDS = 0
        FUMBLES = 0
        FUMBLES_LOST = 0
        TO = 0
        PENALTIES = 0
        PENALTY_YARDS = 0
        RD_CONV = 0
        TOP = 0

        for category in categories:
            if category['name'] == "general":
                FUMBLES = find_object_by_key_value(category['stats'], "name", "fumbles")['value'] / divider
                FUMBLES_LOST = find_object_by_key_value(category['stats'], "name", "fumblesLost")['value'] / divider
                # FUMBLES = category['stats'][0]["value"]
                # FUMBLES_LOST = category['stats'][1]["value"]

            if category['name'] == "passing":
                PASS_COMP = find_object_by_key_value(category['stats'], "name", "completions")['value'] / divider
                # print("find_object_by_key_value(category['stats'], 'name', 'interceptions'): ", find_object_by_key_value(category['stats'], "name", "interceptions"))
                # PASS_COMP = category['stats'][2]["value"]
                PASS_INT = find_object_by_key_value(category['stats'], "name", "interceptions")['value'] / divider
                # PASS_INT = category['stats'][5]["value"]
                PASS_YDS = find_object_by_key_value(category['stats'], "name", "netPassingYards")['value'] / divider
                # PASS_YDS = category['stats'][8]["value"]
                PASS_ATT = find_object_by_key_value(category['stats'], "name", "passingAttempts")['value'] / divider
                # PASS_ATT = category['stats'][12]["value"]
                PASS_TD = find_object_by_key_value(category['stats'], "name", "passingTouchdowns")['value'] / divider
                # PASS_TD = category['stats'][18]["value"]
                SACKED = find_object_by_key_value(category['stats'], "name", "sacks")['value'] / divider
                # SACKED = category['stats'][24]["value"]
                SACKED_YDS = find_object_by_key_value(category['stats'], "name", "sackYardsLost")['value'] / divider
                # SACKED_YDS = category['stats'][25]["value"]

            if category['name'] == "rushing":
                RUSH_ATT = find_object_by_key_value(category['stats'], "name", "rushingAttempts")['value'] / divider
                # RUSH_ATT = category['stats'][6]['value']
                RUSH_TD = find_object_by_key_value(category['stats'], "name", "rushingTouchdowns")['value'] / divider
                # RUSH_TD = category['stats'][11]['value']
                RUSH_YDS = find_object_by_key_value(category['stats'], "name", "rushingYardsPerGame")['value']
                # RUSH_YDS = category['stats'][12]['value']

            if category['name'] == "miscellaneous":
                FD = find_object_by_key_value(category['stats'], "name", "firstDownsPerGame")['value']
                # FD = category['stats'][0]['value']
                TOP = find_object_by_key_value(category['stats'], "name", "possessionTimeSeconds")['value'] / 60 / divider
                # TOP = find_object_by_key_value(category['stats'], "name", "possessionTimeSeconds")['value'] / 60
                RD_CONV = find_object_by_key_value(category['stats'], "name", "thirdDownConvPct")['value'] / 100
                # RD_CONV = category['stats'][14]['value'] / 100
                TO = find_object_by_key_value(category['stats'], "name", "totalGiveaways")['value'] / divider
                # TO = TO_GET['value']
                PENALTIES = find_object_by_key_value(category['stats'], "name", "totalPenalties")['value'] / divider
                # PENALTIES = category['stats'][divider]['value']
                PENALTY_YARDS = find_object_by_key_value(category['stats'], "name", "totalPenaltyYards")['value'] / divider
                # PENALTY_YARDS = category['stats'][18]['value']
        rows.append([
            id,
            team,
            FD,
            RUSH_ATT,
            RUSH_YDS,
            RUSH_TD,
            PASS_COMP,
            PASS_ATT,
            PASS_YDS,
            PASS_TD,
            PASS_INT,
            SACKED,
            SACKED_YDS,
            RUSH_YDS+PASS_YDS,
            FUMBLES,
            FUMBLES_LOST,
            TO,
            PENALTIES,
            PENALTY_YARDS,
            RD_CONV,
            TOP
        ])
    df = pd.DataFrame(data=rows, columns=headers)
    # df.set_index('TEAM_ID', inplace=True)
    return df

def get_json_data(url):
    raw_data = requests.get(url)
    try:
        json = raw_data.json()
    except Exception as e:
        print('exception @ get_json_data: ', e)
        return {}
    return json


def get_todays_games_json(url):
    raw_data = requests.get(url, headers=games_header)
    json = raw_data.json()
    return json.get('gs').get('g')

def to_data_frame(data):
    try:
        print('data @ to dataframe: ', data)
        data_list = data[0]
    except Exception as e:
        print(e)
        return pd.DataFrame(data={})
    headers = [
        'TEAM_ID',
        'TEAM_NAME',
        'FD',
        'RUSH_ATT',
        'RUSH_YDS',
        'RUSH_TD',
        'PASS_COMP',
        'PASS_ATT',
        'PASS_YDS',
        'PASS_TD',
        'PASS_INT',
        'SACKED',
        'SACKED_YDS',
        'TOTAL_YDS',
        'FUMBLES',
        'FUMBLES_LOST',
        'TO',
        'PENALTIES',
        'PENALTY_YARDS',
        '3RD_CONV',
        'TOP',
    ]
    return pd.DataFrame(data=data_list.get('rowSet'), columns=headers)

def create_todays_games(input_list):
    games = []
    for game in input_list:
        home = game.get('h')
        away = game.get('v')
        home_team = home.get('tc') + ' ' + home.get('tn')
        away_team = away.get('tc') + ' ' + away.get('tn')
        games.append([home_team, away_team])
    return games


def create_todays_games_from_odds(input_dict):
    games = []
    for input in input_dict:
        home_team = input['home_team']
        away_team = input['away_team']

        if home_team not in team_index_current or away_team not in team_index_current:
            print('home_team not in index')
            continue
        else:
            games.append([home_team, away_team])
    return games

def create_todays_games_and_odds_from_odds_api(input_dict):
    games_odds = []
    for input in input_dict:
        home_team = input['home_team']
        away_team = input['away_team']
        for book in input['bookmakers']:
            if book['key'] == "fanduel":
                for market in book['markets']:
                    if market['key'] == "h2h":
                        if market['outcomes'][0]['name'] == away_team:
                            away_team_ml_price = market['outcomes'][0]['price']
                            home_team_ml_price = market['outcomes'][1]['price']
                        elif market['outcomes'][0]['name'] == home_team:
                            home_team_ml_price = market['outcomes'][0]['price']
                            away_team_ml_price = market['outcomes'][1]['price']
                    elif market['key'] == "totals":
                        if market['outcomes'][0]['name'] == "Over":
                            over_price = market['outcomes'][0]['price']
                            points = market['outcomes'][0]['point']
                        if market['outcomes'][1]['name'] == "Under":
                            under_price = market['outcomes'][1]['price']
                            points = market['outcomes'][1]['point']
                    elif market['key'] == "spreads":
                        if market['outcomes'][0]['name'] == away_team:
                            away_team_spread_price = market['outcomes'][0]['price']
                            away_team_spread_points = market['outcomes'][0]['point']
                            home_team_spread_price = market['outcomes'][1]['price']
                            home_team_spread_points = market['outcomes'][1]['point']
                        elif market['outcomes'][0]['name'] == home_team:
                            home_team_spread_price = market['outcomes'][0]['price']
                            home_team_spread_points = market['outcomes'][0]['point']
                            away_team_spread_price = market['outcomes'][1]['price']
                            away_team_spread_points = market['outcomes'][1]['point']

        games_odds.append({
            "home": home_team,
            "away": away_team,
            "home_ml": home_team_ml_price,
            "away_ml": away_team_ml_price,
            "total": points,
            "over_price": over_price,
            "under_price": under_price,
            "home_spread_points": home_team_spread_points,
            "home_spread_price": home_team_spread_price,
            "away_spread_points": away_team_spread_points,
            "away_spread_price": away_team_spread_price
        })
    return games_odds

def get_date(date_string):
    year1,month,day = re.search(r'(\d+)-\d+-(\d\d)(\d\d)', date_string).groups()
    year = year1 if int(month) > 8 else int(year1) + 1
    return datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')

def get_polynomial_y(spread):
    # Sample data
    x = np.array([0.5,1.5,2.5,3.5,4.5,5.5,6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5, 20.5, 21.5, 22.5, 23.5, 24.5, 25.5, 26.5, 27.5, 28.5, 29.5, 30.5, 31.5, 32.5, 33.5, 34.5, 35.5, 36.5, 37.5, 38.5])
    y = np.array([.002,.045,.085,.232,.281,.316,.38,.47,.507,.522,.579,.603,.619,.645,.695,.711,.732,.764,.787,.798,.821,.849,.859,.87,.892,.903,.911,.923,.94,.946,.95,.962,.966,.97,.974,.98,.983,.986,.99])
    # The slope at a specific point x_value
    # x_value = 2.5
    # slope = 2 * coeffs[0] * x_value + coeffs[1]
    # print("Slope at x =", x_value, "is", slope)
    coeffs = np.polyfit(x, y, 2)
    poly_func = np.poly1d(coeffs)
    # Get the y-value for a new x-value (e.g., x = 6)
    return poly_func(spread)


def get_spread_confidence(s, conf):
    W = conf
    G = get_polynomial_y(abs(s))
    if s > 0:
        # if at this point the spread is positive we want to use the underdog formula
        L = 100-W
        return ( (L*G)+W )
    else:
        # otherwise, we want to use the favorite formula
        return ( W*(1.0-G) )
    

# POINT DIFF PROB
# TIE -> .2%
# 1 -> 4%
# 2 -> 8%
# 3 -> 23%
# 4 -> 28%
# 5 -> 31%
# 6 -> 38%
# 7 -> 47%
# 8 -> 50%
# 9 -> 52%
