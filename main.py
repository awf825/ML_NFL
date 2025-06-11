import argparse
import datetime
import json
from dateutil import parser
from datetime import timedelta

import pandas as pd
import tensorflow as tf
from colorama import Fore, Style

from src.DataProviders.SbrOddsProvider import SbrOddsProvider
# from src.Predict import XGBoost_Runner
from src.Predict import RF_Runner
from src.Utils.Dictionaries import team_index_current
from src.Utils.tools import create_todays_games_and_odds_from_odds_api, get_json_data, to_data_frame, get_df_data
from src.Utils.Kelly_Criterion import decimal_to_american

now = datetime.datetime.now(datetime.timezone.utc).isoformat()
now_parsed = parser.isoparse(now)
now_ref = f"{now_parsed.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-7]}Z"
now_parsed += timedelta(days=7)
then_ref = f"{now_parsed.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-7]}Z"

odds_url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/?apiKey=411db26899936c6f4079cdb15f258d6b&regions=us&markets=h2h,totals,spreads&commenceTimeFrom={now_ref}&commenceTimeTo={then_ref}"
# odds_url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/?apiKey=411db26899936c6f4079cdb15f258d6b&regions=us&markets=h2h,totals&commenceTimeFrom={then_ref}"
# season stats url for chicago bears (team 3)
data_url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/teams/3/statistics"


def createTodaysGames(games_odds, df):
    todays_games_uo = []
    home_team_odds = []
    away_team_odds = []
    # multi dimensional array: # [points, price]
    home_team_spread_odds = []
    away_team_spread_odds = []
    match_data = []

    for go in games_odds:
        home_team = go['home']
        away_team = go['away']
        if home_team not in team_index_current or away_team not in team_index_current:
            continue
        else:
            todays_games_uo.append(go['total'])
            home_team_odds.append(decimal_to_american(go['home_ml']))
            away_team_odds.append(decimal_to_american(go['away_ml']))

            home_team_spread_odds.append([go['home_spread_points'], decimal_to_american(go['home_spread_price'])])
            away_team_spread_odds.append([go['away_spread_points'], decimal_to_american(go['away_spread_price'])])

        home_idx = 0
        away_idx = 0
        if home_team == 'Jacksonville Jaguars':
            home_idx = team_index_current.get(home_team)-2
        elif home_team == 'Baltimore Ravens' or home_team == 'Houston Texans':
            home_idx = team_index_current.get(home_team)-3
        else:
            home_idx = team_index_current.get(home_team)-1

        if away_team == 'Jacksonville Jaguars':
            away_idx = team_index_current.get(away_team)-2
        elif away_team == 'Baltimore Ravens' or away_team == 'Houston Texans':
            away_idx = team_index_current.get(away_team)-3
        else:
            away_idx = team_index_current.get(away_team)-1

        home_team_series = df.iloc[home_idx]
        away_team_series = df.iloc[away_idx]

        stats = pd.concat([away_team_series, home_team_series])

        match_data.append(stats)

    games_data_frame = pd.concat(match_data, ignore_index=True, axis=1)
    games_data_frame = games_data_frame.T

    frame_ml = games_data_frame.drop(columns=['TEAM_ID', 'TEAM_NAME'])
    data = frame_ml.values
    data = data.astype(float)

    return data, todays_games_uo, frame_ml, home_team_odds, away_team_odds, home_team_spread_odds, away_team_spread_odds


def main():
    if args.odds:
        # odds = SbrOddsProvider(sportsbook=args.odds).get_odds()
        # helper tool to get games from odds, will need to fix up current index
        odds = get_json_data(odds_url)
        # print('book: ', json.dumps(odds, indent=4))
        # exit(0)
        games_odds = create_todays_games_and_odds_from_odds_api(odds)
        if len(games_odds) == 0:
            print("No games found.")
            return
        else:
            print(f"------------------{args.odds} odds data------------------")
            for go in games_odds:
                print(f"{go['away']} ({decimal_to_american(go['away_ml'])}) @ {go['home']} ({decimal_to_american(go['home_ml'])})")
    else:
        print('must select sportsbook')
        return

    if args.week:
        w = int(args.week)
    else:
        w = 0

    df = get_df_data(w)

    # df = to_data_frame(games_odds)
    data, todays_games_uo, frame_ml, home_team_odds, away_team_odds, home_team_spread_odds, away_team_spread_odds = createTodaysGames(games_odds, df)
    
    if args.rf:
        print("---------------RANDOM FOREST Model Predictions---------------")
        RF_Runner.rf_runner(games_odds, data, todays_games_uo, frame_ml, home_team_odds, away_team_odds, home_team_spread_odds, away_team_spread_odds, args.overs)
        print("-------------------------------------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Model to Run')
    parser.add_argument('-rf', action='store_true', help='Run with Random Forest Model')
    parser.add_argument('-odds', help='Sportsbook to fetch from. (fanduel, draftkings, betmgm, pointsbet, caesars, wynn, bet_rivers_ny')
    parser.add_argument('-week', help="Run small sample size of current season (up to given week) against trained model, as opposed to last season")
    parser.add_argument('-overs', action='store_true', help="Only display overs")
    # parser.add_argument('-xgb', action='store_true', help='Run with XGBoost Model')
    # parser.add_argument('-kc', action='store_true', help='Calculates percentage of bankroll to bet based on model edge')
    # parser.add_argument('-bank', help="Bankroll Value (int)")
    # parser.add_argument('-conf', help="The real win percent of how the model is performing on moneylines (float)")
    args = parser.parse_args()
    main()
