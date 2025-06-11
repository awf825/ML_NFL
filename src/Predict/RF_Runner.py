import copy
import os

import numpy as np
import pandas as pd
from colorama import Fore, Style, init, deinit
from src.Utils import Expected_Value
from src.Utils import Kelly_Criterion as kc
from src.Utils.tools import get_spread_confidence

import dill

import email, smtplib, ssl

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

subject = "NFL RANDOM FOREST"
sender_email = "aidenwflynn1@gmail.com"
receiver_email = "faiden454@gmail.com"
password = "zowlanqbvmpweydq"

# Create a multipart message and set headers
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = subject

init()

def get_last_filepath(directory):
    """Gets the filepath of the last file in a directory, sorted alphabetically."""

    if not os.path.exists(directory):
        return None

    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if not files:
        return None

    files.sort()
    # print('os.listdir(directory): ', os.listdir(directory))
    return os.path.join(directory, files[-1])

classifier_uo_path = "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/UO"
classifier_ml_path = "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/ML"
regressor_uo_path = "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Regressor/UO"
regressor_ml_path = "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Regressor/ML"

last_classifier_uo_path = get_last_filepath(classifier_uo_path)
last_classifier_ml_path = get_last_filepath(classifier_ml_path)
last_regressor_uo_path = get_last_filepath(regressor_uo_path)
last_regressor_ml_path = get_last_filepath(regressor_ml_path)

if last_classifier_uo_path and last_classifier_ml_path and last_regressor_uo_path and last_regressor_ml_path:
    print("FILE PATHS OF MODELS BEING USED: ")
    print("last_classifier_uo_path", last_classifier_uo_path)
    print("last_classifier_ml_path", last_classifier_ml_path)
    print("last_regressor_uo_path", last_regressor_uo_path)
    print("last_regressor_ml_path", last_regressor_ml_path)
else:
    print("Not all model files found in the directory.")
    exit(0)

with open(last_regressor_uo_path, 'rb') as file: ou_rfr = dill.load(file)
with open(last_classifier_uo_path, 'rb') as file: ou_rfc = dill.load(file)
# with open(last_regressor_ml_path, 'rb') as file: ml_rfr = dill.load(file)
# with open(last_classifier_ml_path, 'rb') as file: ml_rfc = dill.load(file)


with open(
    "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/ML/2025_2_6_87.4_R2_47.0_games_1979-2024.obj", 'rb'
) as file: ml_rfc_79 = dill.load(file)
with open(
    "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/ML/2025_2_6_87.2_R2_56.2_games_1988-2024.obj", 'rb'
) as file: ml_rfc_88 = dill.load(file)
with open(
    "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/ML/2025_2_6_87.3_R2_52.8_games_1996-2024.obj", 'rb'
) as file: ml_rfc_96 = dill.load(file)
with open(
    "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Regressor/ML/2025_2_6_61.5_R2_60.5_games_1979-2024.obj", 'rb'
) as file: ml_rfr_79 = dill.load(file)
with open(
    "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Regressor/ML/2025_2_6_61.1_R2_62.7_games_1988-2024.obj", 'rb'
) as file: ml_rfr_88 = dill.load(file)
with open(
    "/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Regressor/ML/2025_2_6_60.7_R2_62.3_games_1996-2024.obj", 'rb'
) as file: ml_rfr_96 = dill.load(file)

def rf_runner(games_odds, data, todays_games_uo, frame_ml, home_team_odds, away_team_odds, home_team_spread_odds, away_team_spread_odds, overs_only):
    html = ""
    # ml_predictions_array = []
    # ml_classifier_array = []
    ml_predictions_array_79 = []
    ml_classifier_array_79 = []
    ml_predictions_array_88 = []
    ml_classifier_array_88 = []
    ml_predictions_array_96 = []
    ml_classifier_array_96 = []

    for row in data:
        # ml_predictions_array.append(ml_rfr.predict(np.array([row])))
        # ml_classifier_array.append(ml_rfc.predict(np.array([row])))

        ml_predictions_array_79.append(ml_rfr_79.predict(np.array([row])))
        ml_predictions_array_88.append(ml_rfr_88.predict(np.array([row])))
        ml_predictions_array_96.append(ml_rfr_96.predict(np.array([row])))
        ml_classifier_array_79.append(ml_rfc_79.predict(np.array([row])))
        ml_classifier_array_88.append(ml_rfc_88.predict(np.array([row])))
        ml_classifier_array_96.append(ml_rfc_96.predict(np.array([row])))

    frame_uo = copy.deepcopy(frame_ml)
    frame_uo['OU'] = np.asarray(todays_games_uo)
    data = frame_uo.values
    data = data.astype(float)

    ou_predictions_array = []
    ou_classifier_array = []

    for row in data:
        ou_predictions_array.append(ou_rfr.predict(np.array([row])))
        ou_classifier_array.append(ou_rfc.predict(np.array([row])))

    count = 0

    if overs_only:
        for game in games_odds:
            home_team = game['home']
            away_team = game['away']

            over_confidence = ou_predictions_array[count]
            over_confidence = round(over_confidence[0] * 100, 1)
            classifier_confidence = round(ou_classifier_array[count][0] * 100, 1)

            html += "<hr/>"
            if over_confidence >= 50:
                html += "<h5 style='color:blue;'>" + home_team + ' vs ' + away_team + ': ' + 'OVER ' + str(todays_games_uo[count]) + f" ({over_confidence}%)" + "</h5>"
                print(
                    home_team + ' vs ' + away_team + ': ' +
                    Fore.MAGENTA + 'OVER ' + Style.RESET_ALL + str(todays_games_uo[count]) + Style.RESET_ALL + Fore.CYAN + f" ({over_confidence}%)" + Style.RESET_ALL)
                if classifier_confidence >= 50:
                    html += "<h6 style='color:green;'>Classified.</h6>"
                    print(Fore.GREEN + 'Classifier is in agreement' + Style.RESET_ALL)
                else:
                    html += "<h6 style='color:red;'>Not Classified!</h6>"
                    print(Fore.RED + 'Classifier is not in agreement!!!' + Style.RESET_ALL)
            else:
                html += "<h5 style='color:red;'>" + home_team + ' vs ' + away_team + ': ' + 'UNDER ' + str(todays_games_uo[count]) + f" ({over_confidence}%)" + "</h5>"
                print(
                    home_team + ' vs ' + away_team + ': ' +
                    Fore.BLUE + 'UNDER ' + Style.RESET_ALL + str(todays_games_uo[count]) + Style.RESET_ALL + Fore.CYAN + f" ({over_confidence}%)" + Style.RESET_ALL)
                if classifier_confidence < 50:
                    html += "<h6 style='color:green;'>Classified.</h6>"
                    print(Fore.GREEN + 'Classifier is in agreement' + Style.RESET_ALL)
                else:
                    html += "<h6 style='color:red;'>Not Classified!</h6>"
                    print(Fore.RED + 'Classifier is not in agreement!!!' + Style.RESET_ALL)
            count += 1

        html_part = MIMEText(html, "html")
        message.attach(html_part)
        # message = '\n'.join(html_payload)
        text = message.as_string()

        # Log in to server using secure context and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, text)

        exit(0)
        
    else:
        for game in games_odds:
            home_team = game['home']
            away_team = game['away']

            winner_confidence = ml_predictions_array_79[count]
            winner_confidence = round(winner_confidence[0] * 100, 1)
            # winner_confidence_79 = ml_predictions_array_79[count]
            # winner_confidence_79 = round(winner_confidence_79[0] * 100, 1)
            winner_confidence_88 = ml_predictions_array_88[count]
            winner_confidence_88 = round(winner_confidence_88[0] * 100, 1)
            winner_confidence_96 = ml_predictions_array_96[count]
            winner_confidence_96 = round(winner_confidence_96[0] * 100, 1)

            classifier_confidence_79 = round(ml_classifier_array_79[count][0] * 100, 1)
            classifier_confidence_88 = round(ml_classifier_array_88[count][0] * 100, 1)
            classifier_confidence_96 = round(ml_classifier_array_96[count][0] * 100, 1)

            ev_home = ev_away = 0
            try:
                if home_team_odds[count] and away_team_odds[count]:
                    ev_home = float(Expected_Value.expected_value(ml_predictions_array_79[count][0], int(home_team_odds[count])))
                    ev_away = float(Expected_Value.expected_value(1-ml_predictions_array_79[count][0], int(away_team_odds[count])))

                bankroll_fraction_home = str(kc.calculate_kelly_criterion(home_team_odds[count], ml_predictions_array_79[count][0]))+"%"
                bankroll_fraction_away = str(kc.calculate_kelly_criterion(away_team_odds[count], 1-ml_predictions_array_79[count][0]))+"%"

                html += "<hr/>"
                # xxx_spread_odds = [points, price]
                if winner_confidence >= 50:
                    spread_conf_favorite = get_spread_confidence(home_team_spread_odds[count][0], winner_confidence)
                    html += f"<h5> {home_team}: {winner_confidence}% (1979); {winner_confidence_88}% (1988); {winner_confidence_96}% (1996); </h5>"
                    html += f"<p>{home_team} will cover {home_team_spread_odds[count][0]} ({home_team_spread_odds[count][1]}) {round(spread_conf_favorite,2)}% of the time.</p>" 
                    html += f"{home_team} spread kelly: {kc.calculate_kelly_criterion(home_team_spread_odds[count][1], .01*spread_conf_favorite)}%"
                    html += f"<p>{away_team} will cover {away_team_spread_odds[count][0]} ({away_team_spread_odds[count][1]}) {100-round(spread_conf_favorite,2)}% of the time.</p>" 
                    html += f"{away_team} spread kelly: {kc.calculate_kelly_criterion(away_team_spread_odds[count][1], 1-.01*spread_conf_favorite)}%"
                    print(
                        Fore.GREEN + home_team + Style.RESET_ALL + Fore.CYAN + f" ({winner_confidence}%)" + Style.RESET_ALL + ' vs ' + Fore.RED + away_team + Style.RESET_ALL
                    )
                    classifier_set = {classifier_confidence_79, classifier_confidence_88, classifier_confidence_96}
                    html += f"<p>Classifier Set: {classifier_set} </p>"
                    # if classifier_confidence >= 50:
                    #     html += "<h6 style='color:green;'>Classified.</h6>"
                    #     print(Fore.GREEN + 'Classifier is in agreement.' + Style.RESET_ALL)
                    # else:
                    #     html += "<h6 style='color:red;'>Not Classified!</h6>"
                    #     print(Fore.RED + 'Classifier is not in agreement!!!' + Style.RESET_ALL)
                else:
                    spread_conf_favorite = get_spread_confidence(away_team_spread_odds[count][0], 100-winner_confidence)
                    html += f"<h5> {away_team}: {100-winner_confidence}% (1979); {100-winner_confidence_88}% (1988); {100-winner_confidence_96}% (1996); </h5>"
                    html += f"<p>{away_team} will cover {away_team_spread_odds[count][0]} ({away_team_spread_odds[count][1]}) {round(spread_conf_favorite,2)}% of the time.</p>" 
                    html += f"{away_team} spread kelly: {kc.calculate_kelly_criterion(away_team_spread_odds[count][1], .01*spread_conf_favorite)}%"
                    html += f"<p>{home_team} will cover {home_team_spread_odds[count][0]} ({home_team_spread_odds[count][1]}) {100-round(spread_conf_favorite,2)}% of the time.</p>" 
                    html += f"{home_team} spread kelly: {kc.calculate_kelly_criterion(home_team_spread_odds[count][1], 1-.01*spread_conf_favorite)}%"
                    print(
                        Fore.RED + home_team + Style.RESET_ALL + Fore.CYAN + f" ({winner_confidence}%)" + Style.RESET_ALL + ' vs ' + Fore.GREEN + away_team + Style.RESET_ALL
                    )
                    classifier_set = {classifier_confidence_79, classifier_confidence_88, classifier_confidence_96}
                    html += f"<p>Classifier Set: {classifier_set} </p>"
                    # if classifier_confidence < 50:
                    #     html += "<h6 style='color:green;'>Classified.</h6>"
                    #     print(Fore.GREEN + 'Classifier is in agreement.' + Style.RESET_ALL)
                    # else:
                    #     html += "<h6 style='color:red;'>Not Classified!</h6>"
                    #     print(Fore.RED + 'Classifier is not in agreement!!!' + Style.RESET_ALL)
                if ev_home > 0:
                    print(f"-> {bankroll_fraction_home} on {home_team}")
                if ev_away > 0:
                    print(f"-> {bankroll_fraction_away} on {away_team}")
                print("\n")
            except Exception as err:
                print("Error at Value build: ", err)
            count += 1

        html_part = MIMEText(html, "html")
        message.attach(html_part)
        # message = '\n'.join(html_payload)
        text = message.as_string()

        # Log in to server using secure context and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, text)

    deinit()
