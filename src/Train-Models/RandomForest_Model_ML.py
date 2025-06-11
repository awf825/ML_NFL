import sqlite3
from typing import ByteString
import struct
import time
import seaborn as sns

import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 500)
import xgboost as xgb
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from tqdm import tqdm
import matplotlib.pyplot as plt

import dill

from datetime import date, datetime, timedelta

year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)

# dataset = "games_1996-2024"
dataset = "games_1988-2024"
# dataset = "games_1979-2024"
con = sqlite3.connect("../../Data/games.sqlite")
data = pd.read_sql_query(f"select * from \"{dataset}\"", con, index_col="index")
con.close()

data['VIS_TEAM_NAME'] = data['VIS_TEAM_NAME'].apply(lambda x: list(x)[0] )
data['HOME_TEAM_NAME'] = data['HOME_TEAM_NAME'].apply(lambda x: list(x)[0] )

# data['VIS_3RD_CONV'].astype(float)
# data['HOME_3RD_CONV'].astype(float)
data= data.replace('', np.nan)  # Replace empty strings with NaN
data.dropna(inplace=True)      # Drop rows with NaN values

# data.astype(float)

margin = data['Home-Team-Win']

data.drop(
    ['HOME_TEAM_NAME', 'VIS_TEAM_NAME', 'HOME_SCORE', 'VIS_SCORE', 'SCORE', 'Home-Team-Win', 'OU_COVER', 'OU'],
    axis=1, 
    inplace=True
)


# Split into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.1, random_state=42)

# 10-01-2024, games_1988-2024:
# "bootstrap": false, "max_features": "sqrt", "min_samples_leaf": 2,"min_samples_split": 3,"n_estimators": 600
# 10-01-2024, games_1996-2024:
# "bootstrap": false,  "max_features": "sqrt", "min_samples_leaf": 2,"min_samples_split": 2,"n_estimators": 300
# Create a Random Forest Regressor
rf_regressor = RandomForestRegressor(
    bootstrap=False,
    max_features="sqrt",
    min_samples_leaf=2,
    min_samples_split=3,
    n_estimators=600,
    random_state=42
)
# rf_regressor = RandomForestRegressor(
#     bootstrap=False,
#     max_depth=50,
#     max_features="log2",
#     min_samples_leaf=1,
#     min_samples_split=3,
#     n_estimators=1200,
#     random_state=42
# )

# Fit the model on the training data
rf_regressor.fit(X_train, y_train)

# Make predictions on the test data
y_pred = rf_regressor.predict(X_test)

# print('Accuracy Score when n_estimators equals to 300 : {0:0.3f}'.format(accuracy_score(y_test, y_pred)))

# print('y_pred: ', y_pred)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
avg_value = np.mean(y_test)
relative_error = rmse / avg_value * 100
r2 = r2_score(y_test, y_pred)

print(f'Mean Squared Error (MSE): {mse}')
print(f'Root Mean Squared Error (RMSE): {rmse}')
print(f'R-squared (R2): {r2}')

# X = data.iloc[:,:-1].values
# y = data.iloc[:,-1].values

# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.2, random_state=42)

# rfc = RandomForestClassifier(n_estimators=200, random_state=0)

# # fit the model
# rfc.fit(X_train, y_train)
# y_pred = rfc.predict(X_test)
# print('Accuracy Score when n_estimators equals to 300 : {0:0.3f}'.format(accuracy_score(y_test, y_pred)))

# # Plot residuals
# # residuals = y_test - y_pred
# # plt.scatter(y_pred, residuals)
# # plt.xlabel('Predicted Values')
# # plt.ylabel('Residuals')
# # plt.title('Residuals Plot')
# # plt.axhline(y=0, color='r', linestyle='--')
# # plt.show()

# r2 = r2_score(y_test, y_pred)
# print("r2: ", r2)

# with open(
#     f'/Users/aidenflynn/ML_Python/python-nfl/Models/RF/R2_{round(r2*100, 1)}_RandomForest_{dataset}_{year+month+day}_ML.obj', 
#     'wb'
# ) as f:
#     dill.dump(rf_regressor, f)
with open(
    f'/Users/aidenflynn/ML_Python/python-nfl/Models/RF/{year+month+day}_R2_{round(r2*100, 1)}_{dataset}_ML.obj', 
    'wb'
) as f:
    dill.dump(rf_regressor, f)


# dataset: games_2013-23; Mean Squared Error: 0.11998787911858344
# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.3, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=7, random_state=18)

# dataset: games_2003-23; Mean Squared Error: 0.11017452748578394
# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=200, max_depth=7, random_state=18)

# dataset: games_2000-23; Mean Squared Error: 0.10075630842519973
# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=200, max_depth=13, random_state=18)

# !!!
# dataset: games_1988-2023; Mean Squared Error: 0.09634653192668846
# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=13, random_state=18)
# !!!

# dataset: games_1979-2023; Mean Squared Error: 0.10377265487824501
# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.3, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=200, max_depth=9, random_state=18)

# dataset: games_1996-2023; Mean Squared Error: 0.09546288458540822
# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=200, max_depth=13, random_state=18)

# best R2:
# X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.1, random_state=42)

# # Create a Random Forest Regressor
# rf_regressor = RandomForestRegressor(n_estimators=200, max_depth=15, random_state=18)