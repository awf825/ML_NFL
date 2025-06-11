import json
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
from sklearn.model_selection import train_test_split, cross_val_score, RandomizedSearchCV, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt

import dill

from datetime import date, datetime, timedelta

def evaluate(model, test_features, test_labels):
    predictions = model.predict(test_features)
    errors = abs(predictions - test_labels)
    mape = 100 * np.mean(errors / test_labels)
    accuracy = 100 - mape
    print('Model Performance')
    print('Average Error: {:0.4f} degrees.'.format(np.mean(errors)))
    print('Accuracy = {:0.2f}%.'.format(accuracy))
    
    return accuracy

year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)

dataset = "games_1979-2024"
# dataset = "games_1988-2024"
# dataset = "games_1979-2024"
con = sqlite3.connect("../../../Data/games.sqlite")
data = pd.read_sql_query(f"select * from \"{dataset}\"", con, index_col="index")
con.close()

data['VIS_TEAM_NAME'] = data['VIS_TEAM_NAME'].apply(lambda x: list(x)[0] )
data['HOME_TEAM_NAME'] = data['HOME_TEAM_NAME'].apply(lambda x: list(x)[0] )

data = data.replace('', np.nan)  # Replace empty strings with NaN
data.dropna(inplace=True)      # Drop rows with NaN values

OU = data['OU_COVER']
total = data['OU']
# data['TEAM_ID.1'] = data['TEAM_ID.1'].apply(lambda x: list(x)[0] )

data.drop(
    ['VIS_TEAM_NAME', 'HOME_TEAM_NAME', 'HOME_SCORE', 'VIS_SCORE', 'SCORE', 'Home-Team-Win', 'OU_COVER', 'OU'], 
    axis=1, 
    inplace=True)

data['OU'] = np.asarray(total)
data = data.to_numpy(dtype=float)


# Split into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.1, random_state=42)

# Create a Random Forest Regressor
rf_classifier = RandomForestRegressor(random_state=42)

# Fit the model on the training data
rf_classifier.fit(X_train, y_train)

# Make predictions on the test data
y_pred = rf_classifier.predict(X_test)

# Example params used from what is gathered in GetParams_ML!!!!

# {'n_estimators': 1000, 'min_samples_split': 2, 'min_samples_leaf': 1, 'max_features': 'log2', 'max_depth': 50, 'bootstrap': False}
# param_grid = {
#     'bootstrap': [False],
#     'max_depth': [50,60,70,80],
#     'max_features': ["log2"],
#     'min_samples_leaf': [1,2,3],
#     'min_samples_split': [2,3,4],
#     'n_estimators': [800,900,1000,1110,1200]
# }

#  {'n_estimators': 1400, 'min_samples_split': 5, 'min_samples_leaf': 2, 'max_features': 'sqrt', 'max_depth': None, 'bootstrap': False}
param_grid = {
    'bootstrap': [False],
    'max_depth': [100,110,120,130],
    'max_features': ["sqrt"],
    'min_samples_leaf': [2,3,4],
    'min_samples_split': [2,3,4],
    'n_estimators': [500,600,700]
}
# Create a based model
rfc = RandomForestRegressor()# Instantiate the grid search model
grid_search = GridSearchCV(estimator = rfc, param_grid = param_grid, 
                          cv = 3, n_jobs = -1, verbose = 2)

grid_search.fit(X_train, y_train)
print("Grid Search Best Params: ", json.dumps(grid_search.best_params_, indent=4))

best_grid = grid_search.best_estimator_
print("\n")
print("Printing grid accuracy...\n")
grid_accuracy = evaluate(best_grid, X_train, y_train)

base_model = RandomForestRegressor(n_estimators = 10, random_state = 42)
base_model.fit(X_train, y_train)
print("\n")
print("Printing base accuracy...\n")
base_accuracy = evaluate(base_model, X_train, y_train)

print('Improvement of {:0.2f}%.'.format( 100 * (grid_accuracy - base_accuracy) / base_accuracy))

# grid_search_params results
# 10-01-2024, games_1988-2024:
# {
#     "bootstrap": false,
#     "max_depth": 120,
#     "max_features": "sqrt",
#     "min_samples_leaf": 2,
#     "min_samples_split": 2,
#     "n_estimators": 500
# }
# 10-01-2024, games_1996-2024:
# {
#   "bootstrap": false,  
#   "max_features": "sqrt", 
#   "min_samples_leaf": 2,
#   "min_samples_split": 2,
#   "n_estimators": 300
# }
# 10-01-2024, games_1979-2024
# {
#     "bootstrap": false,
#     "max_depth": 110,
#     "max_features": "sqrt",
#     "min_samples_leaf": 2,
#     "min_samples_split": 2,
#     "n_estimators": 700
# }