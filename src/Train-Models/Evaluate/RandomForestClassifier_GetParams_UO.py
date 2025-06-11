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
from sklearn.ensemble import RandomForestClassifier
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

dataset = "games_1996-2024"
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
rf_classifier = RandomForestClassifier(random_state=42)

# Fit the model on the training data
rf_classifier.fit(X_train, y_train)

# Make predictions on the test data
y_pred = rf_classifier.predict(X_test)

# GET BEST PARAMS THROUGH RANDOM ITERATIONS
n_estimators = [int(x) for x in np.linspace(start = 200, stop = 2000, num = 10)]
# Number of features to consider at every split
max_features = ['log2', 'sqrt']
# Maximum number of levels in tree
max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
max_depth.append(None)
# Minimum number of samples required to split a node
min_samples_split = [2, 5, 10]
# Minimum number of samples required at each leaf node
min_samples_leaf = [1, 2, 4]
# Method of selecting samples for training each tree
bootstrap = [True, False]# Create the random grid

random_grid = {'n_estimators': n_estimators,
               'max_features': max_features,
               'max_depth': max_depth,
               'min_samples_split': min_samples_split,
               'min_samples_leaf': min_samples_leaf,
               'bootstrap': bootstrap}

rf_random = RandomizedSearchCV(estimator = rf_classifier, param_distributions = random_grid, n_iter = 100, cv = 3, verbose=2, random_state=42, n_jobs = -1)

rf_random.fit(X_train, y_train)
print(f'best params. Use these in RF_GetGridAcc: {rf_random.best_params_}')

base_model = RandomForestClassifier(n_estimators = 10, random_state = 42)
base_model.fit(X_train, y_train)
print("Printing base accuracy...\n")
base_accuracy = evaluate(base_model, X_train, y_train)

best_random = rf_random.best_estimator_
print("Printing random accuracy...\n")
random_accuracy = evaluate(best_random, X_train, y_train)
print('Improvement of {:0.2f}%.'.format( 100 * (random_accuracy - base_accuracy) / base_accuracy))

# Latest best params:
# 10-01-2024, games_1988-2024
#  {'n_estimators': 600, 'min_samples_split': 2, 'min_samples_leaf': 2, 'max_features': 'sqrt', 'max_depth': 110, 'bootstrap': False}
# 10-01-2024, games_1996-2024
# {'n_estimators': 1000, 'min_samples_split': 5, 'min_samples_leaf': 2, 'max_features': 'sqrt', 'max_depth': 100, 'bootstrap': False}
# 10-01-2024, games_1979-2024
#  {'n_estimators': 1600, 'min_samples_split': 5, 'min_samples_leaf': 1, 'max_features': 'sqrt', 'max_depth': 70, 'bootstrap': False}