# loop through three datasets. For each:
    # get best random params
    # use those to get best grid search
    # with grid search params, generate model

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
    print('Average Error: {:0.4f} degrees.'.format(np.mean(errors)))
    print('Accuracy: ', accuracy)
    print('\n')
    
    return accuracy

year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)

datasets = ["games_1979-2024","games_1988-2024","games_1996-2024"]

for dataset in datasets:
    print(f'Being evaluating {dataset}...')
    con = sqlite3.connect("/Users/aidenflynn/ML_Python/python-nfl/Data/games.sqlite")
    data = pd.read_sql_query(f"select * from \"{dataset}\"", con, index_col="index")
    con.close()

    data['VIS_TEAM_NAME'] = data['VIS_TEAM_NAME'].apply(lambda x: list(x)[0] )
    data['HOME_TEAM_NAME'] = data['HOME_TEAM_NAME'].apply(lambda x: list(x)[0] )

    data= data.replace('', np.nan)  # Replace empty strings with NaN
    data.dropna(inplace=True)      # Drop rows with NaN values

    margin = data['Home-Team-Win']

    data.drop(
        ['HOME_TEAM_NAME', 'VIS_TEAM_NAME', 'HOME_SCORE', 'VIS_SCORE', 'SCORE', 'Home-Team-Win', 'OU_COVER', 'OU'],
        axis=1, 
        inplace=True
    )

    data.astype(float)

    # Split into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(data, margin, test_size=0.1, random_state=42)
    # Create a Random Forest Regressor
    ref_regressor = RandomForestClassifier(random_state=42)
    # Fit the model on the training data
    ref_regressor.fit(X_train, y_train)
    # Make predictions on the test data
    y_pred = ref_regressor.predict(X_test)
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

    random_grid = {
        'n_estimators': n_estimators,
        'max_features': max_features,
        'max_depth': max_depth,
        'min_samples_split': min_samples_split,
        'min_samples_leaf': min_samples_leaf,
        'bootstrap': bootstrap
    }

    print('Begin RandomizedSearchCV...')
    rf_random = RandomizedSearchCV(estimator = ref_regressor, param_distributions = random_grid, n_iter = 100, cv = 3, verbose=2, random_state=42, n_jobs = -1)

    rf_random.fit(X_train, y_train)
    print("best_params: ", json.dumps(rf_random.best_params_, indent=4))
    best_params = rf_random.best_params_

    base_model = RandomForestClassifier(n_estimators = 10, random_state = 42)
    base_model.fit(X_train, y_train)
    print("Printing base accuracy...")
    base_accuracy = evaluate(base_model, X_train, y_train)
    best_random = rf_random.best_estimator_
    print("Printing random accuracy...")
    random_accuracy = evaluate(best_random, X_train, y_train)

    best_bootstrap = best_params['bootstrap']
    best_min_samples_split = best_params['min_samples_split']
    best_min_samples_leaf = best_params['min_samples_leaf']
    best_max_features = best_params['max_features']
    best_max_depth = best_params['max_depth']
    best_estimators = best_params['n_estimators']

    param_grid = {
        'bootstrap': [best_bootstrap],
        'max_features': [best_max_features],
        'min_samples_leaf': [best_min_samples_leaf, best_min_samples_leaf+1],
        'min_samples_split': [best_min_samples_split, best_min_samples_split+1],
        'n_estimators': [best_estimators-200, best_estimators-100, best_estimators, best_estimators+100, best_estimators+200]
    }
    # Create a based model
    rfc = RandomForestClassifier()# Instantiate the grid search model
    print('Begin 10 fold GridSearchCV...')
    grid_search = GridSearchCV(estimator = rfc, param_grid = param_grid, cv = 10, n_jobs = -1, verbose = 2)

    grid_search.fit(X_train, y_train)
    print("grid_search_best_params: ", json.dumps(grid_search.best_params_, indent=4))
    grid_search_best_params = grid_search.best_params_

    best_grid = grid_search.best_estimator_
    print("\n")
    print("Printing grid accuracy...\n")
    grid_accuracy = evaluate(best_grid, X_train, y_train)

    base_model = RandomForestClassifier(n_estimators = 10, random_state = 42)
    base_model.fit(X_train, y_train)
    print("\n")
    print("Printing base accuracy...\n")
    base_accuracy = evaluate(base_model, X_train, y_train)

    rfc = RandomForestClassifier(
        bootstrap=grid_search_best_params['bootstrap'],
        max_features=grid_search_best_params['max_features'],
        min_samples_leaf=grid_search_best_params['min_samples_leaf'],
        min_samples_split=grid_search_best_params['min_samples_split'],
        n_estimators=grid_search_best_params['n_estimators'],
        random_state=42
    )

    # Fit the model on the training data
    rfc.fit(X_train, y_train)

    # Make predictions on the test data
    y_pred = rfc.predict(X_test)

    # Evaluate the model
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    avg_value = np.mean(y_test)
    relative_error = rmse / avg_value * 100
    r2 = r2_score(y_test, y_pred)
    cross_val = np.mean(cross_val_score(rfc, X_train, y_train, cv=10))

    print(f'Mean Squared Error (MSE): {mse}')
    print(f'Root Mean Squared Error (RMSE): {rmse}')
    print(f'R-squared (R2): {round(r2*100, 1)}')
    print(f'Avg 10-fold Cross Validation: {round(cross_val*100,1)}')

    print('Saving model...')
    with open(
        f'/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/ML/{year}_{month}_{day}_{round(cross_val*100,1)}_R2_{round(r2*100, 1)}_{dataset}.obj', 
        'wb'
    ) as f:
        dill.dump(rfc, f)

    print(f'Completed ML evaluation of {dataset}!')
    print('\n')