import sqlite3
import struct
import time

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_squared_error, accuracy_score, r2_score, cross_val_score
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from tqdm import tqdm

import dill

from datetime import date, datetime, timedelta

year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)

dataset = "games_1996-2024"
# dataset = "games_1988-2024"
# dataset = "games_1979-2024"
con = sqlite3.connect("../../Data/games.sqlite")
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
acc_results = []

# Split into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.1, random_state=42)

# Create a Random Forest Regressor
rf_classifier = RandomForestClassifier(
    bootstrap=False,
    max_depth=100,
    max_features="sqrt",
    min_samples_leaf=2,
    min_samples_split=5,
    n_estimators=800, 
    random_state=18
)
# Fit the model on the training data
rf_classifier.fit(X_train, y_train)

# Make predictions on the test data
y_pred = rf_classifier.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
avg_value = np.mean(y_test)
relative_error = rmse / avg_value * 100
r2 = r2_score(y_test, y_pred)
# 10-Fold Cross validation
cross_val = np.mean(cross_val_score(rf_classifier, X_train, y_train, cv=10))

print(f'Mean Squared Error (MSE): {mse}')
print(f'Root Mean Squared Error (RMSE): {rmse}')
print(f'R-squared (R2): {round(r2*100, 1)}')
print(f'Avg 10-fold Cross Validation: {round(cross_val*100,1)}')

with open(
    f'/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/UO/{year}_{month}_{day}_{round(cross_val*100,1)}_R2_{round(r2*100, 1)}_{dataset}_UO.obj',
    'wb'
) as f:
    dill.dump(rf_classifier, f)

# dataset: "games_2013-23", Mean Squared Error: 0.13912269391750784
# X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.25, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=7, random_state=18)

# dataset: "games_2013-23", Mean Squared Error: 0.13494093565677195
# X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.3, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=100, max_depth=7, random_state=18)

# dataset: "games_2003-23", Mean Squared Error: 0.12648250041382267
# X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=7, random_state=18)

# dataset: "games_2000-23", Mean Squared Error: 0.12598982253106183
# X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=9, random_state=18)

# dataset: "games_1988-2023", Mean Squared Error: 0.11007897335888568
# X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=9, random_state=18)

# dataset: "games_1979-2023", Mean Squared Error: 0.11574803374290529
# X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=9, random_state=18)

#strongest r2 56.0:
# X_train, X_test, y_train, y_test = train_test_split(data, OU, test_size=0.2, random_state=42)
# rf_regressor = RandomForestRegressor(n_estimators=300, max_depth=11, random_state=18)