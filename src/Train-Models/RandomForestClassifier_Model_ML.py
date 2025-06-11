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
from sklearn.model_selection import train_test_split, cross_val_score, RandomizedSearchCV
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
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

# Create a Random Forest RegressorClassifier with latets best grid search params:
# rf_classifier = RandomForestClassifier(
#     bootstrap=False,
#     max_depth=50,
#     max_features="log2",
#     min_samples_leaf=1,
#     min_samples_split=4,
#     n_estimators=800,
#     random_state=42
# )
# Create RFC with latest random accuracy, 10 iterations
rf_classifier = RandomForestClassifier(
    bootstrap=False,
    max_depth=40,
    max_features="sqrt",
    min_samples_leaf=2,
    min_samples_split=5,
    n_estimators=400,
    random_state=42
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
print(f'R-squared (R2): {r2}')
print(f'Avg 10-fold Cross Validation: {cross_val}')

with open(
    f'/Users/aidenflynn/ML_Python/python-nfl/Models/RF/Classifier/ML/{year+month+day}_R2_{round(r2*100, 1)}_{dataset}_ML.obj', 
    'wb'
) as f:
    dill.dump(rf_classifier, f)