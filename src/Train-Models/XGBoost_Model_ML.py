import sqlite3
from typing import ByteString
import struct

import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 500)
import xgboost as xgb
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from tqdm import tqdm

dataset = "games_2003-2023"
con = sqlite3.connect("../../Data/games.sqlite")
data = pd.read_sql_query(f"select * from \"{dataset}\"", con, index_col="index")
con.close()


data['VIS_TEAM_NAME'] = data['VIS_TEAM_NAME'].apply(lambda x: list(x)[0] )
data['HOME_TEAM_NAME'] = data['HOME_TEAM_NAME'].apply(lambda x: list(x)[0] )

# data['VIS_3RD_CONV'].astype(float)
# data['HOME_3RD_CONV'].astype(float)
data= data.replace('', np.nan)  # Replace empty strings with NaN
data.dropna(inplace=True)      # Drop rows with NaN values

margin = data['Home-Team-Win']

data.drop(
    ['HOME_TEAM_NAME', 'VIS_TEAM_NAME', 'HOME_SCORE', 'VIS_SCORE', 'SCORE', 'Home-Team-Win', 'OU_COVER', 'OU'],
    axis=1, 
    inplace=True
)


data = data.to_numpy(dtype=float)
acc_results = []
for x in tqdm(range(300)):
    x_train, x_test, y_train, y_test = train_test_split(data, margin, test_size=.1)

    train = xgb.DMatrix(x_train, label=y_train)
    test = xgb.DMatrix(x_test, label=y_test)

    param = {
        'max_depth': 3,
        'eta': 0.01,
        'objective': 'multi:softprob',
        'num_class': 2
    }
    epochs = 750

    model = xgb.train(param, train, epochs)
    predictions = model.predict(test)
    y = []

    for z in predictions:
        y.append(np.argmax(z))

    acc = round(accuracy_score(y_test, y) * 100, 1)
    print(f"{acc}%")
    acc_results.append(acc)
    # only save results if they are the best so far
    if acc == max(acc_results):
        model.save_model('../../Models/XGB/ML/XGBoost_{}_{}%_ML.json'.format(dataset, acc))
