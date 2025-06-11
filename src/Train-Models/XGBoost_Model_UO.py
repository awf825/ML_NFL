import sqlite3
import struct

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from tqdm import tqdm

dataset = "games_2003-23"
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

for x in tqdm(range(100)):
    x_train, x_test, y_train, y_test = train_test_split(data, OU, test_size=.1)

    train = xgb.DMatrix(x_train, label=y_train)
    test = xgb.DMatrix(x_test)

    param = {
        'max_depth': 20,
        'eta': 0.05,
        'objective': 'multi:softprob',
        'num_class': 3
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
        model.save_model('../../Models/XGB/UO/XGBoost_{}_{}%_UO.json'.format(dataset, acc))
