import os
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np

import datetime

from sklearn.model_selection import train_test_split

# TO MAKE ARGS

market = "SFO-LAX"
dtd_cutoff = 6
lq = 0
uq = 100


DATA_DIR = "/data/16/kendra.frederick/shopping_grid/output/market_pkl_files_take2/"
filename = "{}.pkl".format(market)
df = pd.read_pickle(os.path.join(DATA_DIR, filename))

# HELPER FUNCS
dow_list =  ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
dow_dict = dict(zip(range(7), dow_list))

def extract_dow(df, ow=False):
    df['dept_dt_dow_int'] = df['outDeptDt_dt'].apply(
            lambda d: datetime.date.weekday(d))
    df['dept_dt_dow'] = df['dept_dt_dow_int'].map(
        dow_dict)
    if not ow:
        df['return_dt_dow'] = df['inDeptDt_dt'].apply(
                lambda d: datetime.date.weekday(d))
    return df

df = extract_dow(df, ow=True)


# THE MEAT
target_col = 'min_fare'
X = df.drop(columns=[target_col])
y = df[target_col]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.1, random_state=19)
train_pdf = pd.concat([X_train, y_train], axis=1)
train_pdf["train_test"] = "train"
X_test["train_test"] = "test"
mask_df = pd.concat([train_pdf, X_test], axis=0, sort=False)

# df_rt = mask_df[mask_df['round_trip'] == 1].dropna(subset=[target_col])
# df_rt = extract_dow(df_rt, ow=False)

df_ow = mask_df[mask_df['round_trip'] == 0].dropna(subset=[target_col])
df_ow = extract_dow(df_ow, ow=True)

data = df_ow['min_fare']

llim = np.percentile(data, lq)
ulim = np.percentile(data, uq)

clip_ow_df = df_ow[df_ow['min_fare'].between(llim, ulim)]
len(clip_ow_df) / len(df_ow)


clip_ow_df['dtd_regime'] = np.where(clip_ow_df['days_til_dept'] < dtd_cutoff, 'close_to_dept', 'farther_out')
far_out_df = clip_ow_df[clip_ow_df['dtd_regime'] == 'farther_out']



eda_shifted_features_dict = {
    # shift search day -1
    "min_fare_prev_search_day": {
        'sort_col': 'searchDt',
        'groupby_cols': ['outDeptDt', 'inDeptDt']
    },
    # shift departure day +/- 1
    "min_fare_prev_dept_day": {
        'sort_col': 'outDeptDt',
        'groupby_cols': ['searchDt']
    },
    "min_fare_next_dept_day": {
        'sort_col': 'outDeptDt',
        'groupby_cols': ['searchDt'],
        'shift': -1
    },
}


for feature_name, kwargs in eda_shifted_features_dict.items():
    far_out_df[feature_name] = (far_out_df
                    .sort_values(by=kwargs['sort_col'])
                    .groupby(kwargs['groupby_cols'])[target_col]
                    .shift(kwargs.get("shift", 1)) 
                )

far_out_df['fare_diff_prev_shop_dt'] = far_out_df['min_fare'] - far_out_df['min_fare_prev_search_day']
far_out_df['fare_diff_prev_dept_dt'] = far_out_df['min_fare'] - far_out_df['min_fare_prev_dept_day']

far_out_df['shifted_fare_diff_prev_dd'] = (far_out_df
                                        .sort_values(by=['searchDt'])
                                        .groupby(['outDeptDt'])['fare_diff_prev_dept_dt']
                                        .shift(1)
                                       )
far_out_df['calc_fare_from_prev_dd'] = far_out_df['min_fare_prev_dept_day'] + far_out_df['shifted_fare_diff_prev_dd']


# this or something similar might be a feature for the future (predicting cache TTL, e.g.)
far_out_df['fare_prev_shop_dt_close'] = np.where(
    np.isclose(far_out_df['min_fare'], far_out_df['min_fare_prev_search_day'], 0.01), 1, 0)