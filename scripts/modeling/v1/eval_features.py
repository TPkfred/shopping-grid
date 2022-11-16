"""Modeling
"""

import os
import sys
import datetime
import argparse
import logging

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# modeling
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.compose import make_column_transformer, TransformedTargetRegressor
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import ElasticNetCV, ElasticNet
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (
    mean_squared_error, r2_score, mean_absolute_error,
    mean_absolute_percentage_error, median_absolute_error
)
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, KFold


# project-specific
# must write as such, otherwise complains about relative import
import utils as u 
from preprocess_no_mask import Preprocess


log_dir = "../../logs"
os.makedirs(log_dir, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--market", "-m",
    help="Market to perform modeling on. Ex: LAX-EWR",
)
parser.add_argument(
    "--input_dir",
    help="Input directory - path to aggregated estreaming data",
    default="../../output/market_pdfs_extra-days/"
)
parser.add_argument(
    "--output_dir",
    help="Output directory to save results to.",
    default="../../output/modeling/"
)
parser.add_argument(
    "--partition",
    help="Whether to partition data by days til departure",
    action="store_true",
    default=False
)
parser.add_argument(
    "--partition-window",
    nargs=2,
    required=False,
    help="Window of values to include in partition",
)
# parser.add_argument(
#     "--mask-fraction",
#     help="Fraction of data to mask at a time. Also determines "
#     "number of folds of cross validation",
#     default=0.1
# )
# parser.add_argument(
#     "--tune-lr",
#     help="Whether to tune the Linear Regression model",
    # action="store_true",
#     default=False
# )
# parser.add_argument(
#     "--arg",
#     help="Description ",
#     default="default"
# )

args = parser.parse_args()
# print(args)
partition_window = [int(x) for x in args.partition_window]
# print(pw)

input_dir = args.input_dir
parent_output_dir = args.output_dir
market = args.market
# mask_frac = args.mask_fraction
# tune_lr = args.tune_lr

if args.partition:
    partition_folder = "-".join(args.partition_window)
    output_dir = os.path.join(parent_output_dir, "partitioned_data", partition_folder, market)
else:
    output_dir = os.path.join(parent_output_dir, "all_data", market)

# print(output_dir)
# sys.exit(0)

os.makedirs(output_dir, exist_ok=True)

Configs = u.Configs()
gc = Configs.global_configs
gen_params = gc['general']
dt_params = gc['date_time_params']
data_params = gc['data_params']
model_params = gc["model_params"]

min_days_til_dept = dt_params['min_days_til_dept']
max_days_til_dept = dt_params['max_days_til_dept'] + 1
min_stay_duration = dt_params['los_start']
max_stay_duration = dt_params['los_end'] + 1
search_start = dt_params['search_start']
search_end = dt_params['search_end']
num_search_days = (search_end - search_start).days + 1

target_col = data_params["target_col"]

log_filename = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
logger = u.configure_logger(
    logger_name = __name__,
    log_level = gen_params['stdout_log_level'],
    log_file_path = os.path.join(log_dir, log_filename),
    log_file_level = gen_params['file_log_level']
)

# ==========================
# LOAD & PROCESS DATA
# ==========================

# load data
filename = f"{market}.pkl"
market_pdf = pd.read_pickle(os.path.join(input_dir, filename))
market_configs = Configs.get_market_config(market)

# filter out anomalously high/low fares
count_llim = data_params['count_llim']
fare_ulim = np.percentile(market_pdf[target_col], market_configs["fare_ulim_percentile"])
fare_llim = np.percentile(market_pdf[target_col], market_configs["fare_llim_percentile"])

clip_market_pdf = market_pdf[
    ~((market_pdf['shop_counts'] <= count_llim) & (market_pdf['min_fare'] > fare_ulim))
                            ]
x1 = len(clip_market_pdf)
fare_llim = np.percentile(market_pdf['min_fare'], 0.04)
clip_market_pdf = clip_market_pdf[
    (clip_market_pdf['min_fare'] >= fare_llim)
                            ]
x2 = len(clip_market_pdf)
y = len(market_pdf)

print("Original data size: ", y)
print("Number high fares clipped:", y-x1)
print("Number low fares clipped:", x1-x2)
print("Remaining fraction data:", x2/y)
print("   ")

y = clip_market_pdf[target_col]
X = clip_market_pdf.drop(columns=[target_col])

# X_train, X_hold, y_train, y_hold = train_test_split(
#     X, y, test_size=data_params['test_frac'], random_state=data_params['seed'])

# # must reset indices prior to using `KFold`
# X_train = X_train.reset_index(drop=True)
# y_train = y_train.reset_index(drop=True)

def _get_base_results(y_test, y_pred):
    return {
    "r2": r2_score(y_test, y_pred),
    "mape": mean_absolute_percentage_error(y_test, y_pred),
    "mae": mean_absolute_error(y_test, y_pred),
    "mdae": median_absolute_error(y_test, y_pred),
    }


def inspect_errors(
        full_df, 
        pred_col="pred", 
        filename_base=f"errors",
        clip_window_data=True,
        clip_window=3,
        partition_data=False,
        partition_by="days_til_dept",
        partition_window=[0,0]
    ):
    """
    Inspects and produces a variety of error plots and summary statistics.
    Plots are saved to file. Summary stats are returned.

    Note: File suffix will be appended to `filename_base`.
    
    params:
    --------
    full_df (pd.DataFrames): training data
    pred_col (str): the column that contains the predictions.
    filename_base (str): Base string for filename. `pred_col` will get 
        appended to final filename.
    clip_window_data (bool): whether to clip off data occuring before largest
        trailing average window
    partition_data (bool): whether to "partition" data by `parition_by` column, 
        to include `partition_window` values
    
    returns:
    --------
    dict of summary statitistics (e.g. MAPE, percent under threshold)
    """

    x0 = len(full_df)
    df = full_df.dropna(subset=[pred_col])
    x1 = len(df)

    if clip_window_data:
        search_cutoff = datetime.datetime.combine(
            search_start + datetime.timedelta(days=clip_window), datetime.time(0,0)
        )
        df = df[df['searchDt_dt'] >= search_cutoff]

    if partition_data:
        df = df[df[partition_by].between(*partition_window)]
    
    # calculate errors
    df["error"] = df[target_col] - df[pred_col]
    df['percent_error'] = df["error"] / df[target_col]
    df['abs_percent_error'] = np.abs(df['percent_error'])

    # compute summary statistics
    acc_results = _get_base_results(df[target_col], df[pred_col])
    
    pe_threshold = model_params["outcome"]["percent_error_threshold"]
    x2 = len(df[df['abs_percent_error'] <= pe_threshold])

    acc_results.update({
        'pct_under_threshold': x2/x0,
        'num_nulls': x0 - x1,
        'pct_nulls': (1-(x1/x0))
    })

    print(acc_results)

    # PLOTS
    # ----------------
    nr = 2
    nc = 2
    fig, _ = plt.subplots(nr, nc, figsize=(nc*5, nr*4))
    i = 0
    
    # error distro (1,1)
    i += 1
    plt.subplot(nr, nc, i)
    plt.hist(df['percent_error'], bins=20)
    plt.title(f"Distribution of % error")
    plt.xlabel("% error")
    plt.ylabel("count")

    # error vs dtd (1,2)
    i += 1
    plt.subplot(nr, nc, i)
    plt.scatter(df['days_til_dept'], df['percent_error'], alpha=0.5);
    plt.title(f"Error - {pred_col}")
    plt.xlabel("days til dept")
    plt.ylabel("percent error")

    # error cdf (2,1)
    i += 1
    plt.subplot(nr, nc, i)
    bins = np.arange(0, 1, step=0.01)
    plt.hist(df['abs_percent_error'], bins=bins,
             density=True, histtype='step',
             cumulative=True,);
    plt.vlines(0.01, 0, 1)
    plt.title(f"CDF of absolute % error")
    plt.xlabel("abs % error")
    plt.ylabel("cdf")
    plt.text(0.011, 0.2, "1% error")
    plt.xscale('log')

    # heatmap of error vs. DOW (2,2)
    i += 1
    plt.subplot(nr, nc, i)
    err_col = f"abs_percent_error"
    dow_summ = df.groupby(["dept_dt_dow", "return_dt_dow"])[err_col].mean()
    dow_summ = pd.DataFrame(dow_summ)
    dow_summ.reset_index(inplace=True)
    pvt = pd.pivot(data=dow_summ, index='return_dt_dow', columns='dept_dt_dow', values=err_col)
    sns.heatmap(pvt, cmap="coolwarm",
                vmin=pe_threshold,
                xticklabels=u.dow_list, yticklabels=u.dow_list,
                cbar_kws={'label': "average % error"}
                )
    plt.xlabel("Departure DOW")
    plt.ylabel("Return DOW")

    # Finish up figure & save
    fig.suptitle(f"Error - {pred_col}")
    fig.tight_layout()

    filename = filename_base + "_" + pred_col + ".png"
    filepath = os.path.join(output_dir, filename)
    fig.savefig(filepath, format="png")

    return acc_results


# ================================
# EVAL
# ================================

# preprocess to get features
pp = Preprocess()
pp_df = pp.processs(clip_market_pdf)
pp_df.to_pickle(os.path.join(output_dir, "feature_df.pkl"))


print(pp_df.head())
print(pp_df.columns)


# for clipping data for search dates that fall before largest window is reached
max_window = pp.calc_max_window()

results_dict = {}
for feature_col in pp.feature_list:
    print(f"working on {feature_col}")
    feat_results = inspect_errors(
        full_df=pp_df,
        filename_base=f"{feature_col}_errors",
        clip_window_data=True,
        clip_window=max_window,
        pred_col=feature_col, 
        partition_data= args.partition,
        partition_by="days_til_dept",
        partition_window=partition_window,
    )
    results_dict[feature_col] = feat_results
    print()

results_df = pd.DataFrame.from_dict(results_dict)
results_df.to_pickle(os.path.join(output_dir, "eval.pkl"))

print("Done!")