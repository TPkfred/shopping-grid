"""Modeling
"""

import os
import datetime
import argparse
import logging

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# import matplotlib.cm as cm
import seaborn as sns
# from scipy import stats
# import math
# import pickle

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
import utils as u 
# (
#     Configs,
#     dow_list
# )
from preprocess import Preprocess

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
    "--mask-fraction",
    help="Fraction of data to mask at a time. Also determines "
    "number of folds of cross validation",
    default=0.1
)
parser.add_argument(
    "--tune-lr",
    help="Whether to tune the Linear Regression model",
    default=False
)
# parser.add_argument(
#     "--arg",
#     help="Description ",
#     default="default"
# )

args = parser.parse_args()

input_dir = args.input_dir
parent_output_dir = args.output_dir
market = args.market
mask_frac = args.mask_fraction
tune_lr = args.tune_lr

output_dir = os.path.join(parent_output_dir, market)
os.makedirs(output_dir, exist_ok=True)

Configs = u.Configs()
gc = Configs.global_configs
dt_params = gc['date_time_params']
data_params = gc['data_params']

min_days_til_dept = dt_params['min_days_til_dept']
max_days_til_dept = dt_params['max_days_til_dept'] + 1
min_stay_duration = dt_params['los_start']
max_stay_duration = dt_params['los_end'] + 1
search_start = dt_params['search_start']
search_end = dt_params['search_end']
num_search_days = (search_end - search_start).days + 1

target_col = data_params["target_col"]

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
# fare_llim = np.percentile(market_pdf['min_fare'], 0.04)
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

# split data into train & hold-out sets
y = clip_market_pdf[target_col]
X = clip_market_pdf.drop(columns=[target_col])

X_train, X_hold, y_train, y_hold = train_test_split(
    X, y, test_size=data_params['test_frac'], random_state=data_params['seed'])

# must reset indices prior to using `KFold`
X_train = X_train.reset_index(drop=True)
y_train = y_train.reset_index(drop=True)


# ========================
# DEFINE MODELS
# ========================

model_params = gc["model_params"]
rf_params = model_params["random_forest_regressor"]

feature_dict = {
    'days_til_dept': "norm",
    'stay_duration': "norm", 
    'dept_dt_is_wkehol': "asis",
    'dept_dt_prior_is_wkehol': "asis",
    'dept_dt_next_is_wkehol': "asis",
    'return_dt_is_wkehol': "asis",
    'return_dt_prior_is_wkehol': "asis",
    'return_dt_next_is_wkehol': "asis",
    'min_fare_prev_search_day': "scale",
    'min_fare_prev_dept_day': "scale",
    'min_fare_next_dept_day': "scale",
    'min_fare_prev_return_day': "scale",
    'min_fare_next_return_day': "scale",
    'trail_avg': "scale",
}

# LINEAR REGRESSION MODEL
def get_lr_model(features, tune_lr):

    features_to_scale = [k for k,v in feature_dict.items() if v == "scale" and k in features]
    features_to_norm = [k for k,v in feature_dict.items() if v == "norm" and k in features]
    feature_to_keep_asis = [k for k,v in feature_dict.items() if v == "asis" and k in features]

    final_processor = make_column_transformer(
        (StandardScaler(), features_to_scale),
        (MinMaxScaler(), features_to_norm),
        ('passthrough', feature_to_keep_asis),
        remainder='drop',
        verbose_feature_names_out=False # unsilenced from notebook
    )

    if tune_lr:
        elastic_net_params = model_params["elastic_net_cv"]
        lr_model = make_pipeline(
            final_processor,
            TransformedTargetRegressor(
                regressor=ElasticNetCV(
                    l1_ratio=elastic_net_params["l1_ratios"],
                    alphas=eval(elastic_net_params["alphas"]),
                    cv=3,
                ),
                func=np.log,
                inverse_func=np.exp
            )
        )
    else:
        # use params from previous tuning
        elastic_net_params = model_params["elastic_net"]
        lr_model = make_pipeline(
                final_processor,
                TransformedTargetRegressor(
                    regressor=ElasticNet(
                        l1_ratio=elastic_net_params["l1_ratio"],
                        alpha=elastic_net_params["alpha"], 
                        ),
                    func=np.log,
                    inverse_func=np.exp
                )
            )
    return lr_model


# RANDOM FOREST REGRESSOR
rf_model = RandomForestRegressor(
            n_jobs=-1,
            **rf_params
        )

# =======================
# CROSS VALIDATION
# =======================

def _get_base_results(y_test, y_pred, y_train, train_pred):
    return {
    "test_r2": r2_score(y_test, y_pred),
    "test_mape": mean_absolute_percentage_error(y_test, y_pred),
    "test_mae": mean_absolute_error(y_test, y_pred),
    "test_mdae": median_absolute_error(y_test, y_pred),
    "train_r2": r2_score(y_train, train_pred),
    "train_mape": mean_absolute_percentage_error(y_train, train_pred),
    "train_mae": mean_absolute_error(y_train, train_pred),
    "train_mdae": median_absolute_error(y_train, train_pred)
    }


def cross_val_eval_models(
    X: pd.DataFrame,
    y: pd.Series,
    features: list,
    mask_frac: float,
    rf_model,
    tune_lr_params: bool,
    filename_base: str = f"cv-results"
):
    """
    
    Returns: tuple of 3 dfs:
        (cross-val results, LR predictions, RF predictions
    """
    filename = filename_base + ".pkl"
    results_filepath = os.path.join(output_dir, filename)
    lr_model = get_lr_model(features, tune_lr_params)

    num_splits = int(1/mask_frac)

    kf = KFold(n_splits=num_splits)
    kf_results = []
    test_preds_lr = []
    test_preds_rf = []

    for i, (train_idx, test_idx) in enumerate(kf.split(X, y)):
        print(f"Working on fold {i}")

        # split into train-test for this fold
        X_train_fold, X_test_fold, y_train_fold, y_test_fold = (
            X.loc[train_idx], X.loc[test_idx], 
            y.loc[train_idx], y.loc[test_idx]
        )

        # preprocess 
        preproc = Preprocess(test_size=mask_frac, 
                            clip_first_shop_day=True,
                            first_shop_date=search_start
                            )
        # note the first day of shopping will be filtered out of these
        # so their shapes will not match those of input (X_train_fold)
        X_train_pp, X_test_pp, y_train_pp, y_test_pp = (
            preproc.processs_for_cv(X_train_fold, X_test_fold, y_train_fold, y_test_fold)
        )

        lr_model.fit(X_train_pp, y_train_pp)
        y_pred = lr_model.predict(X_test_pp)
        train_pred = lr_model.predict(X_train_pp)

        tx_list = lr_model.steps[:-1][0][1].transformers
        features_used = [y for x in tx_list for y in x[2]]

        results = _get_base_results(y_test_pp, y_pred, y_train_pp, train_pred)
        results.update({
            "algo": "ElasticNet",
            "fold": i,
            "feature_importances":  lr_model.steps[-1][1].regressor_.coef_,
            "features": features_used
        })
        if tune_lr_params:
            results.update({
                "best_params": {
                    "alpha": lr_model.steps[-1][1].regressor_.alpha_,
                    "l1_ratio": lr_model.steps[-1][1].regressor_.l1_ratio_
            }})
        kf_results.append(results)
        test_preds_lr.extend(list(zip(list(X_test_pp.index), y_pred)))

        # RANDOM FOREST REGRESSOR
        rf_model.fit(X_train_pp[features], y_train_pp)
        y_pred = rf_model.predict(X_test_pp[features])
        train_pred = rf_model.predict(X_train_pp[features])

        results = _get_base_results(y_test_pp, y_pred, y_train_pp, train_pred)
        results.update({
            "algo": "RFR",
            "fold": i,
            "feature_importances": rf_model.feature_importances_,
            "features": features
        })
        if tune_lr_params:
            results.update({
                "best_params": {
                    "n/a"
            }})
        kf_results.append(results)
        test_preds_rf.extend(list(zip(list(X_test_pp.index), y_pred)))
        
    kf_results_df = pd.DataFrame.from_dict(kf_results)
    kf_results_df.to_pickle(results_filepath)

    preds_lr_df = pd.DataFrame(test_preds_lr, columns=['index', 'pred'])
    preds_lr_df.set_index('index', inplace=True)

    preds_rf_df = pd.DataFrame(test_preds_rf, columns=['index', 'pred'])
    preds_rf_df.set_index('index', inplace=True)

    return kf_results_df, preds_lr_df, preds_rf_df


def plot_modeling_results(
        res_df,
        metric_list = ['mae', 'mdae', 'mape', 'r2'],
        save_fig=True,
        filename_base = f"cv-results"
    ):
    """
    res_df: results dataframe from cross-validation
    Note: figure saved as png; file extension will be added
    """
    filename = filename_base + ".png"
    filepath = os.path.join(output_dir, filename)

    res_summ = res_df.groupby("algo").agg(
        {
            'train_r2': ["mean", "std"],
            'test_r2': ["mean", "std"],
            'train_mae': ["mean", "std"],
            'test_mae': ["mean", "std"],
            'train_mdae': ["mean", "std"],
            'test_mdae': ["mean", "std"],
            'train_mape': ["mean", "std"],
            'test_mape': ["mean", "std"],
            
        }
    )

    nr = 1
    nc = len(metric_list)
    fig, _ = plt.subplots(nr, nc, figsize=(nc*4, 5*nr))

    for i, metric in enumerate(metric_list):
        plt.subplot(nr, nc, i+1)
        
        n = len(res_summ)
        xs = np.arange(n)

        w=0.4

        plt.bar(xs-(w/2), res_summ[(f'test_{metric}', 'mean')], yerr=res_summ[(f'test_{metric}', 'std')], 
                width=w, color='royalblue', label='test')
        plt.bar(xs+(w/2), res_summ[(f'train_{metric}', 'mean')], yerr=res_summ[(f'train_{metric}', 'std')],
                width=w, color='lightblue', label='train');
        plt.xticks(xs, res_summ.index)
        plt.legend()
        plt.title(metric);
    
    if save_fig:
        fig.savefig(filepath, format="png")


def inspect_errors(X, y, pred, pred_col="pred", 
        filename_base=f"errors"
    ):
    """
    Inspects and produces a variety of error plots.
    Note: figure saved as a png. File suffix will be appended to
    `filename_base`
    
    params:
        X, y (pd.DataFrames): training data
        pred (pd.DataFrame or str): DataFrame containing 
            predictions *or* name of individual feature
            to inspect. Ideally, predictions are from when sample
            was in test fold from cross-validation
        pred_col (str): If `pred` is a DataFrame, the column 
            that contains the predictions. If `pred` is a string,
            `pred_col` will be set to `pred`.
        filename_base (str): Base string for filename. Final value of
            `pred_col` will get appended.
    """
    if isinstance(pred, str):
        # "preprocess" training data to get feature
        pp = Preprocess(test_size=mask_frac,
                        clip_first_shop_day=True,
                        first_shop_date=search_start
                        )
        X_train_pp, X_test_pp, y_train_pp, y_test_pp = pp.processs(X, y)
        # glue it back together again
        df = pd.concat([pd.concat([X_train_pp, y_train_pp], axis=1),
                  pd.concat([X_test_pp, y_test_pp], axis=1)
                  ], axis=0, sort=False)
        pred_col = pred
    elif isinstance(pred, pd.DataFrame):
        # join predictions to training data
        df = pd.concat([X, y], axis=1)
        df = df.join(pred, how='inner')
        # extract DOW for plotting
        df['dept_dt_dow'] = df['outDeptDt_dt'].apply(
                lambda d: datetime.date.weekday(d))
        df['return_dt_dow'] = df['inDeptDt_dt'].apply(
                lambda d: datetime.date.weekday(d))
    
    # calculate error
    df["error"] = df[target_col] - df[pred_col]
    df['percent_error'] = df["error"] / df[target_col]
    df['abs_percent_error'] = np.abs(df['percent_error'])

    pe_threshold = model_params["outcome"]["percent_error_threshold"]
    x = len(df[df['abs_percent_error'] <= pe_threshold])
    y = len(df)
    print(f"Percent of data that are predicted within "
    f"{pe_threshold*100:.1f}% error threshold: {x/y*100:.1f}%")

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
            xticklabels=u.dow_list, yticklabels=u.dow_list,
            cbar_kws={'label': "average % error"}
            );
    plt.xlabel("Departure DOW")
    plt.ylabel("Return DOW")


    # Finish up figure & save
    fig.suptitle(f"Error - {pred_col}")
    fig.tight_layout()

    filename = filename_base + "_" + pred_col + ".png"
    filepath = os.path.join(output_dir, filename)
    fig.savefig(filepath, format="png")


# ================================
# FITTING, EVAL
# with minor feature selection
# ================================

# INDIVIDUAL FEATURES
ind_feat_err_dict = {
    "trail_avg": "trail_avg",
    "prev_search_day": "min_fare_prev_search_day"
}
for err_label, feature_col in ind_feat_err_dict.items():
    inspect_errors(X_train, y_train, feature_col, 
                    filename_base=f"{err_label}_errors")


# MODELING
feature_selection_dict = {
    "all_features": list(feature_dict.keys()),
    "few_features": ["min_fare_prev_search_day", "trail_avg", "days_til_dept"]
}

for label, features in feature_selection_dict.items():
    res_filename = f"{label}_cv-results"
    cv_res_df, lr_pred_df, rf_pred_df = cross_val_eval_models(
        X=X_train,
        y=y_train,
        features=features, 
        mask_frac=mask_frac,
        rf_model=rf_model,
        tune_lr_params=tune_lr,
        filename_base=res_filename
    )
    plot_modeling_results(cv_res_df, filename_base=res_filename)

    err_dict = {
        "ElasticNet": lr_pred_df,
        "RFR": rf_pred_df,
    }
    for err_label, pred_df in err_dict.items():
        inspect_errors(X_train, y_train, pred_df, 
                       filename_base=f"{label}_{err_label}_errors")

print("Done!")