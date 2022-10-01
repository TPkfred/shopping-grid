# see utils/preprocess.Preprocess

# LAX-EWR


    
# =============================
# RANDOM FOREST REGRESSOR
# =============================

# INSIDE CV LOOP
# ------------------------
# (**USE OF CLASS**)
preproc = Preprocess(test_size=0.1)
# pp for "preprocessed"
X_train_pp, X_test_pp, y_train_pp, y_test_pp = preproc.processs(X_train, y_train)



features = [
    'days_til_dept', 'stay_duration',
    'dept_dt_is_wkehol', 'dept_dt_prior_is_wkehol',
       'dept_dt_next_is_wkehol', 'return_dt_is_wkehol',
       'return_dt_prior_is_wkehol', 'return_dt_next_is_wkehol',
       'min_fare_prev_search_day', 'min_fare_prev_dept_day',
       'min_fare_next_dept_day', 'min_fare_prev_return_day',
       'min_fare_next_return_day',
       'trail_avg'
]
# alt:
# features = preproc.feature_list

results = []

rfr = RandomForestRegressor()
rfr.fit(X_train_pp[features], y_train_pp)
y_pred = rfr.predict(X_test_pp[features])
train_pred = rfr.predict(X_train_pp[features])

results.append({
    "algo": "rfr",
    "test_rmse": math.sqrt(mean_squared_error(y_test_pp, y_pred)),
    "test_mae": mean_absolute_error(y_test_pp, y_pred),
    "test_r2": r2_score(y_test_pp, y_pred),
    "train_rmse": math.sqrt(mean_squared_error(y_train_pp, train_pred)),
    "train_r2": r2_score(y_train_pp, train_pred),
    "train_mae": mean_absolute_error(y_train_pp, train_pred),
    "feature_importances": rfr.feature_importances_
    })

# END CV LOOP
# ----------------------

# bar chart - feature importance
xs = range(len(features))
plt.bar(xs, rfr.feature_importances_)
plt.xticks(xs, features, rotation=90);
plt.ylabel("feature importance");


# tuning hyperparam
model = RandomForestRegressor(
    oob_score=True,
    bootstrap=True,
    n_estimators=200, # set high for now; tune after other params chosen
    n_jobs=-1
)

params = {
    'max_features' : [None, 'log2', 'sqrt'],
    'max_depth': [None, 3, 5, 7, 10],
#     'min_samples_split': stats.expon(scale= )#np.logspace(-5, -1, 10),
    'min_samples_leaf': stats.expon(scale=0.1),
    'criterion': ['mse', 'mae'],
#     'min_impurity_decrease': stats.expon(scale=0.1),
#     'max_samples': [0.75, 0.9, 0.95, 1], # not available in v 0.20
          }

rand_cv = RandomizedSearchCV(model, params, n_iter=20, n_jobs=-1, cv=3, 
                             iid=False, # will be removed in 0.24
                             return_train_score=True,
                             scoring=['r2', 'neg_mean_absolute_error'],
                             refit='neg_mean_absolute_error'
                            )
rand_cv.fit(X_train_pp[features], y_train_pp)
est = rand_cv.best_estimator_

rand_cv.best_score_
rand_cv.best_params_

y_pred = rand_cv.predict(X_test_pp[features])
train_pred = rand_cv.predict(X_train_pp[features])

results.append({
            "algo": "TunedRFR_mae",
            "fold": i,
            "best_params": rand_cv.best_params_,
            "best_score": rand_cv.best_score_,
#             "test_rmse": math.sqrt(mean_squared_error(y_test_pp, y_pred)),
            "test_r2": r2_score(y_test_pp, y_pred),
            "test_mape": mean_absolute_percentage_error(y_test_pp, y_pred),
            "test_mae": mean_absolute_error(y_test_pp, y_pred),
            "test_mdae": median_absolute_error(y_test_pp, y_pred),
#             "train_rmse": math.sqrt(mean_squared_error(y_train_pp, train_pred)),
            "train_r2": r2_score(y_train_pp, train_pred),
            "train_mape": mean_absolute_percentage_error(y_train_pp, train_pred),
            "train_mae": mean_absolute_error(y_train_pp, train_pred),
            "train_mdae": median_absolute_error(y_train_pp, train_pred),
            "feature_importances": est.feature_importances_
            })

# =============================
# LINEAR REGRESSION
# =============================

features_to_scale = [
    'min_fare_prev_search_day',
    'min_fare_prev_dept_day',
    'min_fare_next_dept_day',
    'min_fare_prev_return_day',
    'min_fare_next_return_day',
    'trail_avg'
]

features_to_norm = ['days_til_dept', 'stay_duration']

feature_to_keep_asis = [
 'dept_dt_is_wkehol',
 'dept_dt_prior_is_wkehol',
 'dept_dt_next_is_wkehol',
 'return_dt_is_wkehol',
 'return_dt_prior_is_wkehol',
 'return_dt_next_is_wkehol'
]

preproc = Preprocess(test_size=0.1)
# pp for "preprocessed"
X_train_pp, X_test_pp, y_train_pp, y_test_pp = preproc.processs(X_train, y_train)

final_processor = make_column_transformer(
    (StandardScaler(), features_to_scale),
    (MinMaxScaler(), features_to_norm),
    ('passthrough', feature_to_keep_asis),
    remainder='drop',
#     verbose_feature_names_out=False # our version of sklearn is OLD
)

model = make_pipeline(
    final_processor,
    TransformedTargetRegressor(
        regressor=ElasticNetCV(
            l1_ratio=[.1, .5, .7, .9, .95, .99, 1],
            alphas=np.logspace(-3, 3, 10),
            cv=3,
        ),
        func=np.log,
        inverse_func=np.exp
    )
)

model.fit(X_train_pp, y_train_pp)
y_pred = model.predict(X_test_pp)
train_pred = model.predict(X_train_pp)

results.append({
            "algo": "ElasticCV",
            "fold": i,
            "best_params": {
                "alpha": lr_model.steps[-1][1].regressor_.alpha_,
                "l1_ratio": lr_model.steps[-1][1].regressor_.l1_ratio_
            },
            "best_score": "n/a",
#             "test_rmse": math.sqrt(mean_squared_error(y_test_pp, y_pred)),
            "test_r2": r2_score(y_test_pp, y_pred),
            "test_mape": mean_absolute_percentage_error(y_test_pp, y_pred),
            "test_mae": mean_absolute_error(y_test_pp, y_pred),
            "test_mdae": median_absolute_error(y_test_pp, y_pred),
#             "train_rmse": math.sqrt(mean_squared_error(y_train_pp, train_pred)),
            "train_r2": r2_score(y_train_pp, train_pred),
            "train_mape": mean_absolute_percentage_error(y_train_pp, train_pred),
            "train_mae": mean_absolute_error(y_train_pp, train_pred),
            "train_mdae": median_absolute_error(y_train_pp, train_pred),
            "feature_importances":  lr_model.steps[-1][1].regressor_.coef_
        })


# getting the params used in ElasticNet model:
model.steps[-1][1].regressor_.alpha_
model.steps[-1][1].regressor_.l1_ratio_


# bar chart - coefficients
tx_list = model.steps[:-1][0][1].transformers
features_used = [y for x in tx_list for y in x[2]]

coef_data = model.steps[-1][1].regressor_.coef_

ys = range(len(coef_data))
plt.figure(figsize=(5,8))
h = 0.8
plt.barh(ys, coef_data, height=h)
plt.vlines(0, ys.start-h, ys.stop-1+h, color='grey')
plt.yticks(ys, features_used);
plt.title("Coefficient Importance - ElasticNetCV");


# GENERIC PLOTS
# scatter of y-true vs y-pred

plt.scatter(y_test_pp, y_pred);
min_val = min(y_test_pp)
max_val = max(y_test_pp)
plt.plot([min_val, max_val], [min_val, max_val], color='r', ls='--');
plt.xlabel("predicted")
plt.ylabel("actual")
plt.title("ElasticNetCV");


# bar chart - comparing algo's
results_df = pd.DataFrame.from_dict(results)
xs = np.arange(len(results_df))
w=0.4
plt.bar(xs-(w/2), results_df['test_mae'], width=w, color='royalblue', label='test mae')
plt.bar(xs+(w/2), results_df['train_mae'], width=w, color='lightblue', label='train_mae');
plt.xticks(xs, results_df['algo'])
plt.legend();
plt.ylabel("MAE min fare ($USD)");


# heatmap looking at correlation of features (*after fillna•) w/ target
