"""Code examples for tuning models in spark using cross-validation.

This file is not intended to be run as a script.

You will need to define:
- regression_model: Pipeline of data prep / feature engineering
    + ML algorithm (e.g. LinearRegression)

See modeling_in_spark.py for examples

"""
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# import and define your pipeline, including the algorithm:
from pyspark.ml import Pipeline
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor
)

pipeline = Pipeline(stages=[
    #...
    vector_assembler,
    regression_model
]

def get_cv_results_list(cv_model, print_vals=True):
    cv_metrics = cv_model.avgMetrics
    cv_params = [dict(zip([y.name for y in x.keys()], x.values())) for x in list(cv_model.getEstimatorParamMaps())]
    cv_results = list(zip(cv_params, cv_metrics))
    if print_vals:
        for x in cv_results:
            print(x[0], x[1])
    return cv_results


def get_best_from_cv(cv_model):
    cv_results = list(zip(
        cv_model.getEstimatorParamMaps(), 
        cv_model.avgMetrics
    ))
    cv_results.sort(key=lambda x: x[1])
    best_result = cv_results[0]
    best_metric = best_result[1]
    best_param_dict = best_result[0]
    params = [x.name for x in list(best_param_dict.keys())]
    param_vals = best_param_dict.values()
    best_params = dict(zip(params, param_vals))
    print(best_params, best_metric)
    return best_params


# LINEAR REGRESSION
param_grid = (ParamGridBuilder()
              # regParam is lambda (weight / factor), elasticNet is alpha (ratio)
              # note alpha is the opposite of sklearn's defintion
              .addGrid(regression_model.elasticNetParam, [0.01, 0.1, 0.5, 0.9, 0.99])
              .addGrid(regression_model.regParam, [0.01, 0.1, 1, 10, 100])
              .build()
             )

cv = CrossValidator(estimator=pipeline,
                    estimatorParamMaps=param_grid,
                    evaluator=RegressionEvaluator(labelCol="fare"),
                    numFolds=3,
                    parallelism=5
                   )

cv_model = cv.fit(train)
pred_cv = cv_model.transform(test)



# RANDOM FOREST

param_grid = (ParamGridBuilder()
              .addGrid(regression_model.numTrees, [50, 100, 200])
              .addGrid(regression_model.maxDepth, [3, 5, 7, 10])
              .build()
             )

cv = CrossValidator(estimator=pipeline,
                    estimatorParamMaps=param_grid,
                    evaluator=RegressionEvaluator(labelCol="fare"),
                    numFolds=3,
                    parallelism=5
                   )

cv_model = cv.fit(train)
pred_cv = cv_model.transform(test)
pred_cv = calc_abs_pct_err(pred_cv)

get_best_from_cv(cv_model)

