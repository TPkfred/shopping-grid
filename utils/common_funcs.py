


def calc_shifted_min_fare(df, feature_name="min_fare_last_shop_day"):
    w = (Window
        .partitionBy('market', 'outDeptDt', 'inDeptDt')
        .orderBy("searchDt")
        )

    df = (df
            .withColumn(feature_name, F.lag("min_fare").over(w))
            .withColumn("prev_shop_dt", F.lag("searchDt_dt").over(w))
            .withColumn("prev_shop_date_diff", 
                F.datediff(F.col("searchDt_dt"), F.col("prev_shop_dt")))
        )
    return df

def calc_error(df, col):
    # % error = actual - pred / actual
    df = (df.withColumn("error", F.col("min_fare") - F.col(col))
            .withColumn("abs_pct_error", F.abs(F.col("error")) / F.col("min_fare"))
         )
    return df

def calc_error_stats(df):
    """
    df: dataframe continaing errors
    """
    err_stats = df.describe(['abs_pct_error']).collect()
    median_error = df.approxQuantile("abs_pct_error", [0.5], 0.01)[0]
    err_dict = {r['summary']: r['abs_pct_error'] for r in err_stats}
    err_dict['median'] = median_error
    err_dict['num_markets'] = df.select("market").distinct().count()
    return err_dict


def add_dow_ind(df):
    ## add in DOW indicators
    df = (df
        .withColumn("dept_dt_dow_int", (F.date_format("outDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("dept_dt_dow", F.date_format("outDeptDt_dt", "E"))
        .withColumn("ret_dt_dow_int", (F.date_format("inDeptDt_dt", "u") - 1).cast(T.IntegerType()))
        .withColumn("ret_dt_dow", F.date_format("inDeptDt_dt", "E"))
        )
    return df