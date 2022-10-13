
import pyspark.sql.functions as F


def calc_error(df, pred_col, actual_col="min_fare"):
    # % error = actual - pred / actual
    df = (df.withColumn("error", F.col(actual_col) - F.col(pred_col))
            .withColumn("abs_pct_error", F.abs(F.col("error")) / F.col(actual_col))
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