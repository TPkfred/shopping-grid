from itertools import groupby
import numpy as np

def _inspect_gaps(arr, gap_val=0):
    """
    return:
    -------
    array of: number of gaps, average gap size, max gap size
    """
    grp_arr = [(k, sum(1 for _ in num)) for k, num in groupby(arr)]
    gap_tuples = [v for k,v in grp_arr if k == gap_val]
    
    # # number of gaps > 1 day
    # y = len([x for x in gap_tuples if x > 1])

    if len(gap_tuples) == 0:
        return 0.0, 0.0, 0.0
    else:
        return float(len(gap_tuples)), float(np.mean(gap_tuples)), float(max(gap_tuples))

# inspect_gaps = F.udf(_inspect_gaps, T.ArrayType(T.IntegerType()))
inspect_gaps = F.udf(_inspect_gaps, T.ArrayType(T.FloatType()))


def process(market, input_df):
    mrkt_df = input_df.filter(F.col("market") == market)

    join_cols = ['outDeptDt_dt', 'stay_duration', 'searchDt_dt']

    # join with enum date df
    mrkt_join_df = (mrkt_df.join(date_cross_df, 
                                on=join_cols, 
                                how='outer')
                        .fillna(0, subset=['sum_solution_counts', 'sum_shop_counts', 'shop_ind'])
                )

    # calculate `days_til_dept`
    mrkt_join_df = mrkt_join_df.withColumn('days_til_dept',
                    F.datediff(
                        F.col('outDeptDt_dt'), F.col('searchDt_dt'))
                    )

    # And then filter on it
    mrkt_join_df = mrkt_join_df.filter(F.col('days_til_dept').between(0, max_days_til_dept))

    window = (Window.partitionBy('outDeptDt_dt', 'stay_duration')
                    .orderBy('searchDt_dt')
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )

    win_df = (mrkt_join_df
            .withColumn("values", F.collect_list("shop_ind").over(window))
            .withColumn("num_days_w_shop", F.sum("shop_ind").over(window))
            )

    win_df = (win_df
            .withColumn("num_shop_days", F.size(F.col("values")))
            .withColumn("num_days_wo_shop", F.col("num_shop_days") - F.col("num_days_w_shop"))
            .withColumn("gap_analysis", inspect_gaps(F.col("values")))
            .withColumn("num_gaps", F.col("gap_analysis").getItem(0))
            .withColumn("avg_gap_size", F.col("gap_analysis").getItem(1))
            .withColumn("max_gap_size", F.col("gap_analysis").getItem(2))
            .withColumn("norm_max_gap_size", F.col("max_gap_size")/F.col("num_shop_days"))
            .withColumn("norm_num_gaps", F.col("num_gaps")/F.col("num_shop_days"))
            )