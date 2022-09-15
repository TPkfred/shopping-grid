import matplotlib.cm as cm


def process_coverage_data(df):
    # updated column names
    df = df.withColumn("market", 
    F.concat_ws("-", F.col("origin"), F.col("destination")))

    # convert Dept dates (which are strings) to datetime
    df.registerTempTable("data")
    df = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(outDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(inDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(searchDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS searchDt_dt
        FROM data
    """)

    # filter on round-trip
    df_filt = (df
#                 .filter(F.col("outDeptDt_dt").between(min_dept_dt, max_dept_dt))
                # Note: when we filter on stay duration below, this also
                # effectively accomplishes filtering on round-trip == 1
                .filter(F.col("round_trip") == 1)
            )
    
    # add stay_duration column & fitler on it
    df_filt = df_filt.withColumn('stay_duration', F.datediff(
                    F.col('inDeptDt_dt'), F.col('outDeptDt_dt'))
                )
    # Note this effectively filters out null stay durations, which are one-way trips
    df_filt = df_filt.filter(F.col('stay_duration').between(0, max_stay_duration))
    
    # Add days_til_dept column & filter on it
    df_filt = df_filt.withColumn('days_til_dept',
                    F.datediff(
                        F.col('outDeptDt_dt'), F.col('searchDt_dt'))
                    )
    df_filt = df_filt.filter(F.col('days_til_dept').between(0, max_days_til_dept))
    
    # BY DAYS TIL DEPT
    dtd_df = (df_filt
                .groupBy(["market", "days_til_dept", "stay_duration"])
              # add agg to pull shop counts over
                .agg(
                    F.countDistinct("searchDt_dt").alias("obs_num_days"),
                    F.sum("shop_counts").alias("shop_counts")
                )
                .withColumn("pct_shop_coverage", F.col("obs_num_days") / num_search_days)
                .withColumn("pct_shop_coverage_adj", F.col("obs_num_days") / num_search_days_adj)           
                .repartition("market")
              )
    return dtd_df

def process_oneway_coverage_data(df):
    # updated column names
    df = df.withColumn("market", 
        F.concat_ws("-", F.col("origin"), F.col("destination")))

    # convert Dept dates (which are strings) to datetime
    df.registerTempTable("data")
    df = spark.sql("""
        SELECT *,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(outDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS outDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(inDeptDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS inDeptDt_dt,
            TO_DATE(CAST(UNIX_TIMESTAMP(CAST(searchDt AS string), 'yyyyMMdd') AS TIMESTAMP)) AS searchDt_dt
        FROM data
    """)
    
    # TODO: could clean up variable names here
    
    # filter on dates & round-trip
    # TODO: don't need both this filter and the one on days til dept
    df_filt = (df
                .filter(F.col("outDeptDt_dt").between(min_dept_dt, max_dept_dt))
            )
    df_ow = df_filt.filter(F.col("round_trip") == 0)
    
#     df_filt = df_filt.withColumn('stay_duration',
#                 F.datediff(
#                     F.col('inDeptDt_dt'), F.col('outDeptDt_dt'))
#                 )
    # Note this effectively filters out null stay durations, which are one-way trips
#     df_filt = df_filt.filter(F.col('stay_duration').between(0, max_stay_duration))

    df_ow = df_ow.withColumn('days_til_dept',
                    F.datediff(
                        F.col('outDeptDt_dt'), F.col('searchDt_dt'))
                    )
    df_ow = df_ow.filter(F.col('days_til_dept').between(0, max_days_til_dept))
    
    # BY DAYS TIL DEPT
    dtd_df = (df_ow
                .groupBy(["market", "days_til_dept"])
              # add agg to pull shop counts over
                .agg(
                    F.countDistinct("searchDt_dt").alias("obs_num_days"),
                    F.sum("shop_counts").alias("shop_counts")
                )
                .withColumn("pct_shop_coverage", F.col("obs_num_days") / num_search_days)
                .withColumn("pct_shop_coverage_adj", F.col("obs_num_days") / num_search_days_adj)           
                .repartition("market")
              )
#     dtd_df.cache()
    return dtd_df

input_dir = "/user/kendra.frederick/shop_vol/v5/decoded/with_pcc/"
cov_df = spark.read.option("mergeSchema", True).parquet(input_dir)
airport_df = spark.read.csv("/data/reference/AIRPORT.CSV", header=True)


# TODO: add in option to filter on POS country ORGN/DEPT

# pos = "IN"

def analyze_pos(cov_df, pos, los_start, los_end, filter_on_org_dst=True):
    """
    cov_df (pd.DataFrame): coverage data dataframe
    pos (str): point of sale 
    los_start, los_end (int): length of stay (LOS) limits
    filter_on_org_dst (bool): whether to filter on origin or destination
        airport being in POS country. Default=True.
    """

    pos_cov_df = cov_df.filter(F.col("pos") == pos)

    if filter_on_org_dst:
        pos_cov_df = (pos_cov_df
                        .join(
                            airport_df.select('airport_code', 'country_code'),
                            on=[pos_cov_df['origin'] == airport_df['airport_code']],
                        ).withColumnRenamed("country_code", "origin_country")
                        .drop("airport_code")
                        )
        pos_cov_df = (pos_cov_df
                        .join(
                            airport_df.select('airport_code', 'country_code'),
                            on=[pos_cov_df['destination'] == airport_df['airport_code']],
                        ).withColumnRenamed("country_code", "destination_country")
                        .drop("airport_code")
                        )

        pos_cov_df = pos_cov_df.filter(
            (F.col('origin_country') == pos) | (F.col("destination_country") == pos)
        )

    pos_agg = process_coverage_data(pos_cov_df)

    # filter on LOS, to restrict our analysis
    pos_agg_los = pos_agg.filter(F.col("stay_duration").between(los_start, los_end))

    # take average coverage over this range of LOS
    pos_covg_summ = (pos_agg_los
                    .groupBy("market", "days_til_dept")
                    .agg(
                        F.mean("pct_shop_coverage").alias("avg_coverage"),
                        F.sum("shop_counts").alias("shop_counts")
                    )
                    )

    # compute trailing avg over days til dept
    w_trail = (Window
            .partitionBy("market")
            .orderBy("days_til_dept")
            .rangeBetween(Window.unboundedPreceding, Window.currentRow)
            )
    pos_covg_summ = (pos_covg_summ
                        .withColumn("trailing_avg_over_dtd",
                                    F.mean("avg_coverage").over(w_trail))
                        .withColumn("sum_counts",
                                    F.sum("shop_counts").over(w_trail))
                        )
    pos_cov_summ_pdf = pos_covg_summ.toPandas()

    return pos_cov_summ_pdf

def sort_markets_by_dtd_covg(covg_summ_pdf, dtd):
    # get market list sorted by trailing avg at `dtd` days until dept
    temp = covg_summ_pdf[covg_summ_pdf['days_til_dept'] == dtd]
    market_list_sorted = list(temp.sort_values(by=["trailing_avg_over_dtd", "sum_counts"],
                                            ascending=[False, False]
                                            )['market'])
    return market_list_sorted


# ===========================
# PLOTTING FUNCTIONS
# ===========================

def plot_stacked_market_heatmap(covg_summ_pdf, sorted_market_list, start, num, add_y_labels=True):
    stop = start + num
    markets_to_plot = sorted_market_list[start:stop]
    adj_num = min(len(markets_to_plot), num)

    pvt = covg_summ_pdf.pivot(index='days_til_dept', columns='market', values='avg_coverage')
    pvt_to_plot = pvt[markets_to_plot].transpose()
    
    if add_y_labels:
        plot_height = int(adj_num/5)
    else:
        plot_height = int(adj_num/10)
        
    plt.figure(figsize=(15, plot_height))
    sns.heatmap(pvt_to_plot, cmap='Greens', vmin=0,
                yticklabels=add_y_labels,
                cbar_kws={'label': 'avg coverage', 'shrink': 0.5});
    # plt.title(f"Average coverage across {los_start}- to {los_end}-day trips // POS = {pos}  // markets {start} - {stop}");
    plt.title(f"Average coverage, markets {start} - {stop}");
    plt.show()


def plot_vol_vs_coverage(cov_summ_pdf):
    plt.figure(figsize=(8,5))
    plt.scatter(cov_summ_pdf['shop_counts'], cov_summ_pdf['avg_coverage'], alpha=0.5);
    plt.xlabel("shop counts");
    plt.ylabel("avg coverage");
    plt.title(f"Average coverage across a range of LOS vs. volume");


def plot_violin_distro_vs_dtd(covg_summ_pdf, dtd_list=[14, 30, 60, 90, 120]):
    covg_summ_filt_pdf = covg_summ_pdf[covg_summ_pdf['days_til_dept'].isin(dtd_list)]
    sns.violinplot(covg_summ_filt_pdf['days_til_dept'], covg_summ_filt_pdf['trailing_avg_over_dtd'], palette="Greens");


def line_plot_coverage_vs_dtd(covg_summ_pdf, dtd_list=[14, 30, 60, 90, 120]):
    cmap_name = "Greens"#"GnBu"
    colors = [cm.get_cmap(cmap_name)(x) for x in np.linspace(0, 1, len(dtd_list)+1)]

    plt.figure(figsize=(8,5))
    for i, dtd in enumerate(dtd_list):
        line_plot_df = covg_summ_pdf[covg_summ_pdf['days_til_dept'] == dtd]
        line_plot_df = line_plot_df.sort_values(by=["trailing_avg_over_dtd", "sum_counts"],
                                ascending=[False, False],
                                ).reset_index(drop=True)
        plt.plot(line_plot_df.index, line_plot_df['trailing_avg_over_dtd'],
                color=colors[i+1],
                label=f"{dtd} days out");    
        plt.legend();
        plt.title("Trailing avearage of coverage at various days til deptarture");
        plt.ylabel("avg coverage")
        plt.xlabel("market");

    # line_y = 0.85
    # plt.hlines(line_y, 0, len(market_list_sorted), colors="r", linestyles="dotted");
    # plt.text((len(market_list_sorted)*0.55), line_y + 0.03, f"{line_y} coverage");


def scatter_coverage_vs_vol_by_dtd(covg_summ_pdf, dtd_list=[14, 30, 60, 90, 120]):
    cmap_name = "Greens"
    colors = [cm.get_cmap(cmap_name)(x) for x in np.linspace(0, 1, len(dtd_list)+1)]

    plt.figure(figsize=(8,5))
    for i, dtd in enumerate(dtd_list):
        line_plot_df = covg_summ_pdf[covg_summ_pdf['days_til_dept'] == dtd]
        line_plot_df = line_plot_df.sort_values(by=["sum_counts"],
                                ascending=[False],
                                )
        plt.scatter(line_plot_df['sum_counts'], line_plot_df['trailing_avg_over_dtd'],
                color=colors[i+1],
                label=f"{dtd} days out");    
        plt.legend();
        plt.title("Coverage vs volume for various days til deptarture");
        plt.ylabel("avg coverage")
        plt.xlabel("shop volume");