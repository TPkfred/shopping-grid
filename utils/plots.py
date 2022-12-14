
def plot_shop_coverage(market, market_pdf, what_to_plot="shops", save_fig=False):
    """Heatmap of shopping coverage

    params:
    --------
    what_to_plot (str): ["shops", "vol", "both"]
    """
    # AGGREGATE DATA
    agg_cols = ["num_shop_days", 
                "avg_pct_coverage_shop", 
                "total_num_solutions", "avg_num_solutions"] 
    agg_dict = {"shop_ind": "sum", 
                "avg_shop_days": "mean",
                "sum_solution_counts": ["sum", "mean"]
                        }

    agg_dtd = (market_pdf
                    .groupby(["days_til_dept", "stay_duration"])
                    .agg(agg_dict)
                )
    agg_dtd.columns = agg_cols
    agg_dtd = agg_dtd.reset_index()

    agg_dept_dt = (market_pdf
                    .groupby(["outDeptDt_dt", "stay_duration"])
                    .agg(agg_dict)
                )
    agg_dept_dt.columns = agg_cols
    agg_dept_dt = agg_dept_dt.reset_index()


    # PIVOT & PLOT
    file_name = "_".join([market, "shops"])

    
    # number of shop days -- a boolean
    if what_to_plot in ["shops", "both"]:
        file_name += "_bool"

        pvt1 = agg_dept_dt.pivot(index="stay_duration", 
                                columns="outDeptDt_dt", 
                                values="num_shop_days")

        pvt2 = agg_dept_dt.pivot(index="stay_duration", 
                            columns="outDeptDt_dt", 
                            values="avg_pct_coverage_shop")

        pvt3 = agg_dtd.pivot(index="stay_duration", 
                            columns="days_til_dept", 
                            values="num_shop_days")
        

#         pvt4 = agg_dtd.pivot(index="stay_duration", 
#                             columns="days_til_dept", 
#                             values="avg_pct_coverage_shop")

        n=3
        i=1
#         fig1, _ = plt.subplots(n,1, figsize=(max_days_til_dept//5, (max_stay_duration//(n*3)*2)))
        fig1, _ = plt.subplots(n,1, figsize=(15, 12))
        
        with sns.axes_style("white"):
            plt.subplot(n,1,i)
            sns.heatmap(pvt1, cmap='Greens', square=True,
                        vmin=0,
                        cbar_kws={'label': 'number days w/ a shop',
                                'shrink': 0.5});
            plt.title("Total num shop days");
            plt.ylabel("stay duration (days)")
            plt.xlabel("departure date");
        
        i+=1
        with sns.axes_style("white"):
            plt.subplot(n,1,i)
            sns.heatmap(pvt2, cmap='Greens', square=True,
                        vmin=0, vmax=1.0,
                        cbar_kws={'label': 'avg % coverage',
                                'shrink': 0.5});
            plt.title("Average % Coverage - rolling 3-day window of shopping");
            plt.ylabel("stay duration (days)")
            plt.xlabel("departure date");

        i+=1
        with sns.axes_style("white"):
            plt.subplot(n,1,i)
            sns.heatmap(pvt3, cmap='Greens', square=True,
                        vmin=0,
                        cbar_kws={'label': 'num shop days',
                                'shrink': 0.5});
            plt.title("Num shop days");
            plt.ylabel("stay duration (days)")
            plt.xlabel("days until departure");
                               
        fig1.suptitle(market)
        fig1.tight_layout()
        plt.subplots_adjust(top=0.93, hspace=0.5)
        fig1.show()

        if save_fig:
#             plt.savefig(out_dir + file_name + "_by-dept-date.png", format="png")
            plt.savefig(out_dir + file_name + ".png", format="png")



# DEVELOPED IN NOTEBOOKS 14x

# use for power point explainer
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

# # Probably won't use
# def plot_vol_vs_coverage(cov_summ_pdf):
#     plt.figure(figsize=(8,5))
#     plt.scatter(cov_summ_pdf['shop_counts'], cov_summ_pdf['avg_coverage'], alpha=0.5);
#     plt.xlabel("shop counts");
#     plt.ylabel("avg coverage");
#     plt.title(f"Average coverage vs. volume");


# TODO: updated to allow option to restrict to top X markets
def plot_violin_distro_vs_dtd(covg_summ_pdf, dtd_list=dtd_list):
    covg_summ_filt_pdf = covg_summ_pdf[covg_summ_pdf['days_til_dept'].isin(dtd_list)]
    sns.violinplot(covg_summ_filt_pdf['days_til_dept'], covg_summ_filt_pdf['trailing_avg_over_dtd'], palette="Greens");


# updated to allow option to restrict to top X markets
def line_plot_coverage_vs_dtd(pos,
                              covg_summ_pdf, 
                              dtd_list=dtd_list, 
                              use_color_map=True,
                              top_n=None,
                              sorted_market_list=[],
                              sort_by_vol_only=False,
                              save_fig=False,
                              filename_extra="",
                             ):
    cmap_name = "Greens"
    if use_color_map:
        colors = [cm.get_cmap(cmap_name)(x) for x in np.linspace(0, 1, len(dtd_list)+1)]
    else:
        colors = ["blue", "orange", "green", "red", "cyan", "magenta", "yellow", "black"]
    
    if top_n:
        assert len(sorted_market_list) > 0, "You must supply a sorted market list when you specify `top_n`"
        markets_to_plot = sorted_market_list[:top_n]
        covg_summ_pdf = covg_summ_pdf[covg_summ_pdf['market'].isin(markets_to_plot)]
    
    if sort_by_vol_only:
        sort_by = "trailing_sum_counts"
        asc_arg = False
    else:
        sort_by = ["trailing_avg_over_dtd", "trailing_sum_counts"]
        asc_arg = [False, False]
    
    plt.figure(figsize=(8,5))
    for i, dtd in enumerate(dtd_list):
        line_plot_df = covg_summ_pdf[covg_summ_pdf['days_til_dept'] == dtd]
        line_plot_df = line_plot_df.sort_values(by=sort_by,
                                ascending=asc_arg,
                                ).reset_index(drop=True)
        plt.plot(line_plot_df.index, line_plot_df['trailing_avg_over_dtd'],
                color=colors[i+1],
                label=f"{dtd} days out");    
        plt.legend();
        plt.title("Trailing average of coverage at various days til deptarture");
        plt.ylabel("avg coverage")
        plt.xlabel("market rank");
    
    if save_fig:
        file_name = f"{pos}-line_{filename_extra}"
        plt.savefig(out_dir + file_name + ".png", format="png")

#=======================
# FARE ANALYSIS 
#=======================

dow_list =  ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
dow_dict = dict(zip(range(7), dow_list))

def heatmap_min_fare(pdf_to_plot, first_day, num_days=7):
    """Heatmap of minimum fare by departure date and stay duration.
    One plot per shopping day.

    pdf_to_plot (pandas DataFrame): data to plot
    first_day (datetime): first search/shopping day to plot
    num_days (int): number of search/shopping days to plot.
        Default=7.
    """

    for i in range(num_days):
        search_dt = first_day + datetime.timedelta(days=i)
        one_search_day = pdf_to_plot[pdf_to_plot['searchDt_dt'] == search_dt]
        pvt_data = one_search_day.pivot(index="stay_duration", columns="outDeptDt", values="min_fare",)
        plt.figure(figsize=(10,2)) # w x h
        sns.heatmap(pvt_data, cmap='viridis', square=True,
                cbar_kws={'label': 'lowest fare (USD)',
                            'shrink': 0.5,
                            }
                );
        dow = dow_dict[datetime.date.weekday(search_dt)]
        plt.title(f"Search date: {search_dt.strftime('%Y-%m-%d')}")


    


def heatmap_min_fare_by_dow(market_pdf, market):
    market_pdf['dept_dt_dow'] = market_pdf['outDeptDt_dt'].apply(
        lambda d: datetime.date.weekday(d))
    market_pdf['return_dt_dow'] = market_pdf['inDeptDt_dt'].apply(
            lambda d: datetime.date.weekday(d))
    data_col = "min_fare"
    dow_summ = market_pdf.groupby(["dept_dt_dow", "return_dt_dow"])[data_col].mean()
    dow_summ = pd.DataFrame(dow_summ)
    dow_summ.reset_index(inplace=True)
    pvt = pd.pivot(data=dow_summ, index='return_dt_dow', columns='dept_dt_dow', values=data_col)
    sns.heatmap(pvt, cmap="coolwarm",
            xticklabels=dow_list, yticklabels=dow_list,
            cbar_kws={'label': "avg min fare"}
            );
    plt.xlabel("Departure DOW")
    plt.ylabel("Return DOW");
    plt.title(market)
    plt.show()


def generic_bar_chart(df, data_col, label_col=None, rotate_labels=False, title=None):
    xs = range(len(df))
    plt.bar(xs, df[data_col])
    if label_col:
        plt.xticks(xs, df[label_col])
        plt.xlabel(label_col)
    if rotate_labels:
        plt.xticks(rotation=90)
    plt.ylabel(data_col)
    plt.title(title)



# HISTOGRAM FROM SPARK DATA

from pyspark.ml.feature import Bucketizer
# example split_vals: 
# start with some nice intervals
split_vals = np.linspace(0, 1, 11)
# expand the right tail
split_vals = np.concatenate((split_vals, np.array([5., 10., 50., 100., 1000.])))


def histo_from_spark(df, spilt_vals, data_col, dropna=True):
    bucketizer = Bucketizer(splits=split_vals, inputCol=data_col, outputCol="bucket")
    if dropna:
        df_bin = bucketizer.transform(df.dropna(subset=[data_col]))
    else:
        df_bin = bucketizer.transform(df)

    hist_data = df_bin.groupBy("bucket").count().orderBy("bucket")
    hist_data_pdf = hist_data.toPandas()

    if not dropna:
        hist_data_pdf['llim'] = split_vals[:-1]
        hist_data_pdf['ulim'] = split_vals[1:]
        hist_data_pdf['label'] = hist_data_pdf['llim'].astype('str') + "_" + hist_data_pdf['ulim'].astype('str')
        label_col = 'label'
    else:
        label_col = 'bucket'

    generic_bar_chart(hist_data_pdf, 'count', label_col, True, data_col)




def plot_bar_cum_pct(data_df, top_n, bar_data_col, line_data_col, order_col, x_label, what,
    plot_title, file_name):
    """Example:
    bar_data_col = 'num_searches'
    line_data_col = 'cum_pct_searches'
    order_col = 'days_til_dept'
    x_label = 'advanced purchase (days)'
    top_n = 1000
    what = 'Searches'
    plot_title = 'Distribution of Searches by Advanced Purchase'
    file_name = 'dtd-overlay'
    """

    data_df = data_df.sort_values(by=order_col)[:top_n]
    data_bar = data_df[bar_data_col]
    data_line = data_df[line_data_col]
    xs = range(len(data_bar))

    fig, ax = plt.subplots(figsize=(10,5))
    ax.bar(xs, data_bar);
    ax.set_ylabel(f"Num {what}")
    ax.set_xlabel(x_label)

    ax2 = ax.twinx()
    ax2.plot(xs, data_line, marker=".", color="red")
    ax2.set_ylim(0, 1)
    ax2.set_ylabel(f"Cumulative % Total {what}")

    fig.suptitle(plot_title)
    plt.savefig(f"/tmp/{file_name}.png")