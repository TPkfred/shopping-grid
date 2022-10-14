from pyspark.ml.feature import Bucketizer

# start with some nice intervals
split_vals = np.linspace(0, 1, 11)
# expand the right tail
split_vals = np.concatenate((split_vals, np.array([5., 10., 50., 100., 1000.])))

bucketizer = Bucketizer(splits=split_vals, inputCol="abs_pct_error", outputCol="bucket")
df_bin = bucketizer.transform(df)

hist_data = df_bin.groupBy("bucket").count().orderBy("bucket")
hist_data_pdf = hist_data.toPandas()

hist_data_pdf['llim'] = split_vals[:-1]
hist_data_pdf['ulim'] = split_vals[1:]
hist_data_pdf['label'] = hist_data_pdf['llim'].astype('str') + "_" + hist_data_pdf['ulim'].astype('str')

generic_bar_chart(hist_data_pdf, 'count', 'label', True)