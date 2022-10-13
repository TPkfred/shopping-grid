

# import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 

spark = SparkSession.builder.appName("test").getOrCreate()

sc = spark.sparkContext
print(sc)