from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
valuesB = [(1, "Delhi", "India"), (2, "Tamil Nadu", "India"), (3, "London", "UK"), (4, "Sydney", "AUS"),
           (8, "New York", "USA"), (9, "California", "USA"), (10, "New Jersy", "USA"), (11, "Texas", "USA")]

TableB = spark.createDataFrame(valuesB, ["Roll_Num", "Place", "Country"])

window = Window.partitionBy().orderBy(F.col("Roll_Num"))

final_df = TableB.withColumn("Row_Num", F.row_number().over(window))
final_df.show()


