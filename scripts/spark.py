from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

valuesA = [(1, "Aaman", 90), (2, "Ravi", 90), (3, "Xien", 85), (4, "John", 80), (5, "Jalpa", 88), (6, "Jenny", 98),
           (7, "sheldon", 90), (8, "peterson", 76), (9, "David", 88)]

TableA = spark.createDataFrame(valuesA, ["Roll_Num", "Name", "Score"])

valuesB = [(1, "Delhi", "India"), (2, "Tamil Nadu", "India"), (3, "London", "UK"), (4, "Sydney", "AUS"),
           (8, "New York", "USA"), (9, "California", "USA"), (10, "New Jersy", "USA"), (11, "Texas", "USA")]
TableB = spark.createDataFrame(valuesB, ["Roll_Num", "Place", "Country"])

TableA.show()
TableB.show()
innerjoin = TableA.join(TableB,on="Roll_Num",how="left").fillna('NA')

innerjoin.show()
