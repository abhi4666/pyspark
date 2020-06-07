from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
print("Hello World")

spark = SparkSession.builder.appName("Test").getOrCreate()

valuesA = [(1, "Aaman", 90), (2, "Ravi", 90), (3, "Xien", 85), (4, "John", 80), (5, "Jalpa", 88), (6, "Jenny", 98),
            (7, "sheldon", 90), (8, "peterson", 76), (9, "David", 88)]
columns = ["num","name","score"]

df = spark.createDataFrame(valuesA,columns)
#
# TableA = spark.createDataFrame(valuesA, ["Roll_Num", "Name", "Score"])
#
# winndowspec = Window.partitionBy("Score").orderBy("Score")

# rank = TableA.withColumn("rank",F.rank().over(winndowspec)).show()

filterd = df.filter(df.score == 90).show()

