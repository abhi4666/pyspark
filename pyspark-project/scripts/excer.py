
from pyspark.sql import SparkSession
# from pyspark.sql

# from pyspark.sql import

spark = SparkSession.builder.appName("Spark Program").getOrCreate()

data = spark.createDataFrame([
    (1,'ABHI','Math'),
    (1,'ABHI','Science'),
    (1,'ABHI','Chem'),
    (2,"RAVI","Math"),
    (2,"RAVI","science")
],['ID','NAME',"SUB"]
)

data.show()

finaldf = data.groupBy(['ID','NAME']).agg((',',('SUB')))

# d1= data.select(F.md5('SUB').alias('hash'),('SUB')).show()

finaldf.show(truncate=False)
