from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder.appName("Temp").getOrCreate()

df = spark.createDataFrame((
    ["data is everywhere, to handle data we have to operate on it properly"],
    ["to deal with this we introduce bigdata technologies"],
    ["data engineers are the one who handle these tuffs"]
), schema= ["line"]
)

words = df.select(explode(split(col("line") , " ")).alias("word"))
count_pairs = words.groupBy("word").count()
count_pairs.show()
