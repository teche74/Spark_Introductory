from pyspark.sql.functions import col, rand, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import random

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("department", StringType(), True)
])

data = [
    (
        i,
        f"Name_{i}",
        random.randint(20, 60),
        random.uniform(30000, 120000),
        random.choice(["HR", "Finance", "Engineering", "Sales"])
    )
    for i in range(1, 101)
]

df = spark.createDataFrame(data, schema)


# rank : create rank where same values provide common rank with gap for next values

from pyspark.sql.window import Window
from pyspark.sql.functions import *

spec = Window.partitionBy("department").orderBy("salary")

# every department order by salary
new_df = df.withColumn("rank" , rank().over(spec))

# highest salary for every deaprtment 
df.withColumn("rank" , rank().over(spec)).filter(col("rank") == 1).show()

# +---+-------+---+------------------+-----------+----+
# | id|   name|age|            salary| department|rank|
# +---+-------+---+------------------+-----------+----+
# | 99|Name_99| 55|30125.383275689197|Engineering|   1|
# |  3| Name_3| 53| 33000.93216240707|    Finance|   1|
# | 81|Name_81| 38|34891.229079786215|         HR|   1|
# | 44|Name_44| 40|34252.048751052935|      Sales|   1|
# +---+-------+---+------------------+-----------+----+
