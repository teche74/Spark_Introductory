from pyspark.sql import Row

data = [
    Row(category="Fruits", item="Apple", quantity=100, price=5),
    Row(category="Fruits", item="Banana", quantity=20, price=2),
    Row(category="Fruits", item="Orange", quantity=15, price=4),
    Row(category="Vegetables", item="Carrot", quantity=25, price=3),
    Row(category="Vegetables", item="Tomato", quantity=30, price=2),
    Row(category="Vegetables", item="Potato", quantity=40, price=1),
]

df = spark.createDataFrame(data)
df.show()

from pyspark.sql import functions as F

# total quantity
df.agg(
    F.sum("quantity").alias("total_qty"),
    F.avg("price").alias("average_selling_price")
).show()

# Category with highest profit
df.groupBy("category").agg(
    F.sum(col("quantity") * col("price")).alias("total_profit")
).orderBy("total_profit" , ascending  = False).limit(1).show()

# expensive element of each category
df.groupBy("category").agg(
    F.max("price").alias("max_price"),
    F.first("item").alias("item")
).show()
