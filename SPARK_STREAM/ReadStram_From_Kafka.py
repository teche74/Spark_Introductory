from pyspark import SparkSession

spark = SparkSession.builder.appName().getOrCreate()

#read
data = spark.readStream.format('kafka')..option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test-topic").load()



#write
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
