from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkStreaming').getOrCreate()


# read
spark.readStream.format('socket').option('host' , 'localhost').option('port' , 90).load()



# write to console
qry = spark.writeStream.format('console').outputMode('complete').start()

qry.awaitTermination()
