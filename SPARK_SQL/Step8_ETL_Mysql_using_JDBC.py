from pyspark.sql import SparkSession

session = SparkSession.builder.appName('ETL').getOrCreate()



df = (
  spark.read.format('jdbc')
  .option('url'  , 'your_mysql_url')
  .option('dtable', 'sql_table_you_Want_to_access')
  .option('username' , '...')
  .option('password' , '...')
  .load()
)

df.show() # to show few records

display(df) # to view entire data



df.write.format('jdbc').option('url'  , 'your_mysql_url').option('dtable', 'sql_table_you_Want_to_access').option('username' , '...').option('password' , '...').mode('append').save() # append / overwrite / ignore / error


# if you want batch insert 
df.write.format("jdbc") \
    .option("url", "jdbc:mysql://hostname:3306/dbname") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "target_table") \
    .option("user", "your_username") \
    .option("password", "your_password") \
    .option("batchsize", 10000) \    # control batch inserts
    .mode("append") \
    .save()
