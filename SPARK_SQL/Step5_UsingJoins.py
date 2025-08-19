from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkApp').getOrCreate()


# common column example
df1 = spark.createDataFrame([("Ram" , "Finance" , "1") , ("akash" , "Audit" , "1"),("Anish" , "IT" , "1"), ("Ajay" , "HR" , "3")] , schema = ["name" , "dept" , "id"])
df2 = spark.createDataFrame([("IT" , 45000) , ('HR' , 32000) , ('Audit' , 25000) , ('Finance' , 40000)] , schema = ["dept" , "salary"])

# left join
left_join_df = df1.join(df2 , on = "dept" , how = "left")
left_join_df.orderBy("salary" , ascending = False).show()

# right join
right_join_df = df1.join(df2, on = "dept" , how = "right")

# inner join
inner_join_df = df1.join(df2, on = "inner" , how = "inner")




# if both tables have no common column name 
df1 = spark.createDataFrame([("Ram" , "1") , ("akash" , "2"),("Anish", "4"), ("Ajay" , "3")] , schema = ["name", "dept_id"])
df2 = spark.createDataFrame([("IT" , 45000 , 1) , ('HR' , 32000 , 2) , ('Audit' , 25000 , 3) , ('Finance' , 40000 , 4)] , schema = ["dept" , "salary" , "id"])

df2.join(df1 , on = df1["dept_id"] == df2["id"] , how = "left").show()



# suppose if we want to create all possilbe combination 
df1.crossJoin(df2).show()
