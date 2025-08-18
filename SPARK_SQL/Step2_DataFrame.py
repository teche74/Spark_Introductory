from pyspark.sql import SparkSession

def CreateSparkSession(app_name="SQL_Learning"):
    return SparkSession.builder.appName(app_name).getOrCreate()



def main():
    spark = CreateSparkSession()

    df = spark.createDataFrame([("Alice", 1) , ("Ram",  3) , ("Mohan", 2) , ("Sita", 4) , ("Ravi", 5) , ("Rani", 6) , ("Kiran", 7) , ("Nisha", 8) , ("Gita", 9) , ("Pooja", 10)], schema = ["Name", "Id"])
    # df.show()

    spark.stop()


if __name__ == "__main__":
    main()


