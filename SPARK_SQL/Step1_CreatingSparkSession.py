# Spark session is introduced after version 2.0 and is exetnded version of Spark Context.
# Spark Context is limited to Spark Core functionalities but spark Session is unified entry point which provides access to all Spark functionalities.
# It is used to create DataFrame, register DataFrame as table, execute SQL queries, and read data from various sources.

from pyspark import SparkSession


def create_spark_session(app_name = "SQL_Learning"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    spark = create_spark_session()
    print("Spark Session created successfully!")
    print(f"App Name: {spark.sparkContext.appName}")
    spark.stop()
