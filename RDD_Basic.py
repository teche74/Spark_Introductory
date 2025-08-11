from pyspark import SparkConf, SparkContext

# Configure Spark
conf = SparkConf().setMaster('local').setAppName('WordCount')
sc = SparkContext(conf=conf)

# Ways to create RDDs

# 1. From a file
# Replace with your file path (absolute or relative)
rdd1 = sc.textFile('sample.txt')

# 2. From a Python collection (for learning purposes only)
rdd2 = sc.parallelize([
    "Hello World",
    "Hello Spark",
])


# Transformations : 

# flatMap → splits each line into words
new_rdd = rdd2.flatMap(lambda line: line.split())


# Actions
print("Words list:", new_rdd.collect())  # Returns list of words
print("Total words:", new_rdd.count())   # Counts number of words
print("First word:", new_rdd.first())    # Gets first word

# Stop SparkContext
sc.stop()
