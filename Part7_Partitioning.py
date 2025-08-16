# suppose there is a case where we are processing million of records data for adhoc queries, every querry executes make spark check each node to find data as data is randomly distributed across nodes which make the entire process slow and inefficient 
# Spark uses partition to decide data placing logic inside node (either hash based or range based etc) to ensure that data is accessed effeciently without accessing every node to improve effeciency.
# partition only work on pair RDD (key : value)

from pyspark import SparkConf,SparkContext
from pyspark import StorageLevel

def CreateSparkContext():
    conf = SparkConf().setMaster('local').setAppName('Partitioning')
    return SparkContext(conf = conf)


def main():
    sc = CreateSparkContext()
    file_url = "TempData/example.csv"
    rdd = sc.textFile(file_url)

    pair_rdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))


    # note that partitioning returns a new rdd as it places data with logic
    new_rdd  = pair_rdd.partitionBy(3) # 3 : no of parititons we want to create
    
    # we have to save this other wise it get lost 
    new_rdd.persist(StorageLevel.MEMORY_AND_DISK_SER) 

    # now any selective operation will be effeciently processed compare to normal rdds
    res = (
        new_rdd.reduceByKey(lambda x,y : x+y)
        .filter(lambda x : x[0].startswith('a'))
        .collect()
        )
    
    for word, count in res:
        print(f"{word} : {count}\n")

    sc.stop()