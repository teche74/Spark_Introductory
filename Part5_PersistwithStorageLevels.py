# As we know that rdds will follow lazy computations whihc might be expensive and ineffecient in case of iterative algorithms, to prevent this we use persist where we cache data for future use


from pyspark import SparkConf,SparkContext
from pyspark import StorageLevel


def CreateSparkContext():
    conf = SparkConf().setMaster('local').setAppName('Understanding Caching')
    return SparkContext(conf = conf)


def main():
    sc = CreateSparkContext()

    data = [
        "The easiest way to satisfy the read-only requirement is to broadcast a primitive value or a reference to an immutable object",
        "When we are broadcasting large values, it is important to choose a data serialization format that is both fast and compact",
        "Working with data on a per-partition basis allows us to avoid redoing setup work for each data item"
    ]

    rdd = sc.parallelize(data)

    words_rdd = rdd.flatMap(lambda line : line.split(""))

    words_rdd.persist(StorageLevel.MEMORY_ONLY)    # words_rdd.cache() is words_rdd.persist(StorageLevel.MEMORY_ONLY)

    # Storage levels helps us to decide the strategy we want to apply while caching 
        # MEMORY_ONLY - Store RDD as deserialized Java objects in the JVM. If it does not fit in memory, some partitions will not be cached.
        # MEMORY_AND_DISK - Store RDD as deserialized Java objects in the JVM. If it does not fit in memory, store the partitions that do not fit on disk.
        # MEMORY_ONLY_SER - Store RDD as serialized Java objects in the JVM. If it does not fit in memory, some partitions will not be cached.
        # MEMORY_AND_DISK_SER - Store RDD as serialized Java objects in the JVM. If it does not fit in memory, store the partitions that do not fit on disk.
        # DISK_ONLY - Store RDD partitions only on disk.

    print("Total words:", words_rdd.count())

    counts = words_rdd.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
    print("Word counts:", counts.collect())

    print("Top 5 alphabetical:", words_rdd.takeOrdered(5))


    sc.stop()

