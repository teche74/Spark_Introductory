# To perform operations on RDD's we have 2 ways  : Transformations and actions . Transformations are the operations where end result must be a new RDD. Transforamtions follow lazy evaluation as they only processed when a ceratin action is called which makes them efficient in large data ecosystem

from pyspark import SparkConf, SparkContext


def CreateSparkContext():
    conf = SparkConf().setMaster('local[*]').setAppName('RDDTransformationsApp')
    return SparkContext(conf = conf)

def main():
    sc = CreateSparkContext()
    sc.setLogLevel("ERROR")

    data = [
        "The easiest way to satisfy the read-only requirement is to broadcast a primitive value or a reference to an immutable object",
        "When we are broadcasting large values, it is important to choose a data serialization format that is both fast and compact",
        "Working with data on a per-partition basis allows us to avoid redoing setup work for each data item"
    ]

    rdd = sc.parallelize(data)

    # flatMap : it helps 
    words = rdd.flatMap(lambda line : line.split(" "))

    for ele in words.collect():
        print(f"{ele}\n")
    

    # map : it is used to apply function to each unit/element
    CapitalizeWords = words.map(lambda word : word.upper())

    for ele in CapitalizeWords.collect():
        print(f"{ele}\n")


    # reduceByKey : it is used to aggregate keys of same objects or values
    word_pairs = words.map(lambda word : (word,1))
    pair_count = word_pairs.reduceByKey(lambda x, y : x + y)

    for word, count in pair_count.collect():
        print(f"{word} : {count} \n")


    # sortByKey : it is used to sort elements on basis of keys .
    sorted_with_high_pairs_first = pair_count.sortByKey(ascending = False)
    for word, count in sorted_with_high_pairs_first.collect():
        print(f"{word} : {count} \n")

    sc.stop()


if __name__ == "__main__":
    main()


# map(func) – Apply a function to each element.

# flatMap(func) – Like map, but can return 0 or more elements per input (flattens).

# filter(func) – Keep elements that pass a condition.

# mapPartitions(func) – Apply a function to each partition instead of each element.

# mapPartitionsWithIndex(func) – Like mapPartitions, but also passes the partition index.

# sample(withReplacement, fraction, seed=None) – Sample a fraction of the data.


# union(otherRDD) – Combine two RDDs (may have duplicates).

# intersection(otherRDD) – Elements present in both RDDs.

# subtract(otherRDD) – Elements in this RDD but not in the other.

# distinct([numPartitions]) – Remove duplicates.

# groupByKey([numPartitions]) – Group values with the same key.

# reduceByKey(func, [numPartitions]) – Combine values with the same key using func.

# combineByKey(createCombiner, mergeValue, mergeCombiners, [numPartitions]) – General aggregation.

# aggregateByKey(zeroValue, seqOp, combOp, [numPartitions]) – Aggregate with separate seq & comb steps.

# sortByKey([ascending], [numPartitions]) – Sort by key.

# sortBy(func, [ascending], [numPartitions]) – Sort by custom function.

# join(otherRDD, [numPartitions]) – Inner join by key.

# leftOuterJoin(otherRDD, [numPartitions]) – Left join by key.

# rightOuterJoin(otherRDD, [numPartitions]) – Right join by key.

# fullOuterJoin(otherRDD, [numPartitions]) – Full join by key.

# cogroup(otherRDD, ...) – Group data from multiple RDDs sharing the same keys.

# subtractByKey(otherRDD) – Remove elements with keys in another RDD.

# groupWith(otherRDD) – Group data from multiple RDDs into tuples.


# repartition(numPartitions) – Shuffle data into N partitions.

# coalesce(numPartitions, shuffle=False) – Reduce partitions (optionally with shuffle).

# glom() – Convert each partition into a list.

# zip(otherRDD) – Pair elements with elements from another RDD (must have same partitioning).

# zipWithIndex() – Pair each element with an index.

# zipWithUniqueId() – Pair each element with a unique ID.


# pipe(command) – Pipe elements to an external shell command.

# keyBy(func) – Create a key-value RDD by applying a function to each element for the key.

# partitionBy(numPartitions, partitionFunc) – Partition data using a custom partitioner.