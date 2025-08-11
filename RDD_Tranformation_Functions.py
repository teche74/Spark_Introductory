from pyspark import SparkConf, SparkContext
from functools import reduce

data = [
    "Hello World",
    "Hello Spark",
    "Hello PySpark",
    "Hello Data",
    "Hello RDD",
]

def CreateSparkContext():
    conf = SparkConf().setMaster('local').setAppName('WordCount')
    sc = SparkContext(conf=conf)
    return sc


def main():
    sc = CreateSparkContext()

    rdd = sc.parallelize(data)

    # Transformations

    # flatMap → splits each line into words
    words_rdd = rdd.flatMap(lambda line: line.split())
    print(words_rdd.collect())

    # map → applies function to each element
    print(words_rdd.map(lambda word : word.upper()).collect())

    # filter → filters elements based on condition
    print(words_rdd.filter(lambda word : word.startswith('H')).collect())

    # reduce → aggregates elements using a function
    word_count = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    print(word_count.collect())

    # sortByKeys → sorts RDD by keys
    sorted_rdd = word_count.sortByKey()
    print(sorted_rdd.collect())

    # distinct → removes duplicate elements
    distinct_words = words_rdd.distinct()
    print(distinct_words.collect())

    # union → combines two RDDs
    another_rdd = sc.parallelize(["Hello Spark", "Hello World"])
    union_rdd = words_rdd.union(another_rdd)
    print(union_rdd.collect())

    # intersection → finds common elements in two RDDs
    intersection_rdd = words_rdd.intersection(another_rdd)
    print(intersection_rdd.collect())

    # cartesian → computes the Cartesian product of two RDDs
    cartesian_rdd = words_rdd.cartesian(another_rdd)
    print(cartesian_rdd.collect())

    # groupByKey → groups values by key
    grouped_rdd = word_count.groupByKey()
    print([(k, list(v)) for k, v in grouped_rdd.collect()])

if __name__ == "__main__":
    main()

