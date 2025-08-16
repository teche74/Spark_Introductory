# Actions are the another way to operate on RDD where final result must be some default data type or an RDD 

from pyspark import SparkConf, SparkContext

def CreateSparkContext():
    conf = SparkConf().setMaster('local[*]').setAppName('RDD_Actions_Demo')
    return SparkContext(conf=conf)

def main():
    sc = CreateSparkContext()
    sc.setLogLevel("ERROR")

    data = [
        "The easiest way to satisfy the read-only requirement is to broadcast a primitive value or a reference to an immutable object",
        "When we are broadcasting large values, it is important to choose a data serialization format that is both fast and compact",
        "Working with data on a per-partition basis allows us to avoid redoing setup work for each data item"
    ]

    rdd = sc.parallelize(data)

    words_rdd = rdd.flatMap(lambda line: line.split(" "))

    print("\n=== ACTIONS DEMONSTRATION ===\n")

    # collect() - Brings the whole RDD into Python as a list
    print("1. collect(): Get all elements from RDD")
    print(words_rdd.collect())  

    # count() - Number of elements in the RDD
    print("\n2. count(): Total number of words")
    print(words_rdd.count())

    # first() - First element in RDD
    print("\n3. first(): First word")
    print(words_rdd.first())

    # take(n) - First n elements
    print("\n4. take(5): First 5 words")
    print(words_rdd.take(5))

    # takeSample(withReplacement, num, seed) - Random sample
    print("\n5. takeSample(False, 5): Random 5 words")
    print(words_rdd.takeSample(False, 5))

    # takeOrdered(n, key) - Smallest elements by default
    print("\n6. takeOrdered(5): First 5 in alphabetical order")
    print(words_rdd.takeOrdered(5))

    # reduce(func) - Aggregate values pairwise
    print("\n7. reduce(): Concatenate first few words")
    print(words_rdd.reduce(lambda a, b: a + " " + b))

    # countByValue() - Frequency of each element
    print("\n8. countByValue(): Count of each word")
    counts = words_rdd.countByValue()
    for word, cnt in counts.items():
        print(f"{word} : {cnt}")

    # foreach(func) - Apply a function to each element (no return)
    print("\n9. foreach(): Printing words directly from workers")
    words_rdd.foreach(lambda w: print("Word from worker:", w))

    # saveAsTextFile(path) - Save output to file
    print("\n10. saveAsTextFile(): Saving RDD to output folder 'output_words'")
    words_rdd.saveAsTextFile("output_words")

    # top(n) - Get largest elements
    print("\n11. top(5): 5 largest words (lexicographically)")
    print(words_rdd.top(5))

    sc.stop()

if __name__ == "__main__":
    main()
