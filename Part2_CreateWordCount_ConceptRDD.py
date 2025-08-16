# RDD's : Resilient Distributed Datasets , they are the fundamental building block for storing data in spark it help look data unified to user but processed and split to multiple nodes across the cluster in backend to provide effeciency , parallel execution and fault tolerance.

from pyspark import SparkConf,SparkContext

class WordCount:
    def __init__(self , data : list[int] | list[str] | None):
        self.data = data
        self.sc = SparkContext(conf = SparkConf().setMaster('local').setAppName('WordCountApp'))

    def __shut__(self):
        if self.sc is not None:
            self.sc.stop()

    def WordCount(self):
        if self.data is None:
            print("No data provided for word count.")
            return
        
        rdd = self.sc.parallelize(self.data)
        words = rdd.flatMap(lambda line : line.split(""))
        word_pair = words.map(lambda word : (word,1))
        pair_count= word_pair.reduceByKey(lambda x, y : x+ y)

        for word , count in pair_count.collect():
            print(f"{word} : {count}\n")
        
        self.__shut__()


def main():
    data = [
            "The easiest way to satisfy the read-only requirement is to broadcast a primitive value or a reference to an immutable object",
            "When we are broadcasting large values, it is important to choose a data serialization format that is both fast and compact",
            "Working with data on a per-partition basis allows us to avoid redoing setup work for each data item"
        ]
    wc = WordCount(data)

    wc.WordCount()



if __name__ == "__main__":
    main()
    
