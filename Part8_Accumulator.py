# Accumulators are distributed counters/aggregators in Spark. 
# They allow workers (executors) to safely update a shared variable (e.g., sum, count) without violating RDD immutability. 
# The driver can then read the final aggregated value
from pyspark import SparkContext,SparkConf

def CreateSparkContext():
    conf = SparkConf().setMaster('local').setAppName('Accumulators')
    return SparkContext(conf = conf)

def main():
    url = "TempData/example.txt"
    sc = CreateSparkContext()

    count_white_spaces = sc.accumulator(0)

    rdd = sc.textFile(url)

    def count_space(line):
        nonlocal count_white_spaces
        count_white_spaces += line.count(' ')
        return line
    

    rdd.foreach(count_space)

    print(f"Total white spaces in file: {count_white_spaces.value}")

    sc.stop()




