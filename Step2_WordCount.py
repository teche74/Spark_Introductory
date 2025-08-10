# Start krte hen ek simple word Count program se

from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster('local').setAppName('my_app')

sc = SparkContext(conf = conf)


# ab suru krte hen , first lets read data 
rdd  = sc.textFile('filen_name.extension')

# rdd kya likh diya hena ... are ek cheez btana bhul gya yhan spark apne tarah se data store rkhta use pta he data crucial he, distributed manner me bhi , operation bhi perform honge to khin data loss na ho isilye nya data structure introduce kiya data ko store krne ke liye 'resilient distibuted dataset'.
# data ko store krta he single time input , no updation (immutable bhyi) , these are split into fragments and spread across multiple nodes.

# ab data mil gya to agla kam hoga i think lines abhi sab cheezen mat puchna yar me bhi sheekh rha hun kuch cheezen hum rtenge , baad me sheekhnege okay...
words = rdd.flatMap(lambda line : line.split())

# bola tha mat pucho , pta to chall gya na kya kiya bas mje lo
words_pair = words.map(lambda word : (wrod , 1))

words_count = word_pairs.reduceByKey(lambda a,b : a + b)

for word, count in word_counts.collect():
    print(word, count)


sc.stop()
