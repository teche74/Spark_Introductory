#broadcast variable introduced to send small to medium sized data across all the nodes in a cluster effeciently without splitting it into multiple partitions
# it is read only variable and can be used to share data across all the nodes in a cluster
# it is useful when we have a small dataset that needs to be used by all the nodes

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("BroadcastAndAccumulator")
sc = SparkContext(conf=conf)

broadcast_var = sc.broadcast({
    "apple": "fruit",
    "banana": "fruit",
    "carrot": "vegetable",
    "grape": "fruit"
})

valid_items_acc = sc.accumulator(0)

def process_data(item):
    if item in broadcast_var.value:
        valid_items_acc.add(1) 
        return (item, broadcast_var.value[item])
    else:
        return (item, "not found")


test_data = sc.parallelize(["apple", "banana", "grape", "cherry"])

processed = test_data.map(process_data)

processed.foreach(print)


print("Valid items count:", valid_items_acc.value)
