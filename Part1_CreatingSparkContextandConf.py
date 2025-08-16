# According to me SparkContext is like creating an object of spark Core api to acceess RDD functionalities.
# SparkContext requires 2 basic configuration, first indicates cluster_url while other depicts app_name
    # cluster url is basically used to tell about the envoirment in which spark is operating
        # local : single threaded local machine with no interaction to cluster
        # local[x] :  x threads of local machine are operating with no interaction to cluster
        # local[*] : all threads of local machine are operating with no interaction to cluster
        # mesos/yarn url : connected to specific resource / cluster manager    


from pyspark import SparkContext,SparkConf

class SparkUtil:
    def __init__(self):
        self.sc = None

    def CreateSparkContext(self):
        conf = SparkConf().setMaster('local').setAppName('SparkApp')
        self.sc = SparkContext(conf = conf)


    def GetContext(self):
        if self.sc is not None:
            return self.sc
        return None

    def RemoveSparkContext(self):
        if self.sc is not None:
            self.sc.stop()
        else:
            print("No Spark Context Available")


def main():
    su = SparkUtil()

    su.CreateSparkContext()

    sc = su.GetContext()

    rdd = sc.parallelize([2,3,4,5,1])

    print(list(rdd.collect()))

    su.RemoveSparkContext()


if __name__  == "__main__":
    main()

    