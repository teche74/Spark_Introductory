from pyspark import SparkSession
# this is new right , this time we use spark session , before spark 2.0 version we have spark context which specially focused on Spark Core , after 2.0 version they provide session as unified element which have access to all components of architecture (core, sql , streaming etc)
# note that with SparkSession retrun type get dataframe

class Load_Format:
    def __init__(self):
        self.spark = SparkSession.builder.appName('LoadFile').getOrCreate()

    def LoadJson(self, file_url):
        return self.spark.read.json(file_url)
    
    def LoadTextFile(self,file_url):
        return self.spark.read.text(file_url)
    
    def LoadCSV(self, file_url):
        return self.spark.read.csv(file_url , header = True , inferSchema = True)
    

# if using SparkContext it returns SparkRDD

from pyspark import SparkConf,SparkContext
import json, csv
from io import StringIO


class LoadFormat2:
    def __init__(self, ):
        conf = SparkConf().setMaster('local').setAppName('LoadFile')
        self.sc = SparkContext(conf = conf)

    def LoadJson(self, file_url):
        content = "\n".join(self.sc.textFile(file_url).collect())
        return self.sc.parallelize(json.loads(content))
    
    def LoadTextFile(self,file_url):
        return self.sc.textFile(file_url)
    
    def LoadCSV(self, file_url):
        lines = self.sc.textFile(file_url)
        header = lines.first()
        data = lines.filter(lambda row: row != header)

        def loadRecord(line):
            reader = csv.DictReader([line], fieldnames=header.split(","))
            return next(reader)
        
        return data.map(loadRecord)
        

