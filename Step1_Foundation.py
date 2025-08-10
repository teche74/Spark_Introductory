# follow this once you setup zpark on your pc 

from pyspark import SparkConf  , SparkContext

# SparkConf is little familiar on your mind realting it to configuration, yes you are right buddy , this is used to setup spark configurations
conf = SparkConf().setMaster('local').setAppName('my_app')

# I am assuming that we all learning and want to be it from scratch, initially when we use spark in single machine and for learning or self execution (we can say that we are in Standalone user mode) 'it is one of component of Spark Architecture'.  


# Sparks Basice : 
  # spark is actually introduced like a unified engine with ulitple capablities to provide single point for executing all opertions which previously requires multiple distributed applications.
  # Spark's Components : Spark SQL, Spark Real Time Streaming , Spark Core , Mesos , StandAlone User, YARN , MLllib , GraphX 
  # Spark's Core api internally work's in distru=ibuted manner, it make user looks that data is in single machine but actually behind the scene data is fragmented and distributed acorrs multiple nodes across cluster.



# ab samjhte hen cheezon ko : 
# Spark Context ek tareeke se ek object he access krne ka spark ko , like if we need class behaviour we have to create object.
# iske 2 paramters hen phla cluster url and second app name , normally cluster url jo ki 'setMaster('local')' is tarah likha he ye he btane ke liye ki hum spark ko kis tarah use kr rhen hen. 
  # 'local' : iska mtlv currently hum spark app ko local machine pr run krhe hen using single thread. iska mtlv hum bhi cluster se bhi connect nhi hen kyonki local kam kr rhen hen offcourse.
  # appname to simple hen ki hamri spark application ka naam kya he.

# ab hum create krenge context ya fir session
sc = SparkContext(conf = conf)

# ye 'sc' object ki tarah hi hen ab hum spark ki functionalities use kr skrte hen. 
