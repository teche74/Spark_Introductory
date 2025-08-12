# WHY SPARK COMES ? 

- Spark comes as a extended version of hadoop to provied a unified engine which provides distributed functionalities in a single point to reduce complexity and managiblity overhead.


# Spark Architecture ? 

- Spark Architecture contains several components : 
  - Spark SQL : To provide performing SQL queries on data. ALthough it not requires hadoop, it supports Hadoop Storage format like Parquet, Avro, Hive Table etc
  - Spark Real Time Streaming : It allows to perform operation on real time data without storing it on disk for adhoc anlaysis.
  - Standalone User : It provides single node system or local configuration for learning purpose.
  - Spark MLlib : It provides general Model creation, evaluation and other various techniques.
  - Spark GraphX : It provides algorithms and tuned customized Spark core api for creating vertex and edges , directed graphs etc.
  - YARN : Resource Manager for Spark.
  - Mesos : Cluster manager which operates on dynamic envoirnment and condition.
 
![Spark_Architecture](https://github.com/teche74/Spark_Introductory/blob/main/IMAGES/SparkArchitecture.png?raw=true)



# Spark Core Concepts

- `SparkContext` : To access Spark api we have to create object of it (As i think), and this object we created is called SparkContext and named as 'sc' in general

> When using pyspark terminal through cmd it have default created SparkContext as sc.
<img src="https://github.com/teche74/Spark_Introductory/blob/main/IMAGES/SparkTerminalContext.png?raw=true" alt="Pyspark_terminal_default_context" width="400" height="100">

> To create SparkContext when using Python script , We have to create context using SparkContext with configuration.
<img src="https://github.com/teche74/Spark_Introductory/blob/main/IMAGES/ContextCreationExample.png?raw=true" alt="Pyspark_terminal_default_context" width="800" height="300">

 whenever we set configuration we focuses only on two parameters :
   - .setMaster() `we can say it as cluster url` : It actually tells about the envoirment of our spark application.
     - `local` : It tells that our spark application working on local system with single thread without connecting to cluster.
     - `local[x]` : It tells that spark running on single local machine with x threads.
     - `local[*]` : It tells that spark running on single local machine with all threads.
     - `spark://HOST:PORT` → connect to a standalone Spark cluster.
     - `yarn` → run on Hadoop YARN
     - `mesos://HOST:PORT` → run on Apache Mesos
   - .setAppName() : It tells about the app name of Spark.

- Sparks specially follows `master-driver-executor` where master is `cluster manager`.
- 'Cluster Manager' allocates CPU and memory resources to application.
- 'Driver' job is to run main python script, create's spark context, converts transformation into DAG's, distribute tasks to executors and tells executor what to do, take progress report, check for alive nodes (progress also), collect result from executors and send back to cluster manager.
- 'Executors' job is to perform task, store and retrive data, return result to Driver.

<img src="https://github.com/teche74/Spark_Introductory/blob/main/IMAGES/SparkDistributedComponents.png?raw=true" alt="Pyspark_terminal_default_context" width="700" height="400">
