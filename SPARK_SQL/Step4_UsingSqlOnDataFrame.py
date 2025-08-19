from pyspark.sql import sparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("SparkApp").getOrCreate()

data = ["The crimson lighthouse stood sentinel against the howling winds of autumn, its beacon cutting through layers of fog that rolled in from the restless sea.",
        "Maritime vessels relied on its steady pulse, a rhythmic heartbeat that guided them safely to harbor through treacherous waters.",
        "Quantum mechanics reveals the strange dance of particles at the subatomic level, where electrons exist in probability clouds rather than fixed orbits.",
        "Scientists continue to unravel these mysteries, discovering new phenomena that challenge our understanding of reality itself.",
        "The old bookshop on Maple Street harbored countless stories within its dusty shelves.",
        "Leather-bound classics mingled with contemporary fiction, while the scent of aged paper and ink created an atmosphere of intellectual sanctuary for wandering readers."
       ]

df = spark.createDataFrame(data , schema = ["lines"])


# We cannot directly apply sql to dataframe to do this , we have to first register it as tempView
df.createOrReplaceTempView("view1")

spark.sql("""
  Select * from view1 where view1.lines like 'e____'
""")
