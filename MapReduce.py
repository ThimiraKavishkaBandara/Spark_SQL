from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import desc


sc = SparkContext()
sqlContext = SQLContext(sc)

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)
def load_gbooks_rdd():
    rdd = sc.textFile("gbooks")
    rdd = rdd.map(lambda x: x.split("\t"))
    rdd = rdd.map(lambda y: (y[0], int(y[1]), int(y[2]), int(y[3])))
    return rdd

# Spark SQL - DataFrame API
schema = StructType([
        StructField("word", StringType(), True),
        StructField("count1", IntegerType(), True),
        StructField("count2", IntegerType(), True),
        StructField("count3", IntegerType(), True)])

####
# 4. MapReduce (10 points): List the three most frequent 'word' with their count of appearances
####

# Spark SQL

gbooks_rdd = load_gbooks_rdd()
df = sqlContext.createDataFrame(gbooks_rdd, schema)
df.createOrReplaceTempView('gbooks')
#val_df = sqlContext.sql("SELECT word, COUNT(*) AS 'count(1)' FROM gbooks GROUP BY word ORDER BY 'count(1)' DESC")
val_df = sqlContext.sql("SELECT word, COUNT(*) AS `count(1)` FROM gbooks GROUP BY word ORDER BY `count(1)` DESC")

#val_df.show(3)


val_df.orderBy("count(1)", ascending=False).show(3)
sc.stop()
# There are 18 items with count = 425, so could be different 
# +---------+--------+
# |     word|count(1)|
# +---------+--------+
# |  all_DET|     425|
# | are_VERB|     425|
# |about_ADP|     425|
# +---------+--------+



