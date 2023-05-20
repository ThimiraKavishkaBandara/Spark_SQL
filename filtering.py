from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

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
def load_gbooks_df():
    rdd = sc.textFile("gbooks")
    rdd = rdd.map(lambda x: x.split("\t"))
    rdd = rdd.map(lambda y: (y[0], int(y[1]), int(y[2]), int(y[3])))
    df = rdd.toDF(["word", "count1", "count2", "count3"])
    return df

####
# 3. Filtering (10 points) Count the number of appearances of word 'ATTRIBUTE'
####

# Spark SQL
gbooks_rdd = load_gbooks_rdd()
gbooks_df = load_gbooks_df()
gbooks_df.filter(gbooks_df.word == "ATTRIBUTE").selectExpr("count(1)").show()

sc.stop()
# +--------+                                                                      
# |count(1)|
# +--------+
# |     201|
# +--------+


