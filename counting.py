from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

sc = SparkContext()
sqlContext = C

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
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("count1", IntegerType(), True),
        StructField("count2", IntegerType(), True),
        StructField("count3", IntegerType(), True)
    ])
    df = sqlContext.read \
        .option("delimiter", "\t") \
        .option("header", "false") \
        .schema(schema) \
        .csv("gbooks")
    return df

####
# 2. Counting (10 points): How many lines does the file contains? Answer this question via both RDD api & #Spark SQL
####

# Spark SQL 
gbooks_rdd = load_gbooks_rdd()
gbooks_df = load_gbooks_df()
num_lines = gbooks_df.count()
gbooks_df.selectExpr("count(1)").show()


# +--------+                                                                              
# |count(1)|
# +--------+
# |86618505|
# +--------+


