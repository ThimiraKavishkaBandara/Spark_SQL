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
    rdd = rdd.map(lambda line: line.split("\t"))
    rdd = rdd.map(lambda cols: (cols[0], int(cols[1]), int(cols[2]), int(cols[3])))
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

# Load the gbooks file into an RDD and DataFrame
gbooks_rdd = load_gbooks_rdd()
gbooks_df = load_gbooks_df()

# Print the schema of the DataFrame
gbooks_df.printSchema()

# Stop the SparkContext
sc.stop()



