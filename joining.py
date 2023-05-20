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
schema = StructType([
        StructField("word", StringType(), True),
        StructField("count1", IntegerType(), True),
        StructField("count2", IntegerType(), True),
        StructField("count3", IntegerType(), True)])

####
# 5. Joining (10 points): The following program construct a new dataframe out of 'df' with a much smaller size.
####
# Register table name for SQL
# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via DataFrame API and #Spark SQL API
# Spark SQL API
gbooks_rdd = load_gbooks_rdd()
df = sqlContext.createDataFrame(gbooks_rdd, schema)
df.createOrReplaceTempView('gbooks')
df2 = df.select("word", "count1").distinct().limit(100)
df2.createOrReplaceTempView('gbooks2') 
joined_df = df2.join(df2, ['count1'], 'inner')
print(joined_df.count())
# val_df = sqlContext.sql("SELECT count(*) FROM gbooks2 a JOIN gbooks2 b ON a.count1 = b.count1")
# num_lines = val_df.collect()[0][0]

# output: 210



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
schema = StructType([
        StructField("word", StringType(), True),
        StructField("count1", IntegerType(), True),
        StructField("count2", IntegerType(), True),
        StructField("count3", IntegerType(), True)])

####
# 5. Joining (10 points): The following program construct a new dataframe out of 'df' with a much smaller size.
####
# Register table name for SQL
# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via DataFrame API and #Spark SQL API
# Spark SQL API
gbooks_rdd = load_gbooks_rdd()
df = sqlContext.createDataFrame(gbooks_rdd, schema)
df.createOrReplaceTempView('gbooks')
df2 = df.select("word", "count1").distinct().limit(100)
df2.createOrReplaceTempView('gbooks2') 
joined_df = df2.join(df2, ['count1'], 'inner')
print(joined_df.count())

