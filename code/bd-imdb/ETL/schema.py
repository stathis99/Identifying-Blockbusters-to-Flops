from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType\

# Define custom schema for main 
main_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("tconst", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("numVotes", FloatType(), True),
    StructField("label", BooleanType(), True),
    # Add more fields as needed
])

# Define custom schema for test data
val_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("tconst", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("numVotes", FloatType(), True),
   
])

test_schema = val_schema

extra_schema= StructType([
    StructField("movie_id", StringType(), True),
    StructField("movie_name", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("certificate", StringType(), True),
    StructField("runtime", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("star", StringType(), True),
    StructField("gross(in $)", FloatType(), True),
])



