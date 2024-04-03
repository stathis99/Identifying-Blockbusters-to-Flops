from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType\

# Define custom schema for main 
customSchema = StructType([
    StructField("index", IntegerType(), True),
    StructField("tconst", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("startYear", StringType(), True),
    StructField("endYear", StringType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("numVotes", FloatType(), True),
    StructField("label", BooleanType(), True),
    # Add more fields as needed
])

# Define custom schema for test data
customSchema_test = StructType([
    StructField("index", IntegerType(), True),
    StructField("tconst", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("startYear", StringType(), True),
    StructField("endYear", StringType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("numVotes", FloatType(), True),
   
])


extra_schema= StructType([
    StructField("movie_id", StringType(), True),
    StructField("movie_name_ext", StringType(), True),
    StructField("certificate", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("gross(in $)", FloatType(), True),
])
