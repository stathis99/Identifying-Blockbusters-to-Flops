import pandas as pd


def csv_extract(spark, schema, fullpath):
    return spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .schema(schema) \
    .load(fullpath) \
    .withColumnRenamed('tconst','movie_id') \
    .withColumnRenamed('movie_id','movie') \
    .withColumnRenamed('primaryTitle','movie_name') \
    .withColumnRenamed('gross(in $)','gross') #\
    #.distinct()


def json_extract(spark, fullpath):

    pddf = pd.read_json(fullpath)
    spdf = spark.createDataFrame(pddf)

    return spdf
