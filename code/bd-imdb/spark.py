from pyspark.sql import SparkSession


def build_spark():
    spark = SparkSession.builder \
        .appName("IMDb pipeline") \
        .getOrCreate()

    #To avoid some warnings
    spark.conf.set("spark.sql.debug.maxToStringFields", 100) 

    sc= spark.sparkContext.setLogLevel('WARN')

    return spark, sc