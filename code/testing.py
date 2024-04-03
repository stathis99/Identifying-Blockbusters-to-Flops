# Package Imports
from pyspark.sql import SparkSession

# Custom Imports
from pipeline import csv_extract, transform_external_data, transform_main, transform_test_data
from structs import customSchema, customSchema_test
from model_xgb import build_xgboost_classifier, predict_labels


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IMDb pipeline") \
    .getOrCreate()

#To avoid some warnings
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

sc = spark.sparkContext

# Path to the directory containing CSV files
fullpath = './imdb/train*.csv'
extra_path = './imdb/extra/external.csv'
fullpath_test_hidden = './imdb/test_hidden.csv'
fullpath_validation_hidden = './imdb/validation_hidden.csv'


# Extract Data from csv files and transform the dataset
main = transform_main(csv_extract(spark, customSchema, fullpath))
test_hidden = transform_test_data(csv_extract(spark, customSchema_test, fullpath_test_hidden))
validation_hidden = transform_test_data(csv_extract(spark, customSchema_test, fullpath_validation_hidden))
#directors = transform_directors(json_extract(spark, "./imdb/directing.json"))
#writers = transform_writers(spark.read.json('./imdb/writing.json'))
#extra = transform_external_data(csv_extract(spark, extra_schema, extra_path))

#Build model using train files
model = build_xgboost_classifier(main)

predict_labels(model,test_hidden, "_test_hidden")
predict_labels(model,validation_hidden, "_val_hidden")

spark.stop()