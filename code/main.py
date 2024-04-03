from pyspark.sql import SparkSession

# custom implemented functions
from pipeline import *
from structs import *
from model_functions import *



# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IMDb pipeline") \
    .getOrCreate()

#To avoid some warnings
spark.conf.set("spark.sql.debug.maxToStringFields", 100) 

sc= spark.sparkContext

# Path to the directory containing CSV files
fullpath = './imdb/train*.csv'
extra_path = './imdb/extra/external.csv'
fullpath_test_hidden = './imdb/test_hidden.csv'
fullpath_validation_hidden = './imdb/validation_hidden.csv'


# Extract Data from csv files and transform the dataset
main = transform_main(csv_extract(spark, customSchema, fullpath))
test_hidden = transform_test_data(csv_extract(spark, customSchema_test, fullpath_test_hidden))
validation_hidden = transform_test_data(csv_extract(spark, customSchema_test, fullpath_validation_hidden))
directors = transform_directors(json_extract(spark, "./imdb/directing.json"))
writers = transform_writers(spark.read.json('./imdb/writing.json'))
extra = transform_external_data(csv_extract(spark, extra_schema, extra_path))

#combine external data with main data
main_with_external = left_join(main, extra, "movie")


main_with_external.show(5)


#join validation and test with writers and directors
validation_hidden = inner_join(validation_hidden, writers, 'movie',  directors)
test_hidden = inner_join(test_hidden, writers, 'movie',  directors)

# join main onle with writers and directors for the model. Dont use external
combined_main_writer_directors = inner_join(main, writers, 'movie',  directors)


#Build model using train files
model = build_classifier(combined_main_writer_directors)

predict_labels(model,test_hidden, "_test_hidden")
predict_labels(model,validation_hidden, "_val_hidden")



spark.stop()