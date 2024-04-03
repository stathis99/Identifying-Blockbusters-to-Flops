
from ETL.imports import csv_extract, json_extract
from ETL.transform import transform, transform_extra, transform_directors, transform_writers
from ETL.schema import main_schema, extra_schema, val_schema, test_schema
from ETL.joins import inner_join, left_join, left_join_extra
from spark import build_spark
from model_xgb import build_xgboost_classifier, predict_labels
from pyspark.sql.functions import col, monotonically_increasing_id

FULLPATH = './imdb/train*.csv'
EXTRA_PATH = './imdb/extra/external.csv'
TEST_PATH = './imdb/test_hidden.csv'
VAL_PATH = './imdb/validation_hidden.csv'
DIRECTOR_PATH = './imdb/directing.json'
WRITER_PATH = './imdb/writing.json'


# boiler plate shit, dont worry about it
spark, sc = build_spark()


# Extract data from imdb files and put in default schema
#transform contains combining years into 'year' and lowering title
main = transform(csv_extract(spark, main_schema, FULLPATH), label=True)

# extract data from extra db and put in schema correlating with data
# transform lowers title
extra = transform_extra(csv_extract(spark, extra_schema, EXTRA_PATH)).drop('movie')

# both cases do exact same 
val = transform(csv_extract(spark,val_schema, VAL_PATH), dropna=False)
test = transform(csv_extract(spark, test_schema, TEST_PATH), dropna=False)


#import directors and writers datasets. 
#The tranform function returns movie_id and count of writer/director
#print(main.count())
#
#print(json_extract(spark, DIRECTOR_PATH).count())
#print(spark.read.json(WRITER_PATH).count())

directors = transform_directors(json_extract(spark, DIRECTOR_PATH), main.select('movie','label'))
writers = transform_writers(spark.read.json(WRITER_PATH), main.select('movie', 'label'))

#print('directors')
#directors.show(5)
#
#print('writers')
#writers.show(5)

val = val.withColumn('index_join', monotonically_increasing_id())
test = test.withColumn('index_join', monotonically_increasing_id())

#val.show(5)

# left join on movie_name, runtime and year for main,val and test set 
main_extra = left_join_extra(main, extra)
val_extra = left_join_extra(val, extra)
test_extra = left_join_extra(test, extra)


#main_extra.show(5)
#val_extra.show(5)
#test_extra.show(5)


# inner join the main+extra with writer and director counts on movie ID for train,val and test 
main_complete = inner_join(main_extra, writers, "movie", directors)
val_complete = inner_join(val_extra, writers, "movie", directors)
test_complete = inner_join(test_extra, writers, "movie", directors)

val_complete = val_complete.sort("index_join")
val_complete = val_complete.drop("index_join")
test_complete = test_complete.sort("index_join")
test_complete = test_complete.drop("index_join")

#print("complete")
#main_complete.show(50)
#val_complete.show(50)
#test_complete.show(50)


model = build_xgboost_classifier(main_complete.drop("movie", "movie_name"))

predict_labels(model, val_complete.drop("movie", "movie_name"), "val_pred")
predict_labels(model, test_complete.drop("movie", "movie_name"), "test_pred")


spark.stop()