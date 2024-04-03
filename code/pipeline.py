
from pyspark.sql.functions import col, when

from pyspark.sql.functions import col 
import pandas as pd
import json

from pyspark.sql.functions import col

from pyspark.sql.functions import collect_list
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from unidecode import unidecode
from pyspark.sql.functions import lower, regexp_replace


remove_accents_udf = udf(lambda x: unidecode(x) if x is not None else x, StringType())


def csv_extract(spark, schema, fullpath):
    return spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .schema(schema) \
    .load(fullpath) \
    .withColumnRenamed('tconst','movie_id') \
    .withColumnRenamed('movie_id','movie') \
    .withColumnRenamed('primaryTitle','movie_name') \
    .withColumnRenamed('gross(in $)','gross') \
    .distinct()

def json_extract(spark, fullpath):

    pddf = pd.read_json(fullpath)
    spdf = spark.createDataFrame(pddf)

    return spdf




def transform_main(df):
    #---- Clean Date columns ----

    # There are no rows with startYear == endYear == NULL, thus the two columns can be combined
    df = df.withColumn("year",
                       when(col("startYear").contains("\\N"), col("endYear"))
                       .otherwise(col("startYear")))
    # Drop startYear and endYear columns
    df = df.drop("startYear", "endYear")

    #lowercase and remove accent from names
    df = df.withColumn("movie_name", lower(remove_accents_udf("movie_name")))
    df = df.withColumn("originalTitle", lower(remove_accents_udf("originalTitle")))

    #df.describe().show()
    # Drop null values in our base features
    df = df.dropna(subset=["numVotes", "runtimeMinutes", "year"])

    #df.describe().show()

    return df

def transform_test_data(df):

    # make 2 columns year 1
    df = df.withColumn("year",
                       when(col("startYear").contains("\\N"), col("endYear"))
                       .otherwise(col("startYear")))
    # Drop startYear and endYear columns
    df = df.drop("startYear", "endYear")

    #fill Null values for features as the avg of the column
    df = df.fillna({
        "runtimeMinutes": df.selectExpr("avg(runtimeMinutes) as avg_runtime").collect()[0]["avg_runtime"],
        "numVotes": df.selectExpr("avg(numVotes) as avg_numVotes").collect()[0]["avg_numVotes"],
        "year": df.selectExpr("avg(year) as avg_year").collect()[0]["avg_year"]
    })

    

    return df


def transform_external_data(df):
    #lowercase and remove accent from names
    df = df.withColumn("movie_name_ext", lower(remove_accents_udf("movie_name_ext")))


    return df

def inner_join(df, df2, cond, df3= None ):
    df = df.join(df2, cond, 'inner')

    if df3 is not None:
        df = df.join(df3, cond, 'inner')

    return df

def outer_join(df, df2, cond, df3= None ):
    df = df.join(df2, cond, 'outer')

    if df3 is not None:
        df = df.join(df3, cond, 'outer')

    return df

def left_join(df, df2, cond, df3= None ):
    df = df.join(df2, cond, 'left')

    if df3 is not None:
        df = df.join(df3, cond, 'left')

    return df




def get_info(df):
    # Print DataFrame schema
    #print("Schema:")
    #df.printSchema()

    # Get summary statistics for numeric columns
    print("\nSummary Statistics:")
    df.describe().show()

    # Get the total number of rows
    print("\nNumber of Rows:", df.count())

    # Display first few rows
    print("\nFirst 5 rows:")
    df.show(15)

    # Get column names
    print("\nColumn Names:", df.columns)

    # Get data types of columns
    print("\nData Types:")
    for col, dtype in df.dtypes:
        print(col, ":", dtype)



def transform_directors(df):

    #Drop rows with null values
    df = df.dropna()

    # Group by 'movie' and collect director names into a list
    grouped_df = df.groupBy('movie').agg(collect_list('director').alias('director_ids'))

    grouped_df = grouped_df.withColumn("first_director", col("director_ids")[0]).drop("director_ids")


    return grouped_df

def transform_writers(df):

    #Drop rows with null values
    df = df.dropna()

    # Group by 'movie' and collect director names into a list
    grouped_df = df.groupBy('movie').agg(collect_list('writer').alias('writer_ids'))

    grouped_df = grouped_df.withColumn("first_writer", col("writer_ids")[0]).drop("writer_ids")


    return grouped_df

        
    
def calc_director_score(main, directors_df):
    directors_df = directors_df.join(main[['movie', 'label']], 'movie', 'left')

# Drop rows with missing director_id values
    directors_df = directors_df[directors_df['director']!='\\N']

# Calculate mean label value for each director
    mean_label_per_director = directors_df.groupby('director')['label'].mean().reset_index()

# Merge mean_label_per_director with the original DataFrame on director_id
    director_scores_df = directors_df.merge(mean_label_per_director, on='director', suffixes=('', '_mean_director'))
# Group by movie_id and calculate the mean of mean label per director for each movie
    director_scores_df = director_scores_df.groupby('movie')['label_mean_director'].mean().reset_index()
    director_scores_df.columns = ['movie', 'mean_director_score']

    return director_scores_df


def calc_writer_score(main, writers_df):
    writers_df = writers_df.join(main[['movie', 'label']], 'movie', 'left')

# Drop rows with missing director_id values 
    writers_df = writers_df[writers_df['writer_id']!='\\N']

# Calculate mean label value for each director
    mean_label_per_writer = writers_df.groupby('writer_id')['label'].mean()

# Merge mean_label_per_director with the original DataFrame on director_id
    writer_scores_df = writers_df.merge(mean_label_per_writer, on='writer_id', suffixes=('', '_mean_writer'))

# Group by movie_id and calculate the mean of mean label per director for each movie
    writer_scores_df = writer_scores_df.groupby('movie_id')['label_mean_writer'].mean().reset_index()
    writer_scores_df.columns = ['movie_id', 'mean_writer_score']

    return writer_scores_df