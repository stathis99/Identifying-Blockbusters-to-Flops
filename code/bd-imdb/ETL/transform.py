
from pyspark.sql.functions import col, lower, udf, when, mean, countDistinct, split, size, rand
from pyspark.sql.types import StringType, IntegerType
from unidecode import unidecode
from statistics import median

from pyspark.sql.functions import regexp_extract

remove_accents_udf = udf(lambda x: unidecode(x) if x is not None else x, StringType())



def transform(df, dropna=True, label=False):
    # There are no rows with startYear == endYear == NULL, thus the two columns can be combined
    df = df.withColumn("year",
                       when(col("startYear").contains("\\N"), col("endYear"))
                       .otherwise(col("startYear")))
    # Drop startYear and endYear columns
    df = df.drop("startYear", "endYear", "originalTitle", "index")

    #lowercase and remove accent from names
    df = df.withColumn("movie_name", lower(remove_accents_udf("movie_name")))

    if label:
        df = df.withColumn("label", df['label'].cast(IntegerType()))

    # Calculate median and mean
    median_numVotes = df.approxQuantile("numVotes", [0.5], 0.001)[0]
    mean_runtime = df.agg({"runtimeMinutes": "avg"}).collect()[0][0]

    #print(median_numVotes)
    #print(mean_runtime)

    # Fill NA values with median and mean
    df = df.fillna({"numVotes": median_numVotes, "runtimeMinutes": mean_runtime})

    if dropna:
        #df.describe().show()
        # Drop null values in our base features
        df = df.dropna(subset=["numVotes", "runtimeMinutes", "year"])

        #df.describe().show()

    return df


def transform_extra(df):
    df = df.withColumn("runtimeMinutes", regexp_extract(df["runtime"], r"(\d+)", 1).cast("int"))
    #df = df.withColumn("runtimeMinutes", col("runtime").substr(1, 2).cast("int"))
    df = df.withColumn("movie_name", lower(remove_accents_udf("movie_name")))
    df = df.fillna({'certificate': "Not Rated"})
    df = df.drop('runtime')
    df = df.drop('rating')

    df = df.withColumn("star", size(split(df["star"], ",")))
    df = onehot_genre(df)
    df = onehot_certificate(df)


    return df


def transform_val(df):
    df = df.withColumn("year",
                       when(col("startYear").contains("\\N"), col("endYear"))
                       .otherwise(col("startYear")))
    # Drop startYear and endYear columns
    df = df.drop("startYear", "endYear", "originalTitle")

    return df


def transform_test(df):
    transform_val(df)



def transform_directors(df, labels):
    df = calc_mean_director_Score(df, labels)
    num_directors = df.groupby('movie').agg(countDistinct('director').alias('num_director'))

    df = df.join(num_directors, "movie", how='left')
    df = df.withColumn("num_director", df['num_director'].cast(IntegerType()))

    return df.drop('director').distinct()



def transform_writers(df, labels):
    df = calc_mean_writer_Score(df, labels)

    num_writers = df.groupby('movie').agg(countDistinct('writer').alias('num_writers'))
    df = df.join(num_writers, "movie", how='left')

    df = df.withColumn("num_writers", df['num_writers'].cast(IntegerType()))

    return df.drop('writer').distinct()


def onehot_genre(df):
    df = df.withColumn("genre", lower(col("genre"))).fillna("")

     # Define movie genres list
    movie_genres = ['action', 'adventure', 'animation', 'biography', 'crime', 'family', 'fantasy', 'film-noir', 'history', 'horror', 'mystery', 'romance', 'scifi', 'sports', 'thriller', 'war', 'comedy']

     # Iterate through genres and create new columns
    for genre in movie_genres:
        col_name = 'genre_' + genre
        # Check if genre exists in the genre column and set 1 or 0 accordingly
        df = df.withColumn(col_name, (col("genre").contains(genre)).cast("int"))
    df = df.drop('genre')

    return df

def onehot_certificate(df):
    df = df.withColumn("certificate", lower(col("certificate"))).fillna("")

     # Define movie ratings list
    movie_ratings = ['Not Rated', 'R', 'PG-13', 'Passed', 'Approved']

     # Iterate through ratings and create new columns
    for rating in movie_ratings:
        col_name = 'movie_rating_' + rating.replace(' ', '_')
        # Check if rating exists in the genre column and set 1 or 0 accordingly
        df = df.withColumn(col_name, (col("certificate").contains(rating)).cast("int"))
    df = df.drop('certificate')

    return df


def calc_mean_director_Score(df, labels):
    df = df.join(labels, 'movie', how='left')

    # Drop rows with missing writer_id values
    #df = df.filter(df['director'] != '\\N')

    # Calculate mean label value for each writer
    label_per_director = df.groupby('director').agg(mean('label').alias('label_mean_director'))

    df = df.join(label_per_director, on='director', how='left').select('director', 'movie', 'label_mean_director')

    # Group by 'movie_id' and calculate the mean of mean label per writer for each movie
    mean_label_per_director = df.groupby('movie').agg(mean('label_mean_director').alias('mean_director_score'))

    df = df.join(mean_label_per_director, on='movie', how='left')


    df = df.withColumn("mean_director_score", 
                   when((col("mean_director_score") == 0) | (col("mean_director_score") == 1), 
                        rand() * 0.4 + 0.3).otherwise(col("mean_director_score")))


    return df.select('movie', 'director','mean_director_score')



def calc_mean_writer_Score(df, labels):
   # print(df.count())
    df = df.join(labels, 'movie', how='left')
   #print(df.count())

    # Drop rows with missing writer_id values
    #df = df.filter(df['writer'] != '\\N')

    # Calculate mean label value for each writer
    label_per_writer = df.groupby('writer').agg(mean('label').alias('label_mean_writer'))

    df = df.join(label_per_writer, on='writer', how='left').select('writer', 'movie', 'label_mean_writer')

    #print(df.count())

    # Group by 'movie_id' and calculate the mean of mean label per writer for each movie
    mean_label_per_writer = df.groupby('movie').agg(mean('label_mean_writer').alias('mean_writer_score'))

    df = df.join(mean_label_per_writer, on='movie', how='left')

    #print(df.count())
    df = df.withColumn("mean_writer_score", 
                   when((col("mean_writer_score") == 0) | (col("mean_writer_score") == 1), 
                        rand() * 0.4 + 0.3).otherwise(col("mean_writer_score")))

    return df.select('movie', 'writer','mean_writer_score')
