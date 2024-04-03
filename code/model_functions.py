#for ml
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics
from sparkxgb import XGBoostClassifier
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import explode_outer, col, explode
import pandas as pd
import json
import ast
from pyspark.sql.functions import expr
from pyspark.sql import functions as F

from pyspark.mllib.tree import RandomForest


from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



def build_rf(df):
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df)

# Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = df.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)

    # Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                labels=labelIndexer.labels)

    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)
            
    return model





def build_classifier(df):
    # Convert boolean label column to numeric
    df = df.withColumn("label", col("label").cast("int"))

    # fix year feature
    indexer1 = StringIndexer(inputCol="year", outputCol="year_ndex", handleInvalid="keep")
    # fix writer feature
    indexer2 = StringIndexer(inputCol="first_writer", outputCol="first_writer_index", handleInvalid="keep")

    indexer3 = StringIndexer(inputCol="first_director", outputCol="first_director_index", handleInvalid="keep")


    # indexer4 = StringIndexer(inputCol="certificate", outputCol="certificate_index", handleInvalid="keep")
    # indexer5 = StringIndexer(inputCol="rating", outputCol="rating_index", handleInvalid="keep")
    # indexer6 = StringIndexer(inputCol="genre", outputCol="genre_index", handleInvalid="keep")

    #asseble all features
    assembler = VectorAssembler(
    inputCols=["year_ndex", "runtimeMinutes","first_writer_index","first_director_index","numVotes" ],
    outputCol="features")

    # Define the classifier
    lr = LogisticRegression(featuresCol='features', labelCol='label')



    # Create a pipeline
    pipeline = Pipeline(stages=[indexer1,indexer2,indexer3, assembler, lr])

    # Split the data into training and testing sets
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    model = pipeline.fit(train_data)

    # Make predictions
    predictions = model.transform(test_data)

    #predictions.show(100)

    # Evaluate the model
    evaluator = BinaryClassificationEvaluator(labelCol='label')
    auc = evaluator.evaluate(predictions)
    print("AUC:", auc)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol='label', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)
    print("Accuracy:", accuracy)
        
    return model

def build_full_classifier(df):

    df = df.withColumn("label", col("label").cast("int"))

    # fix year feature
    indexer1 = StringIndexer(inputCol="year", outputCol="year_ndex", handleInvalid="keep")
    # fix writer feature
    indexer2 = StringIndexer(inputCol="first_writer", outputCol="first_writer_index", handleInvalid="keep")

    indexer3 = StringIndexer(inputCol="first_director", outputCol="first_director_index", handleInvalid="keep")


    indexer4 = StringIndexer(inputCol="certificate", outputCol="certificate_index", handleInvalid="keep")
    indexer5 = StringIndexer(inputCol="rating", outputCol="rating_index", handleInvalid="keep")
    indexer6 = StringIndexer(inputCol="genre", outputCol="genre_index", handleInvalid="keep")

    #asseble all features
    assembler = VectorAssembler(
    inputCols=["numVotes", "runtimeMinutes", "year_ndex","first_writer_index","first_director_index", 
               "certificate_index", "rating_index", "genre_index"],
    outputCol="features")

    # Define the classifier
    lr = LogisticRegression(featuresCol='features', labelCol='label')
    #xgb = XGBoostClassifier(df)


    # Create a pipeline
    pipeline = Pipeline(stages=[indexer1,indexer2,indexer3,indexer4,indexer5,indexer6, assembler, lr])
    model = pipeline.fit(df)
    return model

def predict_labels(model, df, csv_name):

    #df.show(10)
    
    predictions = model.transform(df)

    predictions = predictions.withColumn("label", when(predictions["prediction"] == 0, "False").otherwise("True"))

    predictions.select("label").write.mode("overwrite").csv("predictions" +csv_name+".csv", header=False)

   