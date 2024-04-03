# Package Imports
from xgboost.spark import SparkXGBClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml import Pipeline
from pyspark.sql.functions import when, monotonically_increasing_id


# build XGBoost classifier, hyperparameter tuning and return best model
def build_xgboost_classifier(train_data):
    """Build an XGB classifier, perform hyperparameter tuning and return the best model."""
    # Label and feature columns
    label_name = 'label'
    feature_names = [x.name for x in train_data.schema if x.name != label_name]

    # Overwrite until pre-processing is fixed
    print(f'Features used: {feature_names}')

    # Create a xgboost pyspark regressor estimator and set device="cuda"
    xgb = SparkXGBClassifier(
        features_col=feature_names,
        label_col=label_name,
        num_workers=1,
        device="gpu",
    )

    # Create a pipeline
    pipeline = Pipeline(stages=[xgb])

    # Set up parameter grid for hyperparameter tuning
    param_grid = ParamGridBuilder() \
        .addGrid(xgb.n_estimators, [100, 200, 300]) \
        .addGrid(xgb.max_depth, [3, 5, 7]) \
        .addGrid(xgb.learning_rate, [0.1, 0.15, 0.2]) \
        .build()

    # Set up evaluator
    evaluator = BinaryClassificationEvaluator(
        labelCol=label_name,
        rawPredictionCol="prediction"
    )

    # Set up train-validation split
    tvs = TrainValidationSplit(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        trainRatio=0.8
    )

    # Fit the model
    model = tvs.fit(train_data)

    # Return best model from the tuning
    best_model = model.bestModel

    return best_model


def predict_labels(model, df, csv_name):
    """Predict labels for df, using model and save to predictions_csv_name"""
    predictions = model.transform(df)

    predictions = predictions.withColumn("label", when(predictions["prediction"] == 0, "False").otherwise("True"))
    predictions.select(["label"]).coalesce(1).write.mode("overwrite").csv("predictions_" + csv_name, header=False)



