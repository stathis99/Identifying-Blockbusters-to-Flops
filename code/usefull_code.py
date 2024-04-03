    #---- Clean runTimesMinutes col (MAD method) ----

    # There are 13 movies with NULL runtimeMinutes
    # df = df.filter(df.runtimeMinutes.isNull())
    # There are 790 rows with NULL numVotes
    # df = df.filter(df.numVotes.isNull())
    # Mean Imputation for runtimeMinutes and numVotes
    # imputer = Imputer(inputCols=["runtimeMinutes", "numVotes"],
    #                   outputCols=["runtimeMinutes", "numVotes"],
    #                   strategy="mean")
    # model = imputer.fit(df)
    # df = model.transform(df)

    # #fiter out Null runtimeMinutes - May need to change that
    # df = df.filter(df["runtimeMinutes"].isNotNull())

    # median = df.selectExpr('percentile_approx(runtimeMinutes, 0.5)').collect()[0][0]
    # # Calculate Median Absolute Deviation (MAD)
    # df = df.withColumn("MAD_runtimeMinutes", F.abs(F.col("runtimeMinutes") - median))

    # std_dev = df.select(F.stddev("MAD_runtimeMinutes")).collect()[0][0]
    # # filter out rows that MAD is 2 std away
    # df = df.filter(F.abs(df["MAD_runtimeMinutes"] - median) <= 5 * std_dev)
    # df = df.drop("runtimeMinutes")

    # #---- Clean numVotes col (MAD method) ----

    # # Calculate the median
    # median = df.selectExpr('percentile_approx(numVotes, 0.5)').collect()[0][0]
    # # Calculate Median Absolute Deviation (MAD)
    # df = df.withColumn("MAD_numVotes", F.abs(F.col("numVotes") - median))

    # std_dev = df.select(F.stddev("MAD_numVotes")).collect()[0][0]
    # # filter out rows that MAD is 2 std away
    # df = df.filter(F.abs(df["MAD_numVotes"] - median) <= 2 * std_dev)
    # df = df.drop("numVotes")