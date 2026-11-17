
import argparse

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    HashingTF,
    IDF
)

from pyspark.ml.classification import LogisticRegression


# file paths
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    spark = (

        SparkSession.builder
        .appName("CS4371-AGNews-Training")
        .getOrCreate()

    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        # loading training articles

        df = spark.read.parquet(args.data)

        print("Training articles:", df.count())

        # splitting text into lowercase words

        tokenizer = RegexTokenizer(

            inputCol="text",
            outputCol="words",
            pattern="\\W+",
            toLowercase=True
   
        )

        # removing common English words

        remover = StopWordsRemover(

            inputCol="words",
            outputCol="filtered_words"
    
        )

        # converting words into vectors

        hashing_tf = HashingTF(

            inputCol="filtered_words",
            outputCol="tf",
            numFeatures=4096
    
        )

        # calculating TF-IDF

        idf = IDF(

            inputCol="tf",
            outputCol="features"
   
        )

        # training logistic regression model

        lr = LogisticRegression(

            featuresCol="features",
            labelCol="label",
            maxIter=20,
            regParam=0.1,
            family="multinomial"
    
        )

        # one MLlib pipeline

        pipeline = Pipeline(

            stages=[tokenizer, remover, hashing_tf, idf, lr]
    
        )

        print("Starting model training...")

        model = pipeline.fit(df)

        print("Model training completed!")

        # saving the fittted pipeline

        writer = model.write()
        if args.overwrite:
            writer = writer.overwrite()
        writer.save(args.model)

        print("Model saved to:", args.model)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
