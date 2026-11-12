
import argparse


from pyspark.sql import SparkSession
from pyspark.sql.functions import (

    col, trim, length, when

)

from pyspark.ml import PipelineModel

parser = argparse.ArgumentParser()
parser.add_argument("--model", required=True)
parser.add_argument("--checkpoint", required=True)
parser.add_argument("--bootstrap-servers", default="localhost:9092")
parser.add_argument("--topic", default="topic1")
parser.add_argument("--trigger", default="10 seconds")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("CS4371-Live-Classification")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # loading the model trained on AG News

    model = PipelineModel.load(args.model)
    print("Trained MLlib model loaded!")

    # reading messages form Kafka

    raw_stream = (

        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    
    )

    messages = (

        raw_stream
        .selectExpr("CAST(value AS STRING) AS text")
        .select(trim(col("text")).alias("text"))
        .filter(col("text").isNotNull())
        .filter(length(col("text")) > 0)
    
    )

    predictions = model.transform(messages)

    # numeric labels -> category names

    classified = (

        predictions

        .withColumn(

            "category",
            when(col("prediction") == 0, "World")
            .when(col("prediction") == 1, "Sports")
            .when(col("prediction") == 2, "Business")
            .otherwise("Sci/Tech")
       
        )
    
    )

    # totalling messages per category

    category_counts = (

        classified
        .groupBy("category")
        .count()
    
    )

    # displaying totals

    query = (

        category_counts.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    
    )

    print("Live classification started!")
    query.awaitTermination()

finally:
    spark.stop()
