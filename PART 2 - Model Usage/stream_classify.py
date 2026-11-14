
import argparse


from pyspark.sql import SparkSession
from pyspark.sql.functions import (

    col, length, when, get_json_object, regexp_replace

)

from pyspark.ml import PipelineModel

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--value-format", choices=["text", "json"], default="text")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
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
            .option("startingOffsets", args.starting_offsets)
            .load()
    
        )

        value = col("value").cast("string")
        text = value if args.value_format == "text" else get_json_object(value, "$.text")

        messages = (

            raw_stream
            .select(text.alias("text"))
            .select(regexp_replace("text", r"^\s+|\s+$", "").alias("text"))
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
            .queryName("part2_class_counts")
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


if __name__ == "__main__":
    main()
