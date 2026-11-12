
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from pyspark.sql.functions import (

    col, trim, length, when

)

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array


parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--model", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

spark = (

    SparkSession.builder
    .master("local[2]")
    .appName("CS4371-Batch-Classification")
    .getOrCreate()

)

spark.sparkContext.setLogLevel("ERROR")

try:
    # loading input messages

    suffix = Path(args.input).suffix.lower()

    if suffix == ".csv":
        messages = (

            spark.read
            .option("header", "true")
            .csv(args.input)
       
        )

    elif suffix == ".parquet":
        messages = spark.read.parquet(args.input)
    
    else:
        raise ValueError("Input must be CSV or Parquet")

    if "text" not in messages.columns:
        raise ValueError("Input needs a 'text' column")

    # removing useless messages

    messages = (

        messages.select(trim(col("text")).alias("text"))
        .filter(col("text").isNotNull())
        .filter(length(col("text")) > 0)
        .dropDuplicates(["text"])
    
    )

    print("Unique non-empty messages:", messages.count())

    # trained model

    model = PipelineModel.load(args.model)

    predictions = model.transform(messages)

    # numeric predictions -> readable labels

    results = (

        predictions
        .withColumn(
            "category",
            when(col("prediction") == 0, "World")
            .when(col("prediction") == 1, "Sports")
            .when(col("prediction") == 2, "Business")
            .otherwise("Sci/Tech")
       
        )

        .withColumn(

            "probabilities",
            vector_to_array(col("probability"))
        
        )

        .withColumn(

            "confidence",
            col("probabilities")[
                col("prediction").cast("int")
            ]
       
        )

        .select(

            "text",
            "category",
            "confidence"
        
        )

        .cache()
    
    )

    print("\nMESSAGES PER CATEGORY:")
    results.groupBy("category").count().show()

    print("\nFIVE EXAMPLES PER CATEGORY:")

    for category in [

        "World", "Sports", "Business", "Sci/Tech"
    
    ]:
        print(f"\n--- {category} ---")

        rows = (

            results
            .filter(col("category") == category)
            .limit(5)
            .collect()
        
        )

        for row in rows:
            print("Text:", row["text"][:200])
            print(

                f"Probability: {row['confidence']:.2%}"
           
            )

    (
        results.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(args.output)
    )

    print("\nPredictions saved to:", args.output)

    results.unpersist()

finally:
    spark.stop()
