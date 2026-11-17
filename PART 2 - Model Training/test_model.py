
import argparse

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    args = parser.parse_args()

    spark = (

        SparkSession.builder
        .appName("CS4371-Model-Test")
        .getOrCreate()

    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        model = PipelineModel.load(args.model)

        examples = [

            ("The president met with world leaders to discuss peace.",),
            ("The football team won the championship game.",),
            ("Company stocks rose after record quarterly earnings.",),
            ("Scientists developed a new artificial intelligence chip.",)
    
        ]

        df = spark.createDataFrame(examples, ["text"])

        predictions = model.transform(df)

        labels = {

            0: "World",
            1: "Sports",
            2: "Business",
            3: "Sci/Tech"
   
        }

        for row in predictions.select(

            "text", "prediction", "probability"
    
        ).collect():
        
            predicted_label = int(row["prediction"])
            confidence = float(
                row["probability"][predicted_label]
       
            )

            print("\nArticle:", row["text"])
            print("Prediction:", labels[predicted_label])
            print(f"Probability: {confidence:.2%}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
