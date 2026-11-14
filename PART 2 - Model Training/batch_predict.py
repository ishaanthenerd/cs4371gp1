"""Part 2 batch classification of collected messages (files or Kafka)."""
import argparse
import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

LABELS = ["World", "Sports", "Business", "Sci/Tech"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", help="Producer export file/directory")
    source.add_argument("--topic", help="Kafka topic containing full messages")
    parser.add_argument("--input-format", choices=["csv", "json", "parquet"])
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--value-format", choices=["text", "json"], default="text")
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True, help="New Spark CSV directory")
    parser.add_argument("--summary", required=True, help="New local JSON report file")
    args = parser.parse_args()
    summary_path = Path(args.summary)
    if summary_path.exists():
        parser.error("Summary exists; choose a new output path")
    spark = SparkSession.builder.appName("CS4371-Batch-Classification").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.topic:
            raw = (spark.read.format("kafka")
                   .option("kafka.bootstrap.servers", args.bootstrap_servers)
                   .option("subscribe", args.topic).option("startingOffsets", "earliest")
                   .option("endingOffsets", "latest").load())
            value = F.col("value").cast("string")
            text = value if args.value_format == "text" else F.get_json_object(value, "$.text")
            raw = raw.select(text.alias("text"))
        else:
            fmt = args.input_format or Path(args.input.rstrip("/")).suffix.lower().lstrip(".")
            if fmt == "jsonl":
                fmt = "json"
            if fmt not in {"csv", "json", "parquet"}:
                raise ValueError("Specify --input-format csv, json, or parquet for extensionless directories")
            reader = spark.read.format(fmt)
            if fmt == "csv":
                reader = reader.option("header", True).option("multiLine", True).option("escape", '"')
            raw = reader.load(args.input)
        if "text" not in raw.columns or not isinstance(raw.schema["text"].dataType, StringType):
            raise ValueError("Input must contain a string text column")
        raw = raw.select("text").cache()
        raw_count = raw.count()
        nonempty = raw.select(F.regexp_replace("text", r"^\s+|\s+$", "").alias("text")).filter(
            F.col("text").isNotNull() & (F.length("text") > 0)).cache()
        valid_count = nonempty.count()
        messages = nonempty.dropDuplicates(["text"]).cache()
        clean_count = messages.count()
        print(f"Raw messages: {raw_count}; nonempty: {valid_count}; unique nonempty: {clean_count}")
        if raw_count < 1000:
            raise ValueError("Collect at least 1,000 actual streamed messages before running Part 2")
        if clean_count < 50:
            raise ValueError("Need at least 50 unique nonempty messages for the hand-label sample")
        predictions = PipelineModel.load(args.model).transform(messages)
        labels = F.array(*[F.lit(label) for label in LABELS])
        results = (predictions.withColumn("category", F.element_at(labels, F.col("prediction").cast("int") + 1))
                   .withColumn("probabilities", vector_to_array("probability"))
                   .withColumn("confidence", F.col("probabilities")[F.col("prediction").cast("int")])
                   .select(F.sha2("text", 256).alias("id"), "text", "category", "confidence").cache())
        counts = {label: 0 for label in LABELS}
        counts.update({row.category: row['count'] for row in results.groupBy("category").count().collect()})
        examples = {}
        for label in LABELS:
            examples[label] = [row.asDict() for row in results.filter(F.col("category") == label).orderBy("id").limit(5).collect()]
            print(f"\n{label}: {counts[label]} messages")
            for row in examples[label]:
                print(f"{row['confidence']:.2%}: {row['text']}")
        results.write.mode("errorifexists").option("header", True).option("escape", '"').csv(args.output)
        summary = {"raw_messages": raw_count, "nonempty_messages": valid_count,
                   "unique_nonempty_messages": clean_count, "empty_removed": raw_count-valid_count,
                   "duplicates_removed": valid_count-clean_count, "class_counts": counts,
                   "examples": examples, "model": args.model,
                   "source": args.input or args.topic, "value_format": args.value_format}
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Predictions: {args.output}; summary: {args.summary}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
