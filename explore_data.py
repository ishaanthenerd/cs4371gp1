
import argparse
from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()
parser.add_argument("--data", required=True)
args = parser.parse_args()

spark = (

    SparkSession.builder
    .master("local[2]")
    .appName("CS4371-AGNews")
    .getOrCreate()

)

spark.sparkContext.setLogLevel("ERROR")

# loading the training dataset

df = spark.read.parquet(args.data)

# numeric labels -> category names

df.createOrReplaceTempView("ag_news")

print("\nTOTAL ARTICLES:")
print(df.count())
print("\nDATASET SCHEMA:")
df.printSchema()
print("\nARTICLES PER CATEGORY:")

spark.sql("""

    SELECT
        label,
        
        CASE label
            WHEN 0 THEN 'World'
            WHEN 1 THEN 'Sports'
            WHEN 2 THEN 'Business'
            WHEN 3 THEN 'Sci/Tech'
        
        END AS category,
        
        COUNT(*) AS articles
    
    FROM ag_news
    GROUP BY label
    ORDER BY label

    """).show(truncate=False)

print("\nSAMPLE ARTICLES:")
df.show(5, truncate=80)

spark.stop()
