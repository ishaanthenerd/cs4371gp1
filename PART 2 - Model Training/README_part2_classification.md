# Part 2 — News Classification Portion

Part 2 uses a saved Spark ML model to classify news text into World, Sports,
Business, and Sci/Tech.

It supports:
- Batch classification from Kafka or exported files.
- A reproducible sample of 50 messages for human evaluation.
- Live classification with running category counts.

## Files (added or edited)

- `PART 2 - Model Training/batch_predict.py`: clean and classify a batch.
- `PART 2 - Model Training/explore_data.py`: inspect dataset counts.
- `PART 2 - Model Training/test_model.py`: run four example predictions.
- `PART 2 - Model Usage/review_predictions.py`: sample and evaluate human labels.
- `PART 2 - Model Usage/stream_classify.py`: classify incoming Kafka messages.
- `models/ag_news_classifier`: saved model used by both classification workflows.

The model pipeline is:
RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression.

## Environment setup

Run commands from the repository root. Use Python 3.10–3.12 and Java 17
for the documented setup.

```bash
python3 -m venv .venv-part2
source .venv-part2/bin/activate
python -m pip install -r "PART 2 - Model Training/requirements.txt"

export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export SPARK_LOCAL_IP=127.0.0.1

mkdir -p part2-data part2-output part2-checkpoints
```

If `.venv-part2` already exists with dependencies installed, activate it
and set the environment variables without recreating it.

The saved model uses Spark 3.5.8. The requirements and Kafka connector
are pinned to that Spark version. Retraining is not required for this demo.

## Local Kafka demo in Google VM coding editor

Cloud Shell and a Compute Engine VM are separate environments. Run all
demo commands in the environment containing this project and its dependencies.

Docker must be available. On first use, create the Kafka container:

```bash
docker run -d \
  --name part2-demo-kafka \
  -p 127.0.0.1:9092:9092 \
  apache/kafka:3.9.1
```

For later sessions, start the existing container instead:

```bash
docker start part2-demo-kafka
```

Once Kafka is ready, create the topic:

```bash
docker exec part2-demo-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic part2-demo \
  --partitions 1 \
  --replication-factor 1
```

### Terminal 1: run live classification

Activate the environment and set its variables as shown above, then run:

```bash
spark-submit \
  --master 'local[2]' \
  --conf spark.sql.shuffle.partitions=4 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  "PART 2 - Model Usage/stream_classify.py" \
  --bootstrap-servers localhost:9092 \
  --topic part2-demo \
  --value-format json \
  --model models/ag_news_classifier \
  --checkpoint "part2-checkpoints/video-$(date +%Y%m%d-%H%M%S)" \
  --starting-offsets earliest
```

Wait for `Live classification started!`.

This command uses a new checkpoint and includes retained topic history.
To resume an existing run, reuse its checkpoint. Do not run two classifiers
against the same checkpoint simultaneously.

### Terminal 2: publish demo headlines

```bash
docker exec -i part2-demo-kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic part2-demo <<'EOF'
{"text":"The president met with world leaders to discuss peace."}
{"text":"The football team won the championship game."}
{"text":"Company stocks rose after record quarterly earnings."}
{"text":"Scientists developed a new artificial intelligence chip."}
EOF
```

Watch Terminal 1 for updated category counts. The trigger interval is
10 seconds, but processing may take longer.

Each execution sends four messages. Repeated messages count as separate
arrivals in the live workflow.

Stop the classifier with Ctrl+C. Stop the demo broker when finished:

```bash
docker stop part2-demo-kafka
```

## Batch classification

The input must contain at least 1,000 raw messages. File input supports JSON Lines, CSV with a header,
and Parquet, with a string `text` column.

Example using the locally prepared AG News demo file:

```bash
spark-submit \
  --master 'local[2]' \
  --conf spark.sql.shuffle.partitions=4 \
  "PART 2 - Model Training/batch_predict.py" \
  --input part2-data/demo-ag-news.jsonl \
  --model models/ag_news_classifier \
  --output part2-output/demo-run-002/predictions \
  --summary part2-output/demo-run-002/batch_summary.json
```

The demo file was prepared during local setup; it is not downloaded by
this command. For collected news, replace it with your actual export.

Use fresh output paths for each run. Existing outputs are not overwritten.
Spark writes predictions as a directory containing CSV part files.

## Verified demo results and limitations

During local setup:
- The saved model successfully classified four example headlines.
- Dataset exploration completed.
- Batch classification processed 1,000 unique AG News articles.
- Live classification processed the same articles with matching counts.

Predicted totals were:
World: 237; Sports: 288; Business: 210; Sci/Tech: 265.

These articles came from AG News training data, so the results demonstrate
functionality rather than independent model accuracy.

The recorded live demo used manually published headlines. It did not run
the full Finnhub/Part 1/Kibana pipeline.

For real collection, Kafka values must contain complete headlines.
The checked-in Part 1 producer publishes individual words and needs
adaptation before supplying suitable classification inputs.

