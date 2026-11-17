# Part 2 — Google Cloud VM run guide

These commands run Part 2 alongside your existing Part 1 processes, without changing
Part 1 files. Your VM producer sends complete headlines/messages. Each Kafka value
must be either plain UTF-8 text (default) or a JSON object with a top-level `text`
string (`--value-format json`). The repository's Part 1 producer currently shows
word-by-word publishing; use your confirmed full-message VM producer. Part 2 cannot
reconstruct headline boundaries from individual word records.

**Implementation only: no tests, installation, model execution, or VM deployment
were performed for these changes. Run and validate on your VM.**

## 1. VM environment

Use an Ubuntu VM with Python 3.10–3.12 and Java 17. These commands assume you have
cloned/uploaded the repository and changed into its root directory. All project
paths are passed as arguments. Keep the complete checked-in `models` directory.

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jre-headless python3-venv
python3 -m venv .venv-part2
source .venv-part2/bin/activate
python -m pip install -r "PART 2 - Model Training/requirements.txt"
export SETUPTOOLS_USE_DISTUTILS=local
export SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
mkdir -p part2-output part2-data part2-checkpoints
set -o pipefail

MODEL="models/ag_news_classifier"
BROKERS="localhost:9092"
TOPIC="topic1"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
```

Use the virtual environment's `spark-submit` (`.venv-part2/bin/spark-submit`) for
Part 2, even if Part 1 has a separate Spark installation. The saved model's metadata
identifies Spark **3.5.8**; requirements and the Kafka connector match it. Java 17
is supported by [Spark 3.5.8](https://spark.apache.org/docs/3.5.8/). Connector details
are in the [Spark Kafka guide](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html).

The examples run Spark locally on the VM using two cores. Adjust `--master` and
`--driver-memory` for your VM's available resources, allowing room for Part 1 and
Kafka. No Google Cloud API credentials are needed when using local VM disk paths.
Use persistent VM storage for model/output/checkpoint directories. `gs://` paths
are not configured by this setup; copy results to Cloud Storage separately if needed.

Kafka and topic1 must already exist and be reachable. Change `BROKERS` if Kafka is
on another VM; its advertised broker addresses must also be reachable from this VM.
Dependency installation and the initial connector resolution require outbound Internet.
The supplied commands assume your existing plaintext Kafka connection.

If an earlier installation reports missing `distutils` on Python 3.12 or that
PyArrow requires NumPy 2, install the updated requirements above: they include
Setuptools and pin PyArrow to a NumPy 1.x-compatible release. Do not upgrade NumPy
to 2 just to satisfy a newer Arrow release in this Spark 3.5 environment.
Run `java -version` and ensure Java 17 is selected; installing Java 17 does not
necessarily change an existing Java 21/25 default. After selecting Java 17 with
`sudo update-alternatives --config java`, rerun the `JAVA_HOME` export above.
The Cloud Shell terminal is separate from your Compute Engine VM; SSH to the VM
where Kafka runs before following this guide if that is your intended deployment.

## 2.1 AG News counts

The saved model is already included; do not retrain merely to run classification.
Download the training data if you need to produce the required class-count evidence:

```bash
python "PART 2 - Model Training/download_data.py" --output part2-data/ag_news_train.parquet
spark-submit --master 'local[2]' "PART 2 - Model Training/explore_data.py" --data part2-data/ag_news_train.parquet 2>&1 | tee part2-output/ag_news_counts.log
```

Expected official training split: 120,000 rows, 30,000 each for World (0), Sports
(1), Business (2), and Sci/Tech (3). Record your actual output in the report.
The downloader refuses to overwrite an existing file; skip it if already downloaded.

## 2.2 Saved model / optional training

The existing model contains RegexTokenizer, StopWordsRemover, HashingTF, IDF, and
multinomial LogisticRegression. Batch and live classification both use `$MODEL`.
If you choose to retrain, use a new path and point BOTH workflows to it:

```bash
spark-submit --master 'local[2]' --driver-memory 2g "PART 2 - Model Training/train_model.py" --data part2-data/ag_news_train.parquet --model models/ag_news_retrained
MODEL="models/ag_news_retrained"
```

Training refuses an existing model path unless explicitly given `--overwrite`.
`test_model.py` remains a four-example demonstration you may run later; it is not
the assignment's 50-message human evaluation.

## 2.3a Batch classification of at least 1,000 collected messages

Let your full-message Part 1 producer collect at least 1,000 records. Then take a
batch read of all retained topic1 records, from earliest to the latest offsets at
the time of the read:

```bash
spark-submit --master 'local[2]' --driver-memory 2g --packages "$KAFKA_PACKAGE" "PART 2 - Model Training/batch_predict.py" --bootstrap-servers "$BROKERS" --topic "$TOPIC" --value-format text --model "$MODEL" --output part2-output/predictions --summary part2-output/batch_summary.json 2>&1 | tee part2-output/batch.log
```

For a producer export instead of Kafka:

```bash
spark-submit --master 'local[2]' --driver-memory 2g "PART 2 - Model Training/batch_predict.py" --input part2-data/messages.jsonl --model "$MODEL" --output part2-output/predictions --summary part2-output/batch_summary.json
```

File input supports CSV with a header, JSON Lines, and Parquet, all with a string
`text` column. Use `--input-format csv|json|parquet` for extensionless directories.
JSON Lines example: `{"text":"Company earnings rose this quarter."}`.

The program reports raw, nonempty, and deduplicated totals, removes empty/whitespace
and duplicate messages, and enforces at least 1,000 raw messages and 50 unique usable
messages. It writes `id,text,category,confidence` for every cleaned message, counts
all four classes (including zeros), and saves five examples per class where available.
Confidence is the predicted class's probability. The JSON summary contains counts
and examples for the report. Kafka retention must still contain your collection;
expired records cannot be recovered by reading from earliest.

Spark writes a directory containing `part-*.csv` files and `_SUCCESS`. Keep all CSV
parts together. Existing prediction/summary paths are rejected to protect previous
runs; use new paths for a new snapshot. Quoted and multiline CSV text is supported.

## 2.3b Random sample and human evaluation

Create a reproducible random sample of exactly 50 messages from that prediction snapshot:

```bash
python "PART 2 - Model Usage/review_predictions.py" sample --predictions part2-output/predictions --output part2-output/hand_labels.csv --seed 4371
```

Open `hand_labels.csv` and fill only `human_label` with one of:
`World`, `Sports`, `Business`, `Sci/Tech`, or `None of these`.
Do not edit IDs/text. Predictions are omitted from the sample to avoid influencing
your judgments. These labels must come from your own review, not the classifier.
Preserve CSV quoting when editing; a spreadsheet can edit multiline cells.

```bash
python "PART 2 - Model Usage/review_predictions.py" evaluate --predictions part2-output/predictions --labels part2-output/hand_labels.csv --output part2-output/comparison.csv --summary part2-output/evaluation.json
```

The evaluator rejects blank/invalid labels, duplicates, changed text, unknown IDs,
and samples other than 50 rows. `comparison.csv` places human labels next to model
predictions/probabilities. `evaluation.json` reports accuracy on the four-class
subset, how many fit none, a confusion matrix (rows=human, columns=predicted), and
misclassified examples. If all 50 fit none, accuracy is null rather than dividing
by zero. Keep the exact prediction snapshot used to create the sample. Outputs
are never overwritten; use new paths for revisions.

## 2.3c Live class counts

In another terminal, activate `.venv-part2`, set the environment/variables from
step 1, and keep Part 1 running. Start the separate Part 2 classification application:

```bash
spark-submit --master 'local[2]' --driver-memory 2g --packages "$KAFKA_PACKAGE" "PART 2 - Model Usage/stream_classify.py" --bootstrap-servers "$BROKERS" --topic "$TOPIC" --value-format text --model "$MODEL" --checkpoint part2-checkpoints/class_counts --trigger '10 seconds' 2>&1 | tee part2-output/live_counts.log
```

This uses the same saved model, predicts each nonempty incoming message, and prints
running per-class counts at each trigger containing new data. Repeated messages
count as separate arrivals in the live query; batch cleanup removes repeated text.
Only classes encountered so far appear in the live output.

On a fresh checkpoint, `--starting-offsets earliest` (default) includes retained
history. Use `--starting-offsets latest` to start with future arrivals instead.
On restart, checkpoint offsets/counts take precedence over that setting. Use a
new checkpoint for a different topic/model/query; never share Part 1's checkpoint
or run two copies with the same checkpoint. Stop with Ctrl+C. Use a new log filename
if you want to preserve earlier logs.

Because Part 1 must remain unchanged, the class-count query runs in its own Spark
application beside Part 1. The PDF says to add a second query inside the Part 1
application; this separate-process arrangement does not merge the applications.
If your instructor requires a single application literally, that integration will
require a later authorized change to Part 1. Both processes can consume topic1
independently without taking messages away from one another.

## Submission evidence

Include code, prediction CSV parts, `hand_labels.csv`, `comparison.csv`, the class
counts/examples, evaluation summary, and observed live counts. Explain which topics
make sense for your source, where the model succeeds/fails, and a few real mistakes.
Use `REPORT_TEMPLATE.md` as a writing aid; do not submit it with placeholders.
Also follow the PDF's team/contribution/video and AI-use appendix/transcript rules.
No results or accuracy measurements have been fabricated or generated by this change.
