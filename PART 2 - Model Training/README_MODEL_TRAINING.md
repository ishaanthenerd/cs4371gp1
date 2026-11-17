# CS 4371 Group Project 1 — Part 2: Model Training
# Overview:
This section implements a text classification model using Apache Spark Mllib.
The classifier is trained on 120,000 labelled news articles from the AG News dataset. 

The four categories are: World (0), Sports (1), Business (2), and Sci/Tech (3).

The model uses a Spark MLlib pipeline consisting of RegexTokenizer, StopWordsRemover, HashingTF, IDF, and multinomial Logistic Regression.

# Requirements:
Python 3.11, 
Java JDK 17, 
PySpark 3.5.8, 
Hugging Face datasets, 
pyarrow, 

Install the Python dependencies:

python -m pip install pyspark==3.5.8 datasets pyarrow

# Files:
download_data.py -> 
Downloads AG News and saves it as Parquet

explore_data.py -> 
Displays the dataset schema and class distribution

train_model.py ->
Trains and saves the Spark MLlib classification pipeline

test_model.py->
Loads the saved model and tests four example headlines

batch_predict.py ->
Classifies messages from a CSV or Parquet file and exports predictions

The saved classifier is located in the repository's models/ag_news_classifier directory.

# Running the Project:
Run these commands from the PART 2 - Model Training directory. Replace example paths with paths appropriate for your environment.

1. Download the dataset:

python download_data.py --output data/ag_news_train.parquet

2. Explore the dataset:

python explore_data.py --data data/ag_news_train.parquet

3. Train the classifier:

python train_model.py --data data/ag_news_train.parquet --model ../models/ag_news_classifier

The trained model is saved as a Spark PipelineModel.

4. Test the model:

python test_model.py --model ../models/ag_news_classifier

This classifies 4 examples and displays their predicted category and probabilities.

# Model:
Algorithm: Multinomial Logistic Regression

Hashed features: 4,096

Maximum iterations: 20

Regularization parameter: 0.1

Feature representation: TF-IDF

# Notes:
All dataset, model, and output paths are provided through command-line parameters rather than hardcoded local paths.

The four example headlines in test_model.py are to test functionality, not a formal accuracy evaluation. That comes in later.
