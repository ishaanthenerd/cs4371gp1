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

```bash
python -m pip install pyspark==3.5.8 datasets pyarrow
```

# Files:
download_data.py ->  
Downloads AG News and saves it as Parquet

explore_data.py ->  
Displays the dataset schema and class distribution

train_model.py ->  
Trains and saves the Spark MLlib classification pipeline

test_model.py ->  
Loads the saved model and tests four example headlines

The saved classifier is located in the repository's models/ag_news_classifier directory.

# Running the Project:
Run these commands from the PART 2 - Model Training directory. Replace example paths with paths appropriate for your environment.

1. Download the dataset:

```bash
python download_data.py --output data/ag_news_train.parquet
```

2. Explore the dataset:

```bash
python explore_data.py --data data/ag_news_train.parquet
```

3. Train the classifier:

```bash
python train_model.py --data data/ag_news_train.parquet --model ../models/ag_news_classifier
```

The trained model is saved as a Spark PipelineModel.

4. Test the model:

```bash
python test_model.py --model ../models/ag_news_classifier
```

This classifies 4 examples and displays their predicted category and probabilities.

# Model:
Algorithm: Multinomial Logistic Regression

Hashed features: 4,096

Maximum iterations: 20

Regularization parameter: 0.1

Feature representation: TF-IDF

# Dataset Exploration Results:

The AG News training dataset was downloaded from Hugging Face using `download_data.py` and saved locally in Parquet format.

After downloading the data, `explore_data.py` was used to load the dataset into a Spark DataFrame and inspect the number of articles in each category.

The dataset contains two columns:

- `text`: The news article text.
- `label`: The numerical category assigned to each article.

The class distribution obtained from Spark was:

| Label | Category | Number of Articles |
|---|---|---:|
| 0 | World | 30,000 |
| 1 | Sports | 30,000 |
| 2 | Business | 30,000 |
| 3 | Sci/Tech | 30,000 |
| **Total** | | **120,000** |

The dataset is balanced, with an equal number of training articles in each category.

# MLlib Pipeline Implementation:

The model was created using five stages in a Spark MLlib Pipeline:

**1. RegexTokenizer:** Splits the news article text into lowercase words and removes punctuation.

**2. StopWordsRemover:** Removes common words such as "the", "and", and "is" that generally provide less information for classification.

**3. HashingTF:** Converts the remaining words into numerical term-frequency features using 4,096 hashed features.

**4. IDF:** Applies inverse document frequency weighting to give more importance to informative words and produce TF-IDF features.

**5. Logistic Regression:** Uses the generated features to learn patterns associated with the four news categories.

The stages were combined into a single pipeline:

```python
pipeline = Pipeline(
    stages=[tokenizer, remover, hashing_tf, idf, lr]
)
```

Training was performed using:

```python
model = pipeline.fit(df)
```

The fitted pipeline was then saved using:

```python
model.write().overwrite().save(args.model)
```

Saving the model allows it to be reused for future predictions without retraining on the entire dataset.

# Preliminary Model Testing Results:

After training, `test_model.py` was used to load the saved classifier and test it on four example headlines, one representing each category.

The actual results were:

| Example Headline | Predicted Category | Probability |
|---|---|---:|
| The president met with world leaders to discuss peace. | World | 52.68% |
| The football team won the championship game. | Sports | 70.46% |
| Company stocks rose after record quarterly earnings. | Business | 52.88% |
| Scientists developed a new artificial intelligence chip. | Sci/Tech | 61.49% |

All four example headlines were classified into their expected categories.

These predictions confirm that the saved model can be loaded and used for classification. However, these four examples are preliminary functionality tests rather than a formal accuracy evaluation.

# Notes:
All dataset, model, and output paths are provided through command-line parameters rather than hardcoded local paths.

The four example headlines in test_model.py are to test functionality, not a formal accuracy evaluation. That comes in later.

The model was initially developed and tested locally using Python 3.11, Java 17, and PySpark 3.5.8.

During development, saving the trained model initially failed because the Windows Hadoop utilities were not configured. After setting up the required utilities, the trained pipeline was successfully saved to `models/ag_news_classifier`.

Because the classifier was trained on news articles, its performance on shorter headlines or text from different sources may vary. The formal evaluation using manually labeled streamed messages is handled separately in Part 2 — Model Usage.
