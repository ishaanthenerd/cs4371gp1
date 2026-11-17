# Part 2 — Text Classification Model Training

Team 30  - Avizeh
**Component:** Spark MLlib Model Training

## 1. Dataset Preparation and Exploration

For Part 2 of the project, we developed a text classification model using Apache Spark MLlib. The objective was to train a classifier capable of categorizing incoming news messages into four categories: World, Sports, Business, and Science/Technology.

We used the AG News dataset from Hugging Face, which contains 120,000 labeled news articles in its training split. The dataset was downloaded using a Python script (`download_data.py`) and saved in Parquet format for processing with Apache Spark.

After downloading the dataset, we used `explore_data.py` to load the articles into a Spark DataFrame and inspect the dataset schema and class distribution. The dataset contained two columns: `text`, representing the article content, and `label`, representing its numeric category.

The training data was evenly distributed among the four categories:

| Label | Category | Number of Articles |
|---|---|---:|
| 0 | World | 30,000 |
| 1 | Sports | 30,000 |
| 2 | Business | 30,000 |
| 3 | Sci/Tech | 30,000 |
| **Total** | | **120,000** |

This confirmed that all four classes were equally represented in the training dataset.

## 2. Model Development and Training

We implemented a machine learning pipeline using Apache Spark MLlib. The pipeline consisted of five stages:

1. **RegexTokenizer:** Splits each news article into lowercase words and removes punctuation.
2. **StopWordsRemover:** Removes common English words such as "the", "and", and "is" that provide relatively little information for classification.
3. **HashingTF:** Converts the filtered words into numerical term-frequency features using 4,096 hashed features.
4. **IDF:** Applies inverse document frequency weighting to create TF-IDF features, reducing the influence of frequently occurring terms.
5. **LogisticRegression:** Trains a multinomial logistic regression classifier to predict one of the four news categories.

The classifier was configured with a maximum of 20 training iterations and a regularization parameter of 0.1.

The model was trained using all 120,000 labeled articles from the AG News training split. Once training completed, the fitted MLlib PipelineModel was saved to `models/ag_news_classifier`.

Saving the complete pipeline allows the same preprocessing steps and trained classifier to be reused for batch and streaming predictions without retraining the model.

## 3. Preliminary Model Testing

After training, we developed `test_model.py` to load the saved classifier and verify that it generated reasonable predictions.

Four manually written example headlines were used for preliminary testing, with one headline representing each category.

The results were:

| Example Headline | Predicted Category | Predicted Probability |
|---|---|---:|
| The president met with world leaders to discuss peace. | World | 52.68% |
| The football team won the championship game. | Sports | 70.46% |
| Company stocks rose after record quarterly earnings. | Business | 52.88% |
| Scientists developed a new artificial intelligence chip. | Sci/Tech | 61.49% |

The model classified all four example headlines into their expected categories.

These results confirmed that the saved pipeline could be loaded successfully and used to generate predictions with associated probabilities. However, these examples were used only to verify functionality and do not represent a formal accuracy evaluation.

## 4. Model Persistence and Integration

The trained classifier was saved as a Spark MLlib PipelineModel and uploaded to the team's shared GitHub repository.

The saved model can be loaded using `PipelineModel.load()` and applied to new text without repeating the training process.

## 5. Limitations

Although the model successfully classified the four preliminary example headlines, this small test does not establish its overall classification accuracy.

## AI Assistance Disclosure

ChatGPT (OpenAI) was used as an AI assistance tool during the development of this project. It provided guidance on understanding project requirements, structuring the implementation, explaining technical concepts, suggesting code examples, and assisting with troubleshooting.

AI assistance was also used for report writing, including structuring, drafting and improving written content, as well as organising the README documentation. ChatGPT was also used to assist in preparing this AI disclosure.

All code suggestions provided by ChatGPT were independently reviewed, tested and executed locally by me. I verified the functionality of the code within my own development environment and was responsible for the implementation, testing, evaluation and final decisions made throughout the project.


# Part 2 live classification results

Team# 30 / Sammy / stv2200002 / part 2 live classification (not the model training)

## Data and model

Classifies text in 4 categories:
- World, Sports, Business, and Science/Technology (can always have more for future implmentation)

The btach and streaming classification use the aved model at modles/ag_newsclassifier
The model pipeline consists of RegexTokenizer, StopWordsRemover, HashingTF, IDF, and LogisticRegression.
These stages tokenize the text, remove stop words, construct numerical features, and predict a category.

Our first test, we used 1000 articles for ag news with 250 articles from each category.
We manually published example headline for the part2 demo in the Kafka topic.

These inputs were used for app functionality, not using collected from Finnhub which was in part 1


## Batch classification and evaluation

The workflow removes empty messages and duplicates before applying the saved model. It exports each message’s ID, text, predicted category, and confidence.

Produced results:
- Raw messages: 1000
- Nonempty messages: 1000
- Empty messages: 0
- Duplicates: 0

Category distrubtion:
- World: 237
- Sports: 288
- Buisness: 210
- Science/Technology: 265

Even though the input contained 250 articles from each category, the predicted counts differed but not by much

Predictions are stored in part2-out/demo-ag-news/predictions and category counts are stored in part2-output/demo-ag-news/summary.json

## Streaming

The recorded demo ran the part 2 classifier in google cloud shell using kafka topic part2 demo. Messages were JSON objects containing a text field.

The streaming app loaded the saved model, classified messages, and display counts by predicted category. Its processing trigger was set to 10 seconds. A checkpoint directory stored streaming progress.

For the demo, we manually sent headlines about politics, sports, business, and technology but will be connected with the rest of the project.

Here's the video for my porition specfically: https://drive.google.com/file/d/1ffTiki2TNP3Rod77OX2qRW9hXL4VHwej/view?usp=sharing 

The recording demonstrates the Part 2 Kafka-to-classification workflow. It does not demonstrate the complete Finnhub-to-Part-1-to-Kibana pipeline but can be later on.

## Conclusions and limitations

The batch and live workflows successfullly classified demo messages and their category counts matched when processing all 1000 articles. Note that the demo used AG News training data. Its results demo the functionality rather than independent model accuracy (which was in the first part of part 2).

## AI disclosure

Used codex (gpt astra 6.1) for code assistance and generating sample tests but was reviewed and revised accordingly for everything to work as intended. 
