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
