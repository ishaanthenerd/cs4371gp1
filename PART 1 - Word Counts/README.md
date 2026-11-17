(1) Set the API_KEY and ES_PASSWORD fields as environment variables. These should come from your Finnhub account and Elasticsearch account, respectively. Place them in ~/.bashrc and run source ~/.bashrc to see the change take effect. 
(2) Set up a working Spark cluster and Kafka broker. 
(3) Run the following commands in four separate terminals (I set up Google Cloud for this):
> /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
> python3 'PART 1 - Word Counts'/stream_text.py 
> spark-submit \
    --master spark://127.0.0.1:7077 \
    --executor-memory 3g \
    --total-executor-cores 2 \
    'PART 1 - Word Counts'/count_word_freq.py
> python3 'PART 1 - Word Counts'/graph_word_freq.py
You should see debug output of messages being sent / received as well as the visualization ID in the final terminal. Make sure you have enough memory to run Spark + Kafka + Kibana; 16GB was sufficient for me. Setting explicit memory caps on Spark and Kafka is also a good idea.
-
(4) Expose Kibana's port (default: 5601) to localhost so that you can see the plot. Navigate in Elasticsearch (Analytics -> Visualize Library -> {name of your plot}) to get the graph.