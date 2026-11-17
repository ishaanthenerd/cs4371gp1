
## Setup and Execution

### 1. Configure Environment Variables

Set the `API_KEY` and `ES_PASSWORD` environment variables using your Finnhub and Elasticsearch account credentials, respectively.

Add the following to your `~/.bashrc` file:

```bash
export API_KEY="your_finnhub_api_key"
export ES_PASSWORD="your_elasticsearch_password"
```

Apply the changes by running:

```bash
source ~/.bashrc
```

### 2. Start Spark and Kafka

Before running the application, make sure you have a working **Apache Spark cluster** and **Kafka broker** configured and running.

### 3. Run the Application

Open **four separate terminal windows** and execute the following commands, one in each terminal.

**Terminal 1 – Start the Kafka broker**

```bash
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```

**Terminal 2 – Start the text streaming producer**

```bash
python3 'PART 1 - Word Counts'/stream_text.py
```

**Terminal 3 – Run the Spark word frequency processor**

```bash
spark-submit \
    --master spark://127.0.0.1:7077 \
    --executor-memory 3g \
    --total-executor-cores 2 \
    'PART 1 - Word Counts'/count_word_freq.py
```

**Terminal 4 – Generate the word frequency visualization**

```bash
python3 'PART 1 - Word Counts'/graph_word_freq.py
```

You should see debug output showing messages being sent and received throughout the pipeline. The final terminal should also display the **visualization ID**.

**Resource Requirements:**

Make sure your machine has sufficient memory to run Spark, Kafka, and Kibana simultaneously. In my experience, **16 GB of RAM** was sufficient.

Setting explicit memory limits for Spark and Kafka is also recommended to prevent excessive resource consumption.

### 4. Access the Kibana Visualization

Expose Kibana's port (default: `5601`) to localhost so that you can access the dashboard.

Once Kibana is accessible:

1. Open Kibana in your browser.
2. Navigate to **Analytics → Visualize Library**.
3. Locate and select the visualization corresponding to the ID generated in the previous step.

You should now be able to view the word frequency visualization.
