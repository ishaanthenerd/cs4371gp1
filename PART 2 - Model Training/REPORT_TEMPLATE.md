# Part 2 results — complete after running on the VM

Team / member names / NetIDs / contribution table / video link: [fill in as required]

## Data and model

- AG News total and per-class counts: [from ag_news_counts.log]
- Source, Kafka topic, collection period: [actual full-message Part 1 collection]
- Raw / nonempty / unique messages: [from batch_summary.json]
- Saved model used for BOTH batch and streaming: [path]
- Pipeline: RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression.

## Batch and hand evaluation

- Per-class counts and five examples with probabilities: [batch_summary.json]
- Sampling seed: [4371 unless changed]
- Human-labeled messages: 50; in-scope: [N]; None of these: [N].
- Accuracy on in-scope messages only: [evaluation.json]
- Which predicted topics make sense for this source, and why: [your analysis]
- Which messages work well/badly, and why: [your analysis]
- Concrete incorrect examples: [comparison.csv / evaluation.json, explain each]

## Streaming

- Model / trigger / checkpoint / start offsets: [actual values]
- Observed running class counts at several triggers: [live_counts.log]
- Deployment: Part 2 runs alongside unchanged Part 1 as a separate application.

## AI disclosure

List tools, who used each, and purpose; include the required full conversation PDFs
in ai_transcripts. This change used Codex for implementing missing Part 2 workflows
and VM documentation. No tests or model runs were performed in this implementation session.
