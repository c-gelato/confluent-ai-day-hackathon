# Lab5: Real-Time Brand Sentiment and Response Engine

This demo showcases a real-time brand monitoring workflow that turns raw social and news mentions into governed escalation drafts. Social media posts and news coverage are ingested as Schema Registry-backed events, Apache Flink aggregates short sentiment windows by brand, product, and region, and an AI agent drafts a response whenever sentiment falls below a business threshold.

## Business impact

- PR crisis prevention through early warning on fast-moving sentiment drops
- Marketing ROI measurement by tying campaign windows to live brand perception
- Accessible demo surface with a compact local dataset and no external tool-calling requirement

## Prerequisites

### Local dependencies

**Installation instructions:**

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli
```

**Windows:**

```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

### API keys and access

> [!NOTE]
>
> The credentials below are not required in instructor-led workshops — they will be provided for you.

- AWS Bedrock API keys OR Azure OpenAI endpoint and API key
  - Easy key creation: run `uv run api-keys create`

## Deploy the demo

If you have not already cloned the repo:

```bash
git clone https://github.com/confluentinc/quickstart-streaming-agents.git
cd quickstart-streaming-agents
```

Deploy the infrastructure and choose **Lab 5: Brand Sentiment + Response Engine**:

```bash
uv run deploy
```

This deploys the governed source table used by the lab. The event schema itself is registered when you publish the sample data in the next step.

## Use case walkthrough

### Data generation

Publish the sample social and news events:

```bash
uv run lab5_datagen
```

The publisher writes Avro records to the `brand_mentions` topic and registers the value schema in Schema Registry. That means every downstream Flink query uses the same governed event contract.

The source events include:

- `brand` and `product` for the monitored portfolio
- `region` for geographic segmentation
- `source_type` and `channel` for connector lineage
- `sentiment_score` as a numeric value from -1.0 to 1.0
- `headline`, `body`, and `url` to preserve business context for the AI layer

### 1. Visualize live sentiment windows

In the [Flink UI](https://confluent.cloud/go/flink), open a SQL workspace and run the following query:

```sql
SELECT
    window_start,
    window_end,
    brand,
    product,
    region,
    COUNT(*) AS mention_count,
    ROUND(AVG(sentiment_score), 3) AS avg_sentiment,
    SUM(CASE WHEN sentiment_score < 0 THEN 1 ELSE 0 END) AS negative_mentions
FROM TABLE(
    TUMBLE(TABLE brand_mentions, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, brand, product, region;
```

You should see one sharply negative window for `Acme / PulsePhone / NA-East` after the firmware-update complaint burst lands.

### 2. Create the sentiment alert stream

Now turn the aggregation into a continuous alerting table:

```sql
CREATE TABLE brand_sentiment_alerts AS
WITH brand_sentiment_windows AS (
    SELECT
        window_start,
        window_end,
        window_time,
        brand,
        product,
        region,
        COUNT(*) AS mention_count,
        ROUND(AVG(sentiment_score), 3) AS avg_sentiment,
        SUM(CASE WHEN sentiment_score < 0 THEN 1 ELSE 0 END) AS negative_mentions,
        SUM(CASE WHEN source_type = 'news' THEN 1 ELSE 0 END) AS news_mentions,
        SUM(CASE WHEN source_type = 'social' THEN 1 ELSE 0 END) AS social_mentions
    FROM TABLE(
        TUMBLE(TABLE brand_mentions, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
    )
    GROUP BY window_start, window_end, window_time, brand, product, region
)
SELECT
    brand,
    product,
    region,
    window_time,
    mention_count,
    negative_mentions,
    news_mentions,
    social_mentions,
    avg_sentiment,
    CASE
        WHEN avg_sentiment <= -0.80 THEN 'critical'
        WHEN avg_sentiment <= -0.60 THEN 'high'
        ELSE 'elevated'
    END AS severity,
    CONCAT(
        'Brand sentiment drop detected for ', brand, ' / ', product,
        ' in ', region,
        '. Average sentiment is ', CAST(avg_sentiment AS STRING),
        ' across ', CAST(mention_count AS STRING), ' mentions, including ',
        CAST(news_mentions AS STRING), ' news mentions and ',
        CAST(social_mentions AS STRING), ' social posts.'
    ) AS situation_summary
FROM brand_sentiment_windows
WHERE mention_count >= 5
  AND avg_sentiment <= -0.35;
```

Then inspect the alerts:

```sql
SELECT * FROM brand_sentiment_alerts;
```

This table is your decision boundary: only windows with enough traffic and a sufficiently negative average score move forward to the AI response stage.

### 3. Define the AI response agent

The next step is to draft a governed escalation response whenever a window crosses the threshold. The agent does not post externally; it produces a structured internal draft that marketing, support, or PR can approve.

See [CREATE AGENT documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/create-agent.html).

```sql
CREATE AGENT `brand_response_agent`
USING MODEL `llm_textgen_model`
USING PROMPT 'OUTPUT RULES:
1. Respond using exactly these four labeled sections, in this order:
Severity:
Customer Signal:
Draft Response:
Escalation Rationale:
2. Plain text only. No markdown, no bullets outside the labels, no extra sections.
3. Draft Response must be 2-4 sentences, suitable for an internal PR or social response draft.
4. Never promise a fix that is not grounded in the input. If the issue looks unresolved, say the team is investigating.

You are a brand sentiment escalation assistant.

Your job:
- Review the brand, product, region, severity, average sentiment, and mention volumes.
- Infer whether this looks like a localized complaint spike or a broader product-quality issue.
- Draft a calm, factual response for marketing or PR review.
- Recommend why the incident should or should not be escalated immediately.

Use this guidance:
- critical: likely incident response, executive visibility, cross-functional escalation
- high: urgent PR and support coordination
- elevated: monitor closely, route to product marketing or community support

Make the response actionable and concise. Mention the region and product by name.'
WITH (
  'max_iterations' = '6'
);
```

### 4. Generate response drafts with `AI_RUN_AGENT`

Run the agent continuously over every alert:

```sql
CREATE TABLE brand_response_drafts (
    PRIMARY KEY (brand, product, region, window_time) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    brand,
    product,
    region,
    window_time,
    severity,
    avg_sentiment,
    mention_count,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Severity:\\*{0,2}\\s*([^\\n]+)', 1)) AS model_severity,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Customer Signal:\\*{0,2}\\s*\\n([\\s\\S]+?)(?=\\n\\*{0,2}Draft Response:)', 1)) AS customer_signal,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Draft Response:\\*{0,2}\\s*\\n([\\s\\S]+?)(?=\\n\\*{0,2}Escalation Rationale:)', 1)) AS draft_response,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Escalation Rationale:\\*{0,2}\\s*\\n([\\s\\S]+)$', 1)) AS escalation_rationale,
    CAST(response AS STRING) AS raw_response
FROM brand_sentiment_alerts,
LATERAL TABLE(AI_RUN_AGENT(
    `brand_response_agent`,
    `situation_summary`,
    `brand`,
    `product`,
    `region`,
    `severity`
));
```

Now inspect the generated drafts:

```sql
SELECT * FROM brand_response_drafts;
```

The output gives you:

- a machine-readable severity field
- a short explanation of the customer signal pattern
- an AI-generated response draft for review
- an escalation rationale that can route the incident to PR, support, or product teams

## Conclusion

This lab turns governed event streams into a real-time sentiment response engine. Schema Registry keeps the social and news event contract consistent, Flink handles low-latency aggregation and thresholding, and the AI layer drafts response language fast enough to help teams intervene before a story grows into a broader brand event.

## Clean-up

```bash
uv run destroy
```

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Previous Lab**: [Lab4: Public Sector Insurance Claims Fraud Detection](./LAB4-Walkthrough.md)
