# Lab5: Real-Time Brand Incident Command Center

This hackathon lab reframes brand sentiment monitoring as a real-time incident response system. Instead of watching one sentiment stream in isolation, the application correlates public social and news signals with customer support pressure and product rollout events, then uses AI to draft a routed response plan.

This makes the app materially stronger for the category criteria:

- **Confluent Connectors** ingest external operational systems and publish action streams outward.
- **Stream Processing** correlates multiple live event streams, not just one aggregation.
- **Stream Governance** uses Schema Registry-backed contracts across source, derived, and action topics.
- **Business impact** is framed around PR crisis prevention, response SLA reduction, campaign protection, and churn-risk mitigation.

## Architecture

### Source streams

- `brand_mentions` from social and news connectors
- `support_cases` from CRM or support-platform connectors such as Zendesk or Salesforce
- `product_release_events` from engineering release pipelines or deployment webhooks

### Derived streams

- `brand_incident_alerts` from Flink multi-stream correlation
- `brand_response_actions` from AI-generated escalation and response decisions

### Action sinks

- Slack, Teams, Jira, ServiceNow, or webhook sink connectors

The key design shift is that Lab 5 is no longer just a sentiment dashboard. It becomes an **AI-powered brand incident command center**.

## Business impact

- Detects potential brand incidents within minutes instead of waiting for manual escalation
- Correlates sentiment drops with support surges and rollout changes to reduce false positives
- Produces governed machine-readable action events for PR, support, and product operations
- Creates a measurable outcome story: faster response time, lower escalation latency, and better campaign protection

## Competitive demo story

In the hackathon demo, a firmware rollout triggers:

1. A burst of negative public posts and news coverage
2. A parallel spike in support cases
3. A recent product release event that explains the timing
4. A Flink-generated incident alert with severity, blast radius, and likely cause
5. An AI-generated response action routed to the correct team with a response draft and escalation rationale

That gives judges a much stronger end-to-end story than a single stream of sentiment averages.

## Governance model

Lab 5 should use Schema Registry for every event contract, not just the initial input stream.

Recommended governed topics:

- `brand_mentions`
- `support_cases`
- `product_release_events`
- `brand_incident_alerts`
- `brand_response_actions`

Recommended governance story for the demo:

- Compatibility mode enabled for producer-safe evolution
- Additive schema evolution shown with defaults and nullable fields
- Stable downstream contracts for PR, support, and incident-routing consumers
- Clear separation between source facts, derived alerts, and AI action events

See [Flink-schema.sql](./Flink-schema.sql) for the suggested governed Flink schemas and derived tables.

## Prerequisites

### Local dependencies

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli
```

**Windows:**

```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

### API keys and access

- AWS Bedrock API keys OR Azure OpenAI endpoint and API key
- Confluent Cloud access
- For a production-grade demo, credentials for the chosen source and sink connectors

> [!NOTE]
>
> The local sample publisher in this repo now simulates all three inbound streams: `brand_mentions`, `support_cases`, and `product_release_events`. For hackathon judging, the stronger live-demo version is to replace those simulated sources with real connectors and add one outbound sink connector for `brand_response_actions`.

## Deploy the demo

```bash
git clone https://github.com/confluentinc/quickstart-streaming-agents.git
cd quickstart-streaming-agents
uv run deploy
```

Choose **Lab 5: Brand Sentiment + Response Engine**.

## Demo modes

### Mode A: Current repo-friendly demo

Use the local multi-stream publisher already included in the repo:

```bash
uv run lab5_datagen
```

This publishes all three governed source streams:

- `product_release_events`
- `support_cases`
- `brand_mentions`

That makes the repo-friendly mode a real multi-stream Flink demo rather than a single-topic simulation.

### Mode B: Stronger hackathon demo

Use connectors for:

- social/news ingestion into `brand_mentions`
- support-case ingestion into `support_cases`
- release/deployment ingestion into `product_release_events`
- sink connector or webhook subscriber for `brand_response_actions`

This is the version that best aligns to the judging criteria.

## Use case walkthrough

### 1. Observe the public signal

After running `uv run lab5_datagen`, confirm that all three sources are live:

```sql
SELECT * FROM brand_mentions;
SELECT * FROM support_cases;
SELECT * FROM product_release_events;
```

Then start by looking at the raw sentiment windows:

```sql
SELECT
    window_start,
    window_end,
    brand,
    product,
    region,
    COUNT(*) AS mention_count,
    ROUND(AVG(sentiment_score), 3) AS avg_sentiment,
    SUM(CASE WHEN sentiment_score < 0 THEN 1 ELSE 0 END) AS negative_mentions,
    SUM(CASE WHEN source_type = 'news' THEN 1 ELSE 0 END) AS news_mentions
FROM TABLE(
    TUMBLE(TABLE brand_mentions, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, brand, product, region;
```

This query alone is not enough for a competitive app, but it establishes the public-facing signal. The real Flink value comes from the next step, where we clean, join, and score multiple event streams into a single incident view.

### 2. Correlate public sentiment with support pressure and rollout context

This is the core stream-processing step that makes Lab 5 competitive. Instead of triggering on sentiment alone, Flink correlates public negative sentiment, support-case velocity, and nearby release events.

`brand_incident_alerts` is provisioned during `uv run deploy`, so this step writes into an already-governed output contract rather than creating an ad hoc runtime table.

The alerts contract is intentionally append-style. That keeps each incident window as an event record, which is compatible with downstream AI functions that require deterministic input streams.

```sql
INSERT INTO brand_incident_alerts
WITH mention_windows AS (
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
),
support_windows AS (
    SELECT
        window_time,
        brand,
        product,
        region,
        COUNT(*) AS support_case_count,
        SUM(CASE WHEN priority IN ('high', 'urgent') THEN 1 ELSE 0 END) AS urgent_support_cases
    FROM TABLE(
        TUMBLE(TABLE support_cases, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
    )
    GROUP BY window_start, window_end, window_time, brand, product, region
),
release_context AS (
    SELECT
        brand,
        product,
        region,
        release_id,
        release_type,
        release_version,
        event_ts AS release_ts
    FROM product_release_events
)
SELECT
    m.brand,
    m.product,
    m.region,
    m.window_time,
    m.mention_count,
    m.negative_mentions,
    m.news_mentions,
    m.social_mentions,
    m.avg_sentiment,
    COALESCE(s.support_case_count, 0) AS support_case_count,
    COALESCE(s.urgent_support_cases, 0) AS urgent_support_cases,
    r.release_id,
    r.release_type,
    r.release_version,
    CASE
        WHEN m.avg_sentiment <= -0.75 AND COALESCE(s.urgent_support_cases, 0) >= 3 THEN 'critical'
        WHEN m.avg_sentiment <= -0.55 AND COALESCE(s.support_case_count, 0) >= 3 THEN 'high'
        ELSE 'elevated'
    END AS severity,
    (
        ABS(m.avg_sentiment) * 100
        + (COALESCE(s.support_case_count, 0) * 5)
        + (m.news_mentions * 8)
        + CASE WHEN r.release_id IS NOT NULL THEN 15 ELSE 0 END
    ) AS incident_score,
    CONCAT(
        'Brand incident detected for ', m.brand, ' / ', m.product,
        ' in ', m.region,
        '. Average sentiment: ', CAST(m.avg_sentiment AS STRING),
        ', mentions: ', CAST(m.mention_count AS STRING),
        ', support cases: ', CAST(COALESCE(s.support_case_count, 0) AS STRING),
        CASE
            WHEN r.release_id IS NOT NULL THEN CONCAT(', possible trigger: ', r.release_type, ' ', r.release_version)
            ELSE ', no recent release event matched'
        END
    ) AS incident_summary
FROM mention_windows m
LEFT JOIN support_windows s
    ON m.brand = s.brand
   AND m.product = s.product
   AND m.region = s.region
   AND m.window_time = s.window_time
LEFT JOIN release_context r
    ON m.brand = r.brand
   AND m.product = r.product
   AND m.region = r.region
   AND r.release_ts BETWEEN m.window_time - INTERVAL '30' MINUTE AND m.window_time
WHERE m.mention_count >= 5
  AND m.avg_sentiment <= -0.35;
```

This is the centerpiece of the improved design. It proves real stream processing and materially improves business usefulness over a one-stream threshold query.

It is also the strongest part of the “most Flink-driven app” story because it uses Flink SQL for:

- event-time windowing
- stream correlation across three topics
- transformation of raw events into an operational incident model
- scoring and severity derivation inside the stream processor

### 3. Create the AI incident-response agent

The AI layer should no longer just draft generic PR copy. It should produce a response action for the correct team based on severity, likely cause, and blast radius.

```sql
CREATE AGENT `brand_incident_commander`
USING MODEL `llm_textgen_model`
USING PROMPT 'OUTPUT RULES:
1. Respond using exactly these five labeled sections, in this order:
Incident Type:
Recommended Owner:
Customer Impact:
Draft Response:
Escalation Rationale:
2. Plain text only. No markdown outside the labels.
3. Draft Response must be 2-4 sentences suitable for an internal PR, support, or incident-ops team.
4. If the incident appears tied to a release event, explicitly say so.
5. Recommend one owner team only: PR, Support, Product Engineering, or Marketing.

You are a real-time brand incident commander.

Use the incident summary, severity, support pressure, public signal mix, and release context to decide:
- whether the issue is a PR event, support crisis, or product-quality incident
- which team should own first response
- how urgent the issue is
- what short response draft should be reviewed immediately

critical means likely executive visibility and cross-functional escalation.
high means urgent coordinated response.
elevated means active monitoring with one accountable owner.

Be concise, factual, and operational.'
WITH (
  'max_iterations' = '6'
);
```

### 4. Emit governed response actions

The final output should be an action event, not just free-form text. That is what makes the app operational and connector-friendly.

`brand_response_actions` is also provisioned during `uv run deploy`, so the agent output lands in a stable, governed contract that downstream sinks can rely on.

That output contract is also append-style so each generated action remains a durable event for sinks and responders instead of an upserted row.

```sql
INSERT INTO brand_response_actions
SELECT
    brand,
    product,
    region,
    window_time,
    severity,
    incident_score,
    support_case_count,
    urgent_support_cases,
    release_id,
    release_type,
    release_version,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Incident Type:\\*{0,2}\\s*([^\\n]+)', 1)) AS incident_type,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Recommended Owner:\\*{0,2}\\s*([^\\n]+)', 1)) AS recommended_owner,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Customer Impact:\\*{0,2}\\s*\\n([\\s\\S]+?)(?=\\n\\*{0,2}Draft Response:)', 1)) AS customer_impact,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Draft Response:\\*{0,2}\\s*\\n([\\s\\S]+?)(?=\\n\\*{0,2}Escalation Rationale:)', 1)) AS draft_response,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\\*{0,2}Escalation Rationale:\\*{0,2}\\s*\\n([\\s\\S]+)$', 1)) AS escalation_rationale,
    CAST(response AS STRING) AS raw_response
FROM brand_incident_alerts,
LATERAL TABLE(AI_RUN_AGENT(
    `brand_incident_commander`,
    CONCAT(
        'INCIDENT SUMMARY: ', incident_summary, '\n',
        'Brand: ', brand, '\n',
        'Product: ', product, '\n',
        'Region: ', region, '\n',
        'Severity: ', severity, '\n',
        'Incident Score: ', CAST(incident_score AS STRING), '\n',
        'Support Cases: ', CAST(support_case_count AS STRING), '\n',
        'Urgent Support Cases: ', CAST(urgent_support_cases AS STRING), '\n',
        'Release ID: ', COALESCE(release_id, 'none'), '\n',
        'Release Type: ', COALESCE(release_type, 'none'), '\n',
        'Release Version: ', COALESCE(release_version, 'none')
    ),
    CONCAT(brand, '-', product, '-', region, '-', CAST(window_time AS STRING))
));
```

That action stream can be consumed by sink connectors or downstream services to open Jira tickets, send Slack alerts, create support playbooks, or trigger incident workflows.

### 5. What to demo to judges

The strongest short demo sequence is:

1. Show social and news negativity in `brand_mentions`
2. Show support load rising in `support_cases`
3. Show a nearby firmware rollout in `product_release_events`
4. Show Flink produce `brand_incident_alerts`
5. Show AI produce `brand_response_actions`
6. Show the action stream routed to a downstream sink or consumer

That hits all four judging dimensions cleanly.

## Why this version is more competitive

- **Most impactful app:** it helps prevent PR crises and speeds incident response
- **AI business impact:** AI is used to route and draft operational responses, not just summarize text
- **Connectors:** the design explicitly relies on multiple inbound connectors and at least one outbound action sink
- **Stream processing:** multi-stream windowing, joins, scoring, and routing are central to the app
- **Stream governance:** Schema Registry covers source, derived, and action topics with evolution-safe contracts
- **Flink AI bonus points:** the design uses Streaming Agents directly in Flink SQL for response generation

## Recommended next implementation step

The repo now includes local sample publishers for all three inbound streams. The best next move for a stronger hackathon submission is to add:

1. an outbound sink or webhook consumer for `brand_response_actions`
2. an optional `ML_DETECT_ANOMALIES` or similar Flink-native scoring layer on top of `incident_score`
3. one or more real connectors in place of the simulated source publisher

That would shift Lab 5 from a strong prototype into a more compelling end-to-end competition demo.

## Clean-up

```bash
uv run destroy
```

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Previous Lab**: [Lab4: Public Sector Insurance Claims Fraud Detection](./LAB4-Walkthrough.md)
