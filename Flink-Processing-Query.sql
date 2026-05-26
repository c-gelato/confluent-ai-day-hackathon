-- ============================================================
-- Lab 5: Brand Incident Command Center — Flink SQL reference
-- Run these three statements in the Confluent Cloud Flink SQL
-- workspace in the order listed below.
-- ============================================================


-- 1. Register the AI brand incident response agent
--    (run once; idempotent if agent already exists)
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


-- 2. Correlate public sentiment, support pressure, and release context
--    into the governed brand_incident_alerts topic
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
        window_start,
        window_end,
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
            WHEN r.release_id IS NOT NULL
                THEN CONCAT(', possible trigger: ', r.release_type, ' ', r.release_version)
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


-- 3. Run the AI agent on each alert and emit structured, governed response actions
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
    TRIM(SUBSTRING(raw_response FROM POSITION('Incident Type:' IN raw_response) + CHAR_LENGTH('Incident Type:') FOR POSITION('Recommended Owner:' IN raw_response) - POSITION('Incident Type:' IN raw_response) - CHAR_LENGTH('Incident Type:'))) AS incident_type,
    TRIM(SUBSTRING(raw_response FROM POSITION('Recommended Owner:' IN raw_response) + CHAR_LENGTH('Recommended Owner:') FOR POSITION('Customer Impact:' IN raw_response) - POSITION('Recommended Owner:' IN raw_response) - CHAR_LENGTH('Recommended Owner:'))) AS recommended_owner,
    TRIM(SUBSTRING(raw_response FROM POSITION('Customer Impact:' IN raw_response) + CHAR_LENGTH('Customer Impact:') FOR POSITION('Draft Response:' IN raw_response) - POSITION('Customer Impact:' IN raw_response) - CHAR_LENGTH('Customer Impact:'))) AS customer_impact,
    TRIM(SUBSTRING(raw_response FROM POSITION('Draft Response:' IN raw_response) + CHAR_LENGTH('Draft Response:') FOR POSITION('Escalation Rationale:' IN raw_response) - POSITION('Draft Response:' IN raw_response) - CHAR_LENGTH('Draft Response:'))) AS draft_response,
    TRIM(SUBSTRING(raw_response FROM POSITION('Escalation Rationale:' IN raw_response) + CHAR_LENGTH('Escalation Rationale:'))) AS escalation_rationale,
    raw_response
FROM (
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
    ))
);
