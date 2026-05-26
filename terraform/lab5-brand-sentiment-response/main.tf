data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = "../core/terraform.tfstate"
  }
}

locals {
  cloud_provider = data.terraform_remote_state.core.outputs.cloud_provider
  cloud_region   = data.terraform_remote_state.core.outputs.cloud_region
}

data "confluent_organization" "main" {}

data "confluent_flink_region" "lab5_flink_region" {
  cloud  = upper(local.cloud_provider)
  region = local.cloud_region
}

resource "confluent_flink_statement" "brand_mentions_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab5_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "brand-mentions-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `brand_mentions` (
      `mention_id` STRING NOT NULL,
      `brand` STRING NOT NULL,
      `product` STRING NOT NULL,
      `region` STRING NOT NULL,
      `source_type` STRING NOT NULL,
      `channel` STRING NOT NULL,
      `author_handle` STRING,
      `headline` STRING,
      `body` STRING NOT NULL,
      `url` STRING,
      `priority_hint` STRING,
      `sentiment_score` DOUBLE NOT NULL,
      `event_ts` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
      WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

resource "confluent_flink_statement" "support_cases_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab5_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "support-cases-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `support_cases` (
      `case_id` STRING NOT NULL,
      `brand` STRING NOT NULL,
      `product` STRING NOT NULL,
      `region` STRING NOT NULL,
      `customer_tier` STRING,
      `case_channel` STRING,
      `priority` STRING,
      `issue_category` STRING,
      `issue_summary` STRING,
      `event_ts` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
      WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

resource "confluent_flink_statement" "product_release_events_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab5_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "product-release-events-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `product_release_events` (
      `release_id` STRING NOT NULL,
      `brand` STRING NOT NULL,
      `product` STRING NOT NULL,
      `region` STRING NOT NULL,
      `release_type` STRING NOT NULL,
      `release_version` STRING,
      `rollout_percent` INT,
      `initiated_by` STRING,
      `change_summary` STRING,
      `event_ts` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
      WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    data.terraform_remote_state.core
  ]
}

resource "confluent_flink_statement" "brand_incident_alerts_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab5_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "brand-incident-alerts-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `brand_incident_alerts` (
      `brand` STRING NOT NULL,
      `product` STRING NOT NULL,
      `region` STRING NOT NULL,
      `window_time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
      `mention_count` BIGINT,
      `negative_mentions` BIGINT,
      `news_mentions` BIGINT,
      `social_mentions` BIGINT,
      `avg_sentiment` DOUBLE,
      `support_case_count` BIGINT,
      `urgent_support_cases` BIGINT,
      `release_id` STRING,
      `release_type` STRING,
      `release_version` STRING,
      `severity` STRING,
      `incident_score` DOUBLE,
      `incident_summary` STRING
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.brand_mentions_table,
    confluent_flink_statement.support_cases_table,
    confluent_flink_statement.product_release_events_table,
  ]
}

resource "confluent_flink_statement" "brand_response_actions_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.lab5_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "brand-response-actions-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `brand_response_actions` (
      `brand` STRING NOT NULL,
      `product` STRING NOT NULL,
      `region` STRING NOT NULL,
      `window_time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
      `severity` STRING,
      `incident_score` DOUBLE,
      `support_case_count` BIGINT,
      `urgent_support_cases` BIGINT,
      `release_id` STRING,
      `release_type` STRING,
      `release_version` STRING,
      `incident_type` STRING,
      `recommended_owner` STRING,
      `customer_impact` STRING,
      `draft_response` STRING,
      `escalation_rationale` STRING,
      `raw_response` STRING
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.brand_incident_alerts_table,
  ]
}
