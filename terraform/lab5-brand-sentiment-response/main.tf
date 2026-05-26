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
