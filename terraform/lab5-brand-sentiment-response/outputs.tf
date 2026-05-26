output "brand_mentions_table_statement_id" {
  description = "Flink statement ID for brand_mentions table"
  value       = confluent_flink_statement.brand_mentions_table.id
}

output "brand_mentions_table_name" {
  description = "Name of the brand mentions table"
  value       = "brand_mentions"
}

output "brand_incident_alerts_table_statement_id" {
  description = "Flink statement ID for brand_incident_alerts table"
  value       = confluent_flink_statement.brand_incident_alerts_table.id
}

output "brand_response_actions_table_statement_id" {
  description = "Flink statement ID for brand_response_actions table"
  value       = confluent_flink_statement.brand_response_actions_table.id
}

output "lab5_deployment_status" {
  description = "Lab5 deployment status"
  value       = "Lab5 brand sentiment response infrastructure deployed successfully"
}

output "brand_response_http_sink_connector_id" {
  description = "HTTP Sink connector ID for brand_response_actions (empty string if response_webhook_url was not set)"
  value       = length(confluent_connector.brand_response_http_sink) > 0 ? confluent_connector.brand_response_http_sink[0].id : ""
}
