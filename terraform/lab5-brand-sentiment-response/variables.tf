variable "enable_testing_sql" {
  description = "Whether to execute testing-only SQL statements"
  type        = bool
  default     = false
}

variable "response_webhook_url" {
  description = "HTTP endpoint that receives brand_response_actions events via the HTTP Sink connector. Set to any reachable URL (e.g. a Slack incoming webhook or https://webhook.site/<id>). Leave empty to skip connector creation."
  type        = string
  default     = ""
}
