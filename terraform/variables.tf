variable "project" {
  description = "Name prefix for all resources."
  type        = string
  default     = "weather-elt"
}

variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "schedule_expression" {
  description = "EventBridge schedule for the ingest pipeline."
  type        = string
  default     = "rate(1 hour)"
}

variable "alarm_email" {
  description = "Email to receive the billing alarm (leave empty to skip the alarm)."
  type        = string
  default     = ""
}

variable "monthly_cost_alarm_usd" {
  description = "Estimated-charges threshold (USD) that triggers the billing alarm."
  type        = number
  default     = 5
}

variable "pandas_layer_arn" {
  description = <<-EOT
    ARN of the AWS-managed "AWSSDKPandas" Lambda layer for this region/runtime
    (provides pandas + pyarrow). Find the current ARN for python3.11 here:
    https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html
    Example (us-east-1):
    arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python311:13
  EOT
  type        = string
}

variable "raw_expiration_days" {
  description = "Days before raw-zone objects expire (keeps the lake cheap)."
  type        = number
  default     = 30
}
