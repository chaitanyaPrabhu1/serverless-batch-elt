# --------------------------------------------------------------------------- #
# Guardrails: a billing alarm + Lambda error alarms, wired to email via SNS.    #
# Billing metrics live only in us-east-1, so this alarm assumes the default     #
# region or a us-east-1 deployment. Skipped entirely if alarm_email is empty.   #
# --------------------------------------------------------------------------- #
resource "aws_sns_topic" "alerts" {
  count = var.alarm_email == "" ? 0 : 1
  name  = "${var.project}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "billing" {
  count               = var.alarm_email == "" ? 0 : 1
  alarm_name          = "${var.project}-estimated-charges"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = 21600 # 6h
  statistic           = "Maximum"
  threshold           = var.monthly_cost_alarm_usd
  alarm_description   = "Estimated AWS charges exceeded $${var.monthly_cost_alarm_usd}."
  dimensions          = { Currency = "USD" }
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
}

resource "aws_cloudwatch_metric_alarm" "ingest_errors" {
  count               = var.alarm_email == "" ? 0 : 1
  alarm_name          = "${var.project}-ingest-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = aws_lambda_function.ingest.function_name }
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
}

resource "aws_cloudwatch_metric_alarm" "transform_errors" {
  count               = var.alarm_email == "" ? 0 : 1
  alarm_name          = "${var.project}-transform-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = aws_lambda_function.transform.function_name }
  alarm_actions       = [aws_sns_topic.alerts[0].arn]
}
