# --------------------------------------------------------------------------- #
# EventBridge schedule -> ingest Lambda. ingest then chains the transform       #
# Lambda asynchronously (CHAIN_TRANSFORM_LAMBDA), so the serverless path lands   #
# raw + clean data without Airflow. The Glue crawler runs on its own schedule.  #
# --------------------------------------------------------------------------- #
resource "aws_cloudwatch_event_rule" "hourly" {
  name                = "${var.project}-hourly"
  description         = "Trigger the weather ingest Lambda on a schedule."
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "ingest" {
  rule      = aws_cloudwatch_event_rule.hourly.name
  target_id = "ingest-lambda"
  arn       = aws_lambda_function.ingest.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hourly.arn
}

# Let the ingest Lambda invoke the transform Lambda (the chaining call).
data "aws_iam_policy_document" "lambda_invoke" {
  statement {
    sid       = "InvokeTransform"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.transform.arn]
  }
}

resource "aws_iam_role_policy" "lambda_invoke" {
  name   = "${var.project}-lambda-invoke"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_invoke.json
}

# Wire the chaining target into the ingest function's environment.
resource "aws_lambda_function_event_invoke_config" "ingest" {
  function_name          = aws_lambda_function.ingest.function_name
  maximum_retry_attempts = 1
}

# Refresh the catalog shortly after each ingest hour.
resource "aws_glue_trigger" "crawl_schedule" {
  name     = "${var.project}-crawl-schedule"
  type     = "SCHEDULED"
  schedule = "cron(20 * * * ? *)" # 20 min past every hour, after data lands
  enabled  = true

  actions {
    crawler_name = aws_glue_crawler.weather.name
  }
}
