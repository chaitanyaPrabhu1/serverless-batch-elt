# --------------------------------------------------------------------------- #
# Two Lambdas (ingest + transform) built from the same source zip.              #
# pyarrow/pandas are supplied by the AWS-managed pandas layer (var).            #
#                                                                               #
# The deployment package is assembled by `make package` into                   #
# ../build/package/ (lambda *.py + cities.json at the root). Run `make package` #
# before `terraform apply` (or just use `make apply`, which does both).         #
# --------------------------------------------------------------------------- #
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../build/package"
  output_path = "${path.module}/build/lambda.zip"
}

locals {
  lambda_env = {
    DATA_BUCKET       = aws_s3_bucket.lake.bucket
    RAW_PREFIX        = "raw/weather"
    CLEAN_PREFIX      = "clean/weather"
    QUARANTINE_PREFIX = "quarantine/weather"
    CITIES_CONFIG     = "/var/task/cities.json"
    API_BASE_URL      = "https://api.open-meteo.com/v1/forecast"
  }
}

resource "aws_lambda_function" "ingest" {
  function_name    = "${var.project}-ingest"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.11"
  handler          = "ingest.handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60
  memory_size      = 256
  layers           = [var.pandas_layer_arn]
  environment {
    # ingest additionally knows how to chain the transform Lambda.
    variables = merge(local.lambda_env, {
      CHAIN_TRANSFORM_LAMBDA = "${var.project}-transform"
    })
  }
}

resource "aws_lambda_function" "transform" {
  function_name    = "${var.project}-transform"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.11"
  handler          = "transform.handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 120
  memory_size      = 512
  layers           = [var.pandas_layer_arn]
  environment { variables = local.lambda_env }
}

resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${aws_lambda_function.ingest.function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "transform" {
  name              = "/aws/lambda/${aws_lambda_function.transform.function_name}"
  retention_in_days = 14
}
