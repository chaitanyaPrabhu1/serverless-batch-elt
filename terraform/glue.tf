# --------------------------------------------------------------------------- #
# Glue Data Catalog database + crawler over the clean zone, and an Athena       #
# workgroup that writes results back into the lake.                             #
# --------------------------------------------------------------------------- #
resource "aws_glue_catalog_database" "weather" {
  name = local.glue_database
}

resource "aws_glue_crawler" "weather" {
  name          = "${var.project}-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.weather.name

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/clean/weather/"
  }

  # Keep one stable table even as new dt= partitions appear.
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

resource "aws_athena_workgroup" "weather" {
  name          = local.glue_database
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
