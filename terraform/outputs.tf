output "data_bucket" {
  description = "S3 data-lake bucket. Export as DATA_BUCKET for dbt/Airflow."
  value       = aws_s3_bucket.lake.bucket
}

output "glue_database" {
  description = "Glue/Athena database. Export as GLUE_DATABASE."
  value       = aws_glue_catalog_database.weather.name
}

output "glue_crawler" {
  description = "Glue crawler name. Export as GLUE_CRAWLER."
  value       = aws_glue_crawler.weather.name
}

output "athena_workgroup" {
  description = "Athena workgroup. Export as ATHENA_WORKGROUP."
  value       = aws_athena_workgroup.weather.name
}

output "ingest_lambda" {
  description = "Ingest Lambda name. Export as INGEST_LAMBDA."
  value       = aws_lambda_function.ingest.function_name
}

output "transform_lambda" {
  description = "Transform Lambda name. Export as TRANSFORM_LAMBDA."
  value       = aws_lambda_function.transform.function_name
}

output "dbt_env_exports" {
  description = "Copy-paste block to configure dbt + Airflow."
  value       = <<-EOT
    export DATA_BUCKET=${aws_s3_bucket.lake.bucket}
    export AWS_REGION=${var.region}
    export GLUE_DATABASE=${aws_glue_catalog_database.weather.name}
    export ATHENA_WORKGROUP=${aws_athena_workgroup.weather.name}
    export INGEST_LAMBDA=${aws_lambda_function.ingest.function_name}
    export TRANSFORM_LAMBDA=${aws_lambda_function.transform.function_name}
    export GLUE_CRAWLER=${aws_glue_crawler.weather.name}
  EOT
}
