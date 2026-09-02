output "state_machine_arn" {
  description = "Start a load with: aws stepfunctions start-execution --state-machine-arn <this>"
  value       = aws_sfn_state_machine.load.arn
}

output "glue_job_name" {
  description = "Glue job the fan-out invokes, one run per table."
  value       = aws_glue_job.load_table.name
}

output "preflight_function_name" {
  description = "Header check. Invoke it alone to validate a drop without loading it."
  value       = aws_lambda_function.preflight.function_name
}

output "database_name" {
  description = "Glue Catalog database holding the loaded tables."
  value       = aws_glue_catalog_database.raw.name
}

output "table_locations" {
  description = "Where each table's Parquet lives."
  value = {
    orders    = aws_glue_catalog_table.orders.storage_descriptor[0].location
    customers = aws_glue_catalog_table.customers.storage_descriptor[0].location
    shipments = aws_glue_catalog_table.shipments.storage_descriptor[0].location
  }
}
