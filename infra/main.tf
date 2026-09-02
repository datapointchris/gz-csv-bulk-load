locals {
  lake_path = "s3://${var.lake_bucket}/${var.lake_prefix}"

  parquet_input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde         = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
}

data "aws_caller_identity" "current" {}
