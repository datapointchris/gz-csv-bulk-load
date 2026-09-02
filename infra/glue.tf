resource "aws_s3_object" "load_table_script" {
  bucket = var.artifact_bucket
  key    = "${var.name_prefix}/load_table.py"
  source = "${path.module}/../load_table.py"
  etag   = filemd5("${path.module}/../load_table.py")
  tags   = var.tags
}

# schemas.py rides along as an extra module rather than being pasted into the job
# script. The preflight Lambda packages the same file, so the header contract and
# the read schema cannot disagree.
resource "aws_s3_object" "schemas_module" {
  bucket = var.artifact_bucket
  key    = "${var.name_prefix}/schemas.py"
  source = "${path.module}/../schemas.py"
  etag   = filemd5("${path.module}/../schemas.py")
  tags   = var.tags
}

resource "aws_glue_job" "load_table" {
  name         = "${var.name_prefix}-load-table"
  description  = "Load one table's gzipped CSV objects into a dated Parquet partition."
  role_arn     = aws_iam_role.glue.arn
  glue_version = "5.0"

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_workers
  timeout           = var.glue_timeout_minutes

  # A failed load is not retried automatically. The failure is nearly always the
  # vendor's data or a missing table, and neither is fixed by running it again.
  max_retries = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.artifact_bucket}/${aws_s3_object.load_table_script.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = var.max_concurrent_loads
  }

  default_arguments = {
    "--extra-py-files" = "s3://${var.artifact_bucket}/${aws_s3_object.schemas_module.key}"

    # Buckets and the database name change between deployments. The table shapes
    # do not, which is why they live in schemas.py and these do not.
    "--source_bucket" = var.landing_bucket
    "--target_path"   = local.lake_path
    "--database"      = aws_glue_catalog_database.raw.name

    # Makes spark.sql resolve against the Glue Catalog, so the job can register
    # its own partition with ALTER TABLE.
    "--enable-glue-datacatalog" = "true"

    # A bookmark is a hidden store that decides what a run reads. The preflight
    # check decides that here, and it says so out loud.
    "--job-bookmark-option" = "job-bookmark-disable"

    # Autoscaling cannot help. One gzipped file is one task on one core from
    # start to finish, so the useful width is fixed by the file count before the
    # job begins and extra executors would sit idle.
    "--enable-auto-scaling" = "false"

    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }

  tags = var.tags
}
