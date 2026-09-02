data "archive_file" "preflight" {
  type        = "zip"
  output_path = "${path.module}/.build/preflight.zip"

  source {
    content  = file("${path.module}/../check_headers.py")
    filename = "check_headers.py"
  }

  source {
    content  = file("${path.module}/../schemas.py")
    filename = "schemas.py"
  }
}

resource "aws_lambda_function" "preflight" {
  function_name    = "${var.name_prefix}-preflight"
  description      = "Check every vendor file's header against schemas.py before any Glue cost."
  role             = aws_iam_role.preflight.arn
  handler          = "check_headers.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.preflight.output_path
  source_code_hash = data.archive_file.preflight.output_base64sha256

  # Twenty ranged GETs against S3. The objects themselves are never downloaded.
  timeout     = 120
  memory_size = 256

  environment {
    variables = {
      LANDING_BUCKET = var.landing_bucket

      # These feed the sizing note the check logs per table: file count against
      # the cores the cluster will actually offer.
      GLUE_WORKER_TYPE = var.glue_worker_type
      GLUE_WORKERS     = var.glue_workers
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "preflight" {
  name              = "/aws/lambda/${aws_lambda_function.preflight.function_name}"
  retention_in_days = 30
  tags              = var.tags
}
