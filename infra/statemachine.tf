resource "aws_cloudwatch_log_group" "states" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}"
  retention_in_days = 30
  tags              = var.tags
}

resource "aws_sfn_state_machine" "load" {
  name     = "${var.name_prefix}-load"
  role_arn = aws_iam_role.states.arn

  definition = templatefile("${path.module}/statemachine.asl.json", {
    preflight_function_arn = aws_lambda_function.preflight.arn
    glue_job_name          = aws_glue_job.load_table.name

    # The Map's width and the job's concurrent-run cap come from one variable.
    # If the Map were the wider of the two, the extra iterations would be
    # rejected the moment they started.
    max_concurrent_loads = var.max_concurrent_loads
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.states.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tags = var.tags
}
