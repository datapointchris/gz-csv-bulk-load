# Runs under `terraform test` with no credentials and no state. It pins what the load job and
# the fan-out depend on: each catalog table's name, partition key and location, and the one
# variable that caps both the Map and the Glue job's concurrent runs.

# A mocked role's ARN is a generated string, and the Glue job and the Lambda refuse a role that
# is not an ARN when the mocked resources are applied.
mock_provider "aws" {
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::111111111111:role/mock" }
  }
}

variables {
  aws_region      = "us-east-1"
  landing_bucket  = "landing"
  lake_bucket     = "lake"
  artifact_bucket = "artifacts"
}

# A mocked aws_caller_identity returns a generated account id. Pinning it keeps any ARN built
# from it well-formed, so a failure is about the assertion rather than the mock.
override_data {
  target = data.aws_caller_identity.current
  values = { account_id = "111111111111" }
}

# A mocked policy document returns a generated string, and aws_iam_role and aws_iam_role_policy
# reject it with `contains an invalid JSON policy: not a JSON object`. The policies' contents are
# not what these assertions read.
override_data {
  target = data.aws_iam_policy_document.glue_assume
  values = { json = "{}" }
}

override_data {
  target = data.aws_iam_policy_document.glue
  values = { json = "{}" }
}

override_data {
  target = data.aws_iam_policy_document.preflight_assume
  values = { json = "{}" }
}

override_data {
  target = data.aws_iam_policy_document.preflight
  values = { json = "{}" }
}

override_data {
  target = data.aws_iam_policy_document.states_assume
  values = { json = "{}" }
}

override_data {
  target = data.aws_iam_policy_document.states
  values = { json = "{}" }
}

# The load job looks each table up by its schemas.py name and refuses any partitioning but
# load_date, so a rename or a new partition key here fails every run of that table.
run "catalog_tables_match_what_the_load_job_reads" {
  command = plan

  assert {
    condition = [
      aws_glue_catalog_table.orders.name,
      aws_glue_catalog_table.customers.name,
      aws_glue_catalog_table.shipments.name,
    ] == ["orders", "customers", "shipments"]
    error_message = "table names: ${aws_glue_catalog_table.orders.name}, ${aws_glue_catalog_table.customers.name}, ${aws_glue_catalog_table.shipments.name}"
  }

  assert {
    condition = alltrue([
      for table in [aws_glue_catalog_table.orders, aws_glue_catalog_table.customers, aws_glue_catalog_table.shipments] :
      [for key in table.partition_keys : key.name] == ["load_date"]
    ])
    error_message = "every table is partitioned by load_date alone"
  }

  # The job writes to <target_path>/<table>, and target_path is the lake path.
  assert {
    condition     = aws_glue_catalog_table.orders.storage_descriptor[0].location == "s3://lake/raw/orders"
    error_message = "orders location: ${aws_glue_catalog_table.orders.storage_descriptor[0].location}"
  }

  assert {
    condition     = aws_glue_job.load_table.default_arguments["--target_path"] == "s3://lake/raw"
    error_message = "job target path: ${aws_glue_job.load_table.default_arguments["--target_path"]}"
  }
}

# If the Map were wider than the job's cap, the extra iterations would be rejected with
# ConcurrentRunsExceededException. The definition embeds the Lambda's ARN, which is known only
# once the mocked resources are applied.
run "one_variable_caps_the_map_and_the_job" {
  command = apply

  variables {
    max_concurrent_loads = 7
  }

  assert {
    condition     = aws_glue_job.load_table.execution_property[0].max_concurrent_runs == 7
    error_message = "job cap: ${aws_glue_job.load_table.execution_property[0].max_concurrent_runs}"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.load.definition).States.LoadTables.MaxConcurrency == "{% 7 %}"
    error_message = "Map MaxConcurrency: ${jsondecode(aws_sfn_state_machine.load.definition).States.LoadTables.MaxConcurrency}"
  }
}
