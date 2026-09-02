data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name_prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue" {
  statement {
    sid       = "ReadVendorDrop"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.landing_bucket}/*"]
  }

  statement {
    sid       = "ListVendorDrop"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.landing_bucket}"]
  }

  statement {
    sid = "WriteLake"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["arn:aws:s3:::${var.lake_bucket}/${var.lake_prefix}/*"]
  }

  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.lake_bucket}"]
  }

  statement {
    sid       = "ReadJobArtifacts"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.artifact_bucket}/${var.name_prefix}/*"]
  }

  # No CreateTable and no DeleteTable. The tables are deployed by Terraform, and
  # a job that cannot create one cannot paper over a missing one either.
  statement {
    sid = "CatalogReadAndPartition"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${var.database_name}",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.database_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "${var.name_prefix}-glue"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}


# ---------------------------------------------------------------- preflight

data "aws_iam_policy_document" "preflight_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "preflight" {
  name               = "${var.name_prefix}-preflight"
  assume_role_policy = data.aws_iam_policy_document.preflight_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "preflight_basic" {
  role       = aws_iam_role.preflight.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "preflight" {
  statement {
    sid       = "ReadVendorHeaders"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.landing_bucket}/*"]
  }

  statement {
    sid       = "ListVendorDrop"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.landing_bucket}"]
  }
}

resource "aws_iam_role_policy" "preflight" {
  name   = "${var.name_prefix}-preflight"
  role   = aws_iam_role.preflight.id
  policy = data.aws_iam_policy_document.preflight.json
}


# ---------------------------------------------------------------- state machine

data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "states" {
  name               = "${var.name_prefix}-states"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "states" {
  statement {
    sid       = "InvokePreflight"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.preflight.arn]
  }

  # GetJobRun is what the .sync integration polls with, and BatchStopJobRun is
  # how an aborted execution stops the runs it started.
  statement {
    sid = "RunGlueJob"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.load_table.name}"]
  }

  statement {
    sid = "Logging"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "states" {
  name   = "${var.name_prefix}-states"
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states.json
}
