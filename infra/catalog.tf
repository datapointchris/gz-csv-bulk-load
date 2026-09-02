# The tables exist because they were deployed, not because a job created them.
# The load job calls GetTable and fails if one is missing, so a dropped table or a
# wrong --database surfaces as a red run in seconds instead of as a silent load
# into a table nobody reads.
#
# The column lists here also appear in schemas.py. That duplication is deliberate
# and nothing generates one from the other. The two answer different questions —
# this describes the target table, including the corrupt-record column and the
# partition key, while schemas.py describes the header the vendor must send. They
# diverge properly once a column is renamed on the way in or given a real type.
# load_table.check_catalog compares them on every single run, which is a stronger
# guarantee than a generator and needs no build step.

resource "aws_glue_catalog_database" "raw" {
  name        = var.database_name
  description = "Vendor tables as delivered, one partition per load."
}

resource "aws_glue_catalog_table" "orders" {
  name          = "orders"
  database_name = aws_glue_catalog_database.raw.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  partition_keys {
    name = "load_date"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.lake_path}/orders"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "order_date"
      type = "string"
    }
    columns {
      name = "status"
      type = "string"
    }
    columns {
      name = "total_amount"
      type = "string"
    }
    columns {
      name    = "_corrupt_record"
      type    = "string"
      comment = "Raw text of a row that did not parse into the declared column count. Empty on a healthy load."
    }
  }
}

resource "aws_glue_catalog_table" "customers" {
  name          = "customers"
  database_name = aws_glue_catalog_database.raw.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  partition_keys {
    name = "load_date"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.lake_path}/customers"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "full_name"
      type = "string"
    }
    columns {
      name = "email"
      type = "string"
    }
    columns {
      name = "postal_code"
      type = "string"
    }
    columns {
      name = "signup_date"
      type = "string"
    }
    columns {
      name    = "_corrupt_record"
      type    = "string"
      comment = "Raw text of a row that did not parse into the declared column count. Empty on a healthy load."
    }
  }
}

resource "aws_glue_catalog_table" "shipments" {
  name          = "shipments"
  database_name = aws_glue_catalog_database.raw.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  partition_keys {
    name = "load_date"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.lake_path}/shipments"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    columns {
      name = "shipment_id"
      type = "string"
    }
    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "carrier"
      type = "string"
    }
    columns {
      name = "shipped_at"
      type = "string"
    }
    columns {
      name = "delivered_at"
      type = "string"
    }
    columns {
      name    = "_corrupt_record"
      type    = "string"
      comment = "Raw text of a row that did not parse into the declared column count. Empty on a healthy load."
    }
  }
}
