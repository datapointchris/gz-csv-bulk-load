variable "aws_region" {
  description = "Region everything is deployed into."
  type        = string
}

variable "name_prefix" {
  description = "Prefix for every resource name, so several deployments can share an account."
  type        = string
  default     = "gz-csv-load"
}

variable "landing_bucket" {
  description = "Bucket the vendor drops gzipped CSV into. Read-only to this pipeline."
  type        = string
}

variable "lake_bucket" {
  description = "Bucket the Parquet output is written to."
  type        = string
}

variable "lake_prefix" {
  description = "Key prefix under lake_bucket holding the loaded tables."
  type        = string
  default     = "raw"
}

variable "artifact_bucket" {
  description = "Bucket the Glue script and its shared modules are uploaded to."
  type        = string
}

variable "database_name" {
  description = "Glue Catalog database holding the loaded tables."
  type        = string
  default     = "raw"
}

variable "glue_worker_type" {
  description = <<-EOT
    Glue worker type. G.1X is 4 cores per worker, G.2X is 8, G.4X is 16, G.8X is 32.
    One gzipped file is one Spark task on one core for its whole life, so cores beyond
    the largest table's file count are idle.
  EOT
  type        = string
  default     = "G.2X"
}

variable "glue_workers" {
  description = <<-EOT
    Total workers, one of which is the driver. Size for the table with the most files:
    (glue_workers - 1) * cores_per_worker should be at least that count. One size serves
    every table — the waste on the smaller ones is well under a dollar per run.
  EOT
  type        = number
  default     = 3
}

variable "glue_timeout_minutes" {
  description = "Hard stop for a single table's load, so a stuck job cannot bill overnight."
  type        = number
  default     = 60
}

variable "max_concurrent_loads" {
  description = <<-EOT
    Tables loaded at once. Caps the Step Functions Map and the Glue job's concurrent runs
    together — if the Map were wider than the job allows, the extra iterations would fail
    with ConcurrentRunsExceededException. Must be at least the number of tables in schemas.py.
  EOT
  type        = number
  default     = 4
}

variable "tags" {
  description = "Tags applied to every resource that accepts them."
  type        = map(string)
  default     = {}
}
