# gz-csv-bulk-load

Loads large gzipped CSV drops into Parquet. Step Functions fans out one Glue
Spark run per table; each run reads that table's files in parallel and writes a
single dated partition. Every file's header is checked against a contract in
`schemas.py` before any compute starts, so a vendor adding or dropping a column
fails the run in seconds rather than corrupting a table.

Three tables, roughly twenty objects, the largest around 6 GB compressed.

## Gzip decides the sizing, and nothing else does

Spark cannot split a gzip member. Each `.gz` object is read by exactly one task
on one core, start to finish, so:

- A table's useful cluster width is its **file count**. Cores beyond that idle.
- A table's wall clock is bounded below by its **largest single file**, no matter
  how many workers you add.
- `inferSchema` is disqualified. Inference is a second pass, and against an
  unsplittable file it doubles the single-threaded time.
- Autoscaling cannot help, so it is off.

The highest-leverage change available is not in this repo: **ask the vendor for
more, smaller files.** Two hundred objects instead of twenty makes the load
several times faster for the same money, and it is a parameter on their unload.

A different codec is not the answer. Zstd is not splittable either, and bzip2 is
splittable but decompresses so slowly it gives the gain back.

## Layout

```text
schemas.py          the tables, their columns, and the CSV dialect
load_table.py       the Glue job. one table per run
check_headers.py    the Lambda. validates every file's header
infra/              Terraform. hand-written, no generation from schemas.py
tests/              runs locally, no AWS
```

## How a run works

```text
Preflight (Lambda, seconds)
  ├─ list each table's objects
  ├─ range-GET 64 KB of each, inflate, take the first line
  ├─ compare to schemas.py — every file, not one per table
  └─ return the table list and one load_date for the whole run
        │
        ▼
Map  (MaxConcurrency = max_concurrent_loads)
  ├─ Glue run --table orders    --load_date 2026-09-02
  ├─ Glue run --table customers --load_date 2026-09-02
  └─ Glue run --table shipments --load_date 2026-09-02
        │
        ▼   inside one run
  check the catalog table exists and matches  ← fails in seconds if not
  read every .gz  (1 file = 1 task = 1 core, no shuffle)
  write  load_date=<date>/  replacing only that partition
  ALTER TABLE ADD IF NOT EXISTS PARTITION
  count rows that did not parse, fail if any
```

Only 64 KB of each object is fetched by the check. Gzip is a stream, so the
leading block inflates without the rest of the file — a 6 GB object and a 6 MB
object cost the same.

Any table failing fails the whole execution. A partial load reporting success is
worse than a red run.

## Adding a table

Two edits, in this order:

1. A `Table` in `schemas.py`, appended to `TABLES`.
2. An `aws_glue_catalog_table` resource in `infra/catalog.tf`, with the same
   columns plus `_corrupt_record`, partitioned by `load_date`.

Then `terraform apply`. The fan-out reads `TABLES` at runtime, so its width
follows on its own.

The column list appearing in both files is deliberate, and nothing generates one
from the other. They answer different questions — Terraform describes the target
table, `schemas.py` describes the header the vendor must send — and they diverge
once a column is renamed on the way in or given a real type. `check_catalog`
compares them on every single run, which catches drift faster than a generator
would and needs no build step.

## Why the contract is in `schemas.py` and not the Glue Catalog

The catalog cannot be the thing you validate against.

- It does not exist until infrastructure is deployed, so a new environment has
  nothing to compare a header to.
- It is mutable from the console and by crawlers, so a contract stored there can
  be changed without anyone reviewing it.
- Reading it needs AWS credentials, which puts the check out of reach of CI and
  of a laptop.

A contract that detects vendor drift has to be reviewable. When the vendor adds a
column you want a pull request, not someone quietly editing a catalog entry until
the error stops.

The catalog is written from `schemas.py` and compared against it. It is never
read to decide whether a load is valid.

## Everything loads as a string

The requirement is to fail on new or missing columns, which is a name check.
Types add nothing to it, and strings remove a class of silent loss — `N/A`,
`(500)`, a leading zero and a thousands separator all survive as text rather than
becoming null under a cast.

Typing belongs in whatever reads these tables, where a bad value can be
quarantined instead of failing a read that has already run for twenty minutes.

To add types later, give `Table.columns` `name: type` pairs and build a DDL
string in `read_schema`. One function changes.

## The job does not create its table

`check_catalog` fails if the table is absent. It does not create it.

A missing table is an infrastructure fault. Creating it here would turn a wrong
`--database` into a run that silently succeeds against a table nobody reads —
and every run after it would succeed too, while the real table went stale with
nothing showing red.

`register_partition` does use `ADD IF NOT EXISTS`, and that is a different case.
A partition already existing is expected, because it is what a retry of the same
run looks like.

## Deploying

```bash
cd infra
terraform init \
  -backend-config="bucket=<state-bucket>" \
  -backend-config="key=gz-csv-bulk-load/terraform.tfstate" \
  -backend-config="region=<region>"

cp terraform.tfvars.example terraform.tfvars   # then edit it
terraform apply
```

Terraform uploads `load_table.py` and `schemas.py` to the artifact bucket and
zips the Lambda, so a code change is deployed by `terraform apply` and nothing
else.

## Running

```bash
aws stepfunctions start-execution --state-machine-arn "$(terraform -chdir=infra output -raw state_machine_arn)"
```

To validate a drop without loading it, invoke the check alone:

```bash
aws lambda invoke --function-name "$(terraform -chdir=infra output -raw preflight_function_name)" /dev/stdout
```

## Tests

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest
```

The Spark tests need a JDK 17 on the path, matching what Glue 5.0 runs. They
prove the reader options against real gzipped files, which is worth doing
locally — `escape`, `enforceSchema` and `mode` all fail silently when they are
wrong, and a deployed run is a slow place to find that out.

## What this deliberately does not do

Each of these was considered and left out. Putting one back should be a decision,
not a drive-by.

- **No Iceberg.** Full replace per load. Dated partitions give the atomicity
  actually needed here, and `partitionOverwriteMode=dynamic` means a failed run
  leaves the previous load intact.
- **No crawler.** `schemas.py` is the contract, and inference is exactly the
  behaviour that has to fail loudly instead.
- **No DynamicFrames.** `create_dynamic_frame` resolves schemas by inference,
  same problem, and it is slower than a plain DataFrame.
- **No job bookmarks.** The preflight check decides what a run reads, and it says
  so in the log.
- **No shared library.** This repo depends on nothing of its own. Twelve
  duplicated lines beat a dependency someone has to go and read.
- **No YAML.** The tables are Python, so a typo is an import error in an editor
  with autocomplete rather than a `KeyError` inside a Glue job.
- **No per-table cluster sizing.** One shape, sized for the largest table. The
  waste on the smaller ones is well under a dollar per run, and that is cheaper
  than a configuration system.
