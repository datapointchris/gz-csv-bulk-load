# gz-csv-bulk-load

Loads large gzipped CSV drops into Parquet on AWS. Step Functions fans out one Glue Spark run per
table. Each run reads that table's files in parallel and writes a single dated partition. Every
file's header is checked against a contract in `schemas.py` before any compute starts. A vendor
adding or dropping a column fails the run in seconds rather than corrupting a table.

The stock AWS route is a crawler feeding a DynamicFrame job. Both infer the schema. This pipeline
declares it in code, and a file that disagrees fails the run. It checks column names and lands every
value as text. Typing, cleaning and quarantining bad values belong to whatever reads the tables.

## Install

The pipeline runs in AWS, and Terraform deploys it from `infra/`. The machine you deploy from needs
uv, Terraform at the floor `infra/versions.tf` sets, the AWS CLI with credentials for the target
account, and a JDK 17 for the Spark tests.

```bash
git clone https://github.com/datapointchris/gz-csv-bulk-load.git && cd gz-csv-bulk-load
uv sync --all-extras    # the dev tools into .venv, with pyspark at the version Glue runs
```

## Starting from scratch

Terraform creates no bucket. Four must exist before the first `apply`: one for Terraform's state,
the landing bucket the vendor drops into, the lake bucket the Parquet goes to, and the artifact
bucket the job is uploaded to.

```bash
uv run pytest                                  # proves the CSV reader options on real gzipped files, no AWS
cd infra
terraform init \
  -backend-config="bucket=<state-bucket>" \
  -backend-config="key=gz-csv-bulk-load/terraform.tfstate" \
  -backend-config="region=<region>"            # given here because they differ per deployment
cp terraform.tfvars.example terraform.tfvars   # then name the three buckets and size the cluster
terraform apply                                # uploads the job, zips the Lambda, creates the tables

aws lambda invoke /dev/stdout \
  --function-name "$(terraform output -raw preflight_function_name)"  # checks every header, loads nothing
arn=$(aws stepfunctions start-execution --query executionArn --output text \
  --state-machine-arn "$(terraform output -raw state_machine_arn)")   # the check again, then the load
aws stepfunctions describe-execution --execution-arn "$arn" \
  --query status --output text                                        # SUCCEEDED once every table loaded
```

`terraform.tfvars` is gitignored, so each deployment keeps its own. The example's comments carry the
sizing arithmetic for the cluster.

`terraform apply` is the whole deploy. It uploads `load_table.py` and `schemas.py` to the artifact
bucket and zips `check_headers.py` with `schemas.py` into the Lambda. A code change ships the same
way. There is no separate deploy step, and no `aws lambda update-function-code` path.

Invoking the check alone validates a drop without starting a cluster. A clean drop prints the table
list and the run's `load_date`. A bad one comes back as the function's error, listing every bad file
under its table.

## A run checks every header before any cluster starts

```text
Preflight  check_headers.py, a Lambda, seconds
  ├─ list each table's objects
  ├─ range-GET 64 KB of each, inflate, take the first line
  ├─ compare to schemas.py — every file, not one per table
  └─ return the table list and one load_date for the whole run
        │
        ▼
Map  infra/statemachine.asl.json, MaxConcurrency = max_concurrent_loads
  └─ one Glue run per table in schemas.TABLES:  --table <name> --load_date <date>
        │
        ▼
Glue run  load_table.py, one table
  check the catalog table exists and matches   ← fails in seconds if not
  read every .gz   (1 file = 1 task = 1 core, no shuffle)
  write load_date=<date>/, replacing only that partition
  ALTER TABLE ADD IF NOT EXISTS PARTITION
  count rows that did not parse, fail if any
```

The Lambda and the Glue job both carry the same `schemas.py`. The header contract and the read
schema therefore cannot disagree. `infra/` is hand-written Terraform, with nothing generated from
`schemas.py`. `tests/` runs locally with no AWS.

## Gzip decides the sizing, and nothing else does

Spark cannot split a gzip member. Each `.gz` object is read by exactly one task on one core, start
to finish, so:

- A table's useful cluster width is its **file count**. Cores beyond that idle.
- A table's wall clock is bounded below by its **largest single file**, no matter how many workers
  you add.
- `inferSchema` is disqualified. Inference is a second pass, and against an unsplittable file it
  doubles the single-threaded time.
- Autoscaling cannot help, so it is off.

Size `glue_workers` for the table with the most files. One worker is the driver, so
`(glue_workers - 1)` times the worker type's cores should reach that file count. The cores per
worker type are in the `glue_worker_type` description in `infra/variables.tf`. The preflight logs
one line per table saying how many cores will idle or that files will queue.

The highest-leverage change available is not in this repo: **ask the vendor for more, smaller
files.** Two hundred objects instead of twenty makes the load several times faster on the same
cluster. It is usually one setting on the vendor's export.

A different codec is not the answer. Zstd is not splittable either. Bzip2 is splittable but
decompresses so slowly it gives the gain back.

## `check_headers.py` — every file's header, for one ranged GET each

Every file is checked, not one per table. The failure that reaches the warehouse otherwise is a
vendor changing the header partway through a drop. The first file is fine and the fourth is not.

Only 64 KB of each object is fetched. Gzip is a stream, so the leading block inflates without the
rest of the file. A 6 GB object and a 6 MB object cost the same. A header wider than that window
fails with an error naming `schemas.HEADER_BYTES`, the setting to raise.

The whole report is assembled before anything raises. A run with three broken tables is fixed in one
pass rather than three. Each bad file is listed with its missing columns, its extra columns, or the
first position where the order disagrees. A table matching no objects fails too, because a load of
nothing that reports success looks exactly like a working run.

The check ignores the event it is invoked with. The tables come from `schemas.py`, so a malformed
invocation cannot narrow what gets checked. It returns one `load_date` for every table in the run,
so a retry replaces the partition the first attempt wrote.

## `load_table.py` — one table, streamed from gzip to Parquet with no shuffle

The job compares the catalog table to `schemas.py` before it builds a Spark session. A wrong
`--database` or a drifted table costs seconds rather than the length of the largest file.

The read options are in `schemas.CSV_OPTIONS`. Three of them fail silently when they are wrong, and
a Spark test proves each one:

- `escape` is `"`, because RFC 4180 doubles a quote to escape it. Spark's backslash default shifts
  every column after the first embedded quote and raises nothing.
- `enforceSchema` is `false`, so Spark compares each header to the schema and raises. The default
  maps columns by position, which loads a reordered file with every column mislabeled. This is the
  backstop for a file that lands between the preflight and the read.
- `mode` is `PERMISSIVE`. A row that will not parse goes to `_corrupt_record` with its raw text
  instead of abandoning a twenty-minute read.

`multiLine` stays `false`. Whole-file mode is far slower and holds a whole file in memory. Turn it
on only if the vendor embeds real newlines inside quoted fields.

The write uses dynamic partition overwrite, which replaces only the partition the frame holds.
Static overwrite deletes the whole table path first, so a failed run would leave an empty table.
`spark.sql.files.maxRecordsPerFile` rolls a task's output into several Parquet files. Without it,
one large input file becomes one multi-gigabyte object. Splitting it with `repartition` would add a
shuffle.

After the write, the job reads the partition back and counts the rows carrying `_corrupt_record`. A
count above zero fails the run. That partition stays written, so the bad rows can be read, and a
rerun after the fix replaces it.

## `schemas.py` is the contract, and the Glue Catalog never is

A header is validated against `schemas.py`. The catalog is written from it and compared against it
on every run. It is never read to decide whether a load is valid. Three things rule it out as the
contract:

- It does not exist until infrastructure is deployed, so a new environment has nothing to compare a
  header to.
- It is mutable from the console and by crawlers, so a contract stored there can be changed without
  anyone reviewing it.
- Reading it needs AWS credentials, which puts the check out of reach of CI and of a laptop.

A contract that detects vendor drift has to be reviewable. When the vendor adds a column you want a
pull request, not someone quietly editing a catalog entry until the error stops.

## Adding a table is two edits, and the fan-out follows

```bash
uv run python -c 'import schemas; print(*schemas.TABLES)'   # the tables a run loads
```

The two edits go in this order:

1. A `Table` in `schemas.py`, appended to `TABLES`.
2. An `aws_glue_catalog_table` resource in `infra/catalog.tf`, with the same columns plus
   `_corrupt_record`, partitioned by `load_date`.

Then `terraform apply`. The preflight returns every name in `TABLES`, and the Map fans out over that
list at runtime. `max_concurrent_loads` caps how many tables load at once, so a table past the cap
waits for a slot. The `table_locations` output in `infra/outputs.tf` names each table by hand, so a
new table is missing from it until it is added there.

The column list appearing in both files is deliberate, and nothing generates one from the other.
They answer different questions. Terraform describes the target table. `schemas.py` describes the
header the vendor must send. The two diverge once a column is renamed on the way in or given a real
type. The job compares them on every run, which catches drift faster than a generator would and
needs no build step.

## Everything loads as a string

The requirement is to fail on new or missing columns, which is a name check. Types add nothing to
it. Strings remove a class of silent loss. `N/A`, `(500)`, a leading zero and a thousands separator
all survive as text, where a cast would null them or strip the zero.

Typing belongs in whatever reads these tables. There a bad value can be quarantined instead of
failing a read that has already run for twenty minutes.

Types would not be a one-function change. `Table.columns` feeds the header check, the catalog
comparison and the read schema. `infra/catalog.tf` declares every column as `string`.

## Any table failing fails the run, and nothing retries a load

A partial load reporting success is worse than a red run. The Map catches nothing, so one table
failing fails the execution.

The Glue job never retries itself. The failure is nearly always the vendor's data or a missing
table, and running it again fixes neither. Step Functions retries a Glue run only when the job's
concurrent-run cap rejects it, because that is a scheduling collision rather than a data fault.
`max_concurrent_loads` sets both that cap and the Map's concurrency, so the two cannot drift apart.

The job does not create its table. It fails if the table is absent. A missing table is an
infrastructure fault. Creating it here would turn a wrong `--database` into a run that silently
succeeds against a table nobody reads. Every run after it would succeed too, while the real table
went stale with nothing showing red.

Registering the partition does use `ADD IF NOT EXISTS`, and that is a different case. A partition
already existing is expected, because it is what a retry of the same run looks like.

## Tests run without AWS, and the Spark ones need a JDK 17

`awsglue` exists only inside a running Glue job. `tests/conftest.py` registers a stub module before
`load_table` is imported, so everything below that import is testable on a laptop.

The tests using the `spark` fixture need a JDK 17 on the path, matching what Glue 5.0 runs. Without
one they error with `JAVA_GATEWAY_EXITED` rather than failing an assertion, and the rest pass. CI
provides the JDK.

Those Spark tests prove the reader options against real gzipped files. `escape`, `enforceSchema` and
`mode` all fail silently when they are wrong. A misconfigured reader returns a DataFrame rather than
raising, and a deployed run is a slow place to find that out.

## What this deliberately does not do

Each of these was considered and left out. Putting one back should be a decision, not a drive-by.

- **No Iceberg.** Every load is a full replace, written to its own dated partition. That gives the
  atomicity needed here, and dynamic partition overwrite means a failed run leaves the previous load
  intact.
- **No crawler.** `schemas.py` is the contract, and inference is exactly the behavior that has to
  fail loudly instead.
- **No DynamicFrames.** `create_dynamic_frame` resolves schemas by inference, the same problem, and
  it is slower than a plain DataFrame.
- **No job bookmarks.** A bookmark is a hidden store that decides what a run reads. Here every run
  reads every object matching each table's prefix and pattern, and the preflight logs how many
  files that is.
- **No shared library.** This repo has no runtime dependency at all. Lambda and Glue both ship
  boto3, and Glue ships PySpark, so anything added would have to be packaged into both. Copied lines
  beat a dependency someone has to go and read.
- **No YAML.** The tables are Python, so a typo is an import error in an editor with autocomplete
  rather than a `KeyError` inside a Glue job.
- **No per-table cluster sizing.** One shape, sized for the table with the most files. The waste on
  the smaller ones is well under a dollar per run, which is cheaper than a configuration system.
