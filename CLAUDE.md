# gz-csv-bulk-load

The README carries the design and the reasoning behind it. Read it before changing anything —
in particular "Gzip decides the sizing, and nothing else does", which explains why the cluster is
sized the way it is, and "What this deliberately does not do", which lists what was considered
and left out. Putting one of those back is a decision, not a drive-by.

## Where a change goes

```text
schemas.py          the tables, their columns, and the CSV dialect
load_table.py       the Glue job. one table per run
check_headers.py    the Lambda. validates every file's header before compute starts
infra/              Terraform. hand-written, no generation from schemas.py
tests/              runs locally, no AWS
```

Adding a table follows the README's "Adding a table is two edits, and the fan-out follows". That
section also names the hand-written `table_locations` output in `infra/outputs.tf`, which a new
table needs an entry in.

## Every AWS call has one door

Anything that leaves the process goes through one of these functions. A new call goes inside the
door for its service, never beside it.

- **The landing bucket, from the Lambda.** `check_headers.handler` creates the only S3 client and
  passes it down. `list_table_objects` lists a table's keys, and `read_header` makes the one ranged
  GET per object.
- **The Glue Catalog, read.** `load_table.main` creates the only Glue client and passes it to
  `check_catalog`, which makes the one `get_table` call.
- **The Glue Catalog, written.** `register_partition` runs the job's only `spark.sql`, the
  `ALTER TABLE ... ADD IF NOT EXISTS PARTITION`.
- **S3 through Spark.** `read_csv` reads the vendor files, `write_partition` writes the Parquet, and
  `count_corrupt` reads that partition back.

No other function creates a boto3 client or runs `spark.sql`. A new external effect gets one
wrapper of its own, named here.

## Tests

```bash
task setup    # once
task test     # pytest, then terraform test in infra/
```

The README's "Tests run without AWS, and the Spark ones need a JDK 17" covers the JDK and what the
Spark tests prove. `tests/conftest.py` registers the `awsglue` stub before `load_table` is
imported, so anything importing `load_table` outside those fixtures fails on that line.

## Decisions a change must not undo in passing

Each is argued in the README section named beside it.

- No generator between `schemas.py` and `infra/catalog.tf`. `check_catalog` compares the two on
  every run. See "Adding a table is two edits, and the fan-out follows".
- Never read the Glue Catalog to decide whether a load is valid. See "`schemas.py` is the contract,
  and the Glue Catalog never is".
- No types on the columns. See "Everything loads as a string".
- No per-table error swallowing, and `check_catalog` never creates a missing table. See "Any table
  failing fails the run, and nothing retries a load".
- No deploy path besides `terraform apply`. See "Starting from scratch".
