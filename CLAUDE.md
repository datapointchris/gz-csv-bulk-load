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

Adding a table is two edits, in this order: a `Table` appended to `TABLES` in `schemas.py`, then an
`aws_glue_catalog_table` in `infra/catalog.tf` with the same columns plus `_corrupt_record`,
partitioned by `load_date`. The Step Functions fan-out reads `TABLES` at runtime, so nothing else
needs widening.

## The column list is duplicated on purpose

`schemas.py` and `infra/catalog.tf` both name every column, and nothing generates one from the
other. They answer different questions. Terraform describes the target table; `schemas.py` describes
the header the vendor must send. The two diverge the moment a column is renamed on the way in.

`check_catalog` compares them on every run, so drift fails a load rather than corrupting a table.
Do not add a generator to remove the duplication — the comparison catches more than a build step
would, and needs no build step.

## The contract is never read from the catalog

`schemas.py` is the thing a header is validated against. The Glue Catalog is written from it and
compared to it, never read to decide whether a load is valid. The catalog does not exist in a fresh
environment, it is mutable from the console and by crawlers, and reading it needs credentials that
CI and a laptop do not have.

## Everything loads as a string

The requirement is to fail on new or missing columns, which is a name check. Types add nothing to
it and remove a class of silent loss: `N/A`, `(500)`, a leading zero and a thousands separator all
survive as text but not as a cast. Typing belongs in whatever reads these tables.

## Tests

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest
```

`awsglue` exists only inside a running Glue job, so `tests/conftest.py` registers a stub module
before `load_table` is imported. Anything importing `load_table` outside that fixture set will fail
on that line.

The tests using the `spark` fixture need a JDK 17 on the path, matching what Glue 5.0 runs. Without
one they error with `JAVA_GATEWAY_EXITED` rather than failing an assertion. CI provides a JDK; a
machine without one will see those tests error and the rest pass.

**Those Spark tests are the point, not ceremony.** `escape`, `enforceSchema` and `mode` all fail
silently when they are wrong — a misconfigured reader returns a DataFrame rather than raising, so
the damage shows up as bad data much later. The tests prove the reader options against real gzipped
files, which is far cheaper than finding out inside a deployed run.

## Deploying

Terraform uploads `load_table.py` and `schemas.py` to the artifact bucket and zips the Lambda, so a
code change ships with `terraform apply` and nothing else. There is no separate deploy step, and no
`aws lambda update-function-code` path to reach for.

Backend config is supplied at init time; the bucket, key and region are in the README's "Deploying"
section. `infra/terraform.tfvars` is gitignored — copy `infra/terraform.tfvars.example` and edit it.

## Failure posture

Any table failing fails the whole execution. A partial load reporting success is worse than a red
run, so do not add per-table error swallowing to keep an execution green.

`check_catalog` fails when a table is absent and does not create it. A missing table is an
infrastructure fault, and creating it here would turn a wrong `--database` into a run that
succeeds against a table nobody reads. `register_partition` does use `ADD IF NOT EXISTS`, which is
a different case — a partition already existing is what a retry looks like.
