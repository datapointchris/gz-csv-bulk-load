"""Validate every vendor file's header before any compute starts.

Reads the first `schemas.HEADER_BYTES` of each object and inflates it. Gzip is a
stream, so the leading DEFLATE block decompresses without the rest of the file —
checking a 6 GB object costs one ranged GET.

Every file is checked, not one per table. The failure that reaches the warehouse
otherwise is a vendor changing the header partway through a drop, where the first
file is fine and the fourth is not.

The whole report is assembled before anything is raised, so a run with three
broken tables is fixed in one pass rather than three.

Returns the table names for the Step Functions Map to fan out over, and the
`load_date` every table in the run writes into.
"""

import fnmatch
import logging
import os
import zlib
from datetime import UTC
from datetime import datetime

import boto3

import schemas

log = logging.getLogger()
log.setLevel(logging.INFO)

GZIP_WINDOW_BITS = 47
"""`zlib` needs 32 added to the window size to accept a gzip wrapper rather than a
bare DEFLATE stream. 15 is the maximum window; 15 + 32 is the documented idiom."""

CORES_PER_WORKER = {'G.1X': 4, 'G.2X': 8, 'G.4X': 16, 'G.8X': 32}
"""Used only to warn about sizing. One .gz file is one Spark task on one core for
its whole life, so a table's useful width is its file count and nothing more."""


# ---------------------------------------------------------------- pure


def first_line(compressed: bytes) -> bytes:
    """Inflate the head of a gzip stream and return its first line.

    Args:
        compressed: The leading bytes of a gzip object.

    Returns:
        The first line, without its terminator.

    Raises:
        ValueError: If no newline appears in what inflated. The header is then
            wider than the window, and `schemas.HEADER_BYTES` needs raising.
    """
    inflated = zlib.decompressobj(wbits=GZIP_WINDOW_BITS).decompress(compressed)
    newline = inflated.find(b'\n')
    if newline == -1:
        raise ValueError(f'No newline in the first {len(compressed)} bytes once inflated. Raise schemas.HEADER_BYTES.')
    return inflated[:newline].rstrip(b'\r')


def parse_header_line(line: bytes, encoding: str, delimiter: str, quote: str) -> tuple[str, ...]:
    """Split a header line into column names.

    A byte-order mark decodes into the first column name and would fail the
    comparison as `﻿order_id` rather than as a real mismatch, so it is
    stripped here.

    Args:
        line: The raw header line.
        encoding: Text encoding the vendor writes.
        delimiter: Field separator.
        quote: Quote character, stripped from each field when present.

    Returns:
        Column names, in file order.
    """
    text = line.decode(encoding).lstrip('﻿')
    return tuple(field.strip().strip(quote) for field in text.split(delimiter))


def describe_mismatch(expected: tuple[str, ...], actual: tuple[str, ...]) -> str | None:
    """Explain how a file's header differs from the contract.

    Args:
        expected: Column names from `schemas`.
        actual: Column names read from the file.

    Returns:
        A message a person can act on, or None when the header matches exactly.
    """
    if expected == actual:
        return None

    missing = [column for column in expected if column not in actual]
    extra = [column for column in actual if column not in expected]

    if missing or extra:
        lines = []
        if missing:
            lines.append(f'missing: {", ".join(missing)}')
        if extra:
            lines.append(f'extra:   {", ".join(extra)}')
        return '\n'.join(lines)

    # Same names, different order. Report the first position that disagrees —
    # a positional load would put every subsequent column in the wrong place.
    for position, (want, got) in enumerate(zip(expected, actual, strict=True), start=1):
        if want != got:
            return f'reordered: column {position} is {got!r}, the contract says {want!r}'

    return None


def sizing_note(table_name: str, file_count: int, worker_type: str, workers: int) -> str:
    """Compare a table's file count to the cores the cluster will offer.

    Args:
        table_name: Table the note is about.
        file_count: Objects matched for this table.
        worker_type: Glue worker type the job is configured with.
        workers: Total workers, one of which is the driver.

    Returns:
        A one-line note for the log.
    """
    cores = (workers - 1) * CORES_PER_WORKER.get(worker_type, 0)
    if cores == 0:
        return f'{table_name}: {file_count} files, cluster size unknown'
    if file_count < cores:
        return f'{table_name}: {file_count} files, {cores} cores available — {cores - file_count} will idle'
    if file_count > cores:
        return f'{table_name}: {file_count} files, {cores} cores available — files will queue'
    return f'{table_name}: {file_count} files, {cores} cores available — matched'


# ---------------------------------------------------------------- impure


def list_table_objects(s3, bucket: str, table: schemas.Table) -> list[str]:
    """List every object belonging to one table.

    Args:
        s3: An S3 client.
        bucket: Landing bucket.
        table: The table to list.

    Returns:
        Matching keys, sorted.
    """
    keys: list[str] = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=table.prefix):
        keys.extend(item['Key'] for item in page.get('Contents', ()) if fnmatch.fnmatch(item['Key'], table.source_glob))
    return sorted(keys)


def read_header(s3, bucket: str, key: str) -> tuple[str, ...]:
    """Read one object's header without downloading it.

    Args:
        s3: An S3 client.
        bucket: Landing bucket.
        key: Object key.

    Returns:
        Column names, in file order.
    """
    head = s3.get_object(Bucket=bucket, Key=key, Range=f'bytes=0-{schemas.HEADER_BYTES - 1}')['Body'].read()
    return parse_header_line(
        first_line(head),
        encoding=schemas.CSV_OPTIONS['encoding'],
        delimiter=schemas.CSV_OPTIONS['sep'],
        quote=schemas.CSV_OPTIONS['quote'],
    )


def check_table(s3, bucket: str, table: schemas.Table) -> tuple[int, list[str]]:
    """Check every file belonging to one table.

    Args:
        s3: An S3 client.
        bucket: Landing bucket.
        table: The table to check.

    Returns:
        The file count, and one problem string per bad file. A table matching no
        objects is itself a problem — a load of nothing that reports success is
        indistinguishable from a working run.
    """
    keys = list_table_objects(s3, bucket, table)
    if not keys:
        return 0, [f'  no objects matched s3://{bucket}/{table.source_glob}']

    problems = []
    for key in keys:
        mismatch = describe_mismatch(table.columns, read_header(s3, bucket, key))
        if mismatch is not None:
            indented = mismatch.replace('\n', '\n      ')
            problems.append(f'  {key}\n      {indented}')
    return len(keys), problems


def handler(event: dict, context: object) -> dict:
    """Check every table, then hand the fan-out its item list.

    Args:
        event: Unused. The tables come from `schemas`, not from the caller, so a
            malformed invocation cannot narrow what gets checked.
        context: Unused Lambda context.

    Returns:
        The table names for the Map, and the `load_date` every table writes into.
        One date for the whole run means a retry replaces the partition it wrote
        the first time.

    Raises:
        ValueError: If any file's header disagrees with `schemas`, or if any table
            matched no objects.
    """
    bucket = os.environ['LANDING_BUCKET']
    worker_type = os.environ.get('GLUE_WORKER_TYPE', '')
    workers = int(os.environ.get('GLUE_WORKERS', '0'))
    s3 = boto3.client('s3')

    report = []
    for table in schemas.TABLES.values():
        file_count, problems = check_table(s3, bucket, table)
        log.info(sizing_note(table.name, file_count, worker_type, workers))
        if problems:
            report.append(f'{table.name} — edit schemas.{table.name.upper()}.columns\n' + '\n'.join(problems))

    if report:
        raise ValueError(f'Header check failed for {len(report)} of {len(schemas.TABLES)} tables.\n\n' + '\n\n'.join(report) + '\n')

    return {
        'tables': list(schemas.TABLES),
        'load_date': datetime.now(UTC).date().isoformat(),
    }
