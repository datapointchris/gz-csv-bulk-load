"""The tables this pipeline loads, and the CSV dialect they arrive in.

This module is the contract. A file's header must match its table's `columns`
exactly and in order, or the run fails before any compute starts.

The contract lives here rather than in the Glue Catalog for three reasons. The
catalog does not exist until infrastructure is deployed, so a new environment
would have nothing to check against. The catalog is mutable from the console and
by crawlers, so a contract stored there can be changed without review. And
reading it needs AWS credentials, which puts the check out of reach of CI and of
a laptop.

The catalog is written from this list and compared against it on every load. It
is never read to decide whether a load is valid.

Adding a table is a `Table` below plus an `aws_glue_catalog_table` resource in
`infra/catalog.tf`. Nothing else moves — the Step Functions fan-out reads
`TABLES` at runtime, so its width follows automatically.
"""

from dataclasses import dataclass

PARTITION_COLUMN = 'load_date'
"""Every load writes one partition, named for the date the run started. A retry
of the same run targets the same partition and replaces it, so a rerun is safe."""

CORRUPT_RECORD_COLUMN = '_corrupt_record'
"""Rows Spark could not parse into the declared column count land here with their
raw text. Without this column those rows would be written as all-null and would
be indistinguishable from real sparse rows. Empty on a healthy load."""


@dataclass(frozen=True)
class Table:
    """One vendor table, and the objects that carry it.

    Attributes:
        name: Catalog table name, and the `--table` argument the Glue job takes.
        prefix: Key prefix under the landing bucket, trailing slash included.
        pattern: Glob matching this table's objects beneath `prefix`.
        columns: The vendor's header, in order. Every column loads as a string —
            coercion belongs downstream, where a bad value can be quarantined
            instead of failing a read that has already run for twenty minutes.
    """

    name: str
    prefix: str
    pattern: str
    columns: tuple[str, ...]

    @property
    def source_glob(self) -> str:
        """Key glob for this table's objects, relative to the landing bucket.

        Returns:
            The prefix and pattern joined, so the preflight check and the Spark
            read resolve the same set of objects rather than building the path
            two ways.
        """
        return f'{self.prefix}{self.pattern}'

    @property
    def catalog_columns(self) -> tuple[str, ...]:
        """Data columns of the catalog table, in order.

        Returns:
            The vendor's columns plus the corrupt-record column. The partition
            column is not here — Glue holds partition keys separately from the
            storage descriptor's columns.
        """
        return (*self.columns, CORRUPT_RECORD_COLUMN)


# ---------------------------------------------------------------- the tables

ORDERS = Table(
    name='orders',
    prefix='vendor/orders/',
    pattern='orders_csv*.gz',
    columns=(
        'order_id',
        'customer_id',
        'order_date',
        'status',
        'total_amount',
    ),
)

CUSTOMERS = Table(
    name='customers',
    prefix='vendor/customers/',
    pattern='customers_csv*.gz',
    columns=(
        'customer_id',
        'full_name',
        'email',
        'postal_code',
        'signup_date',
    ),
)

SHIPMENTS = Table(
    name='shipments',
    prefix='vendor/shipments/',
    pattern='shipments_csv*.gz',
    columns=(
        'shipment_id',
        'order_id',
        'carrier',
        'shipped_at',
        'delivered_at',
    ),
)

TABLES = {table.name: table for table in (ORDERS, CUSTOMERS, SHIPMENTS)}


# ---------------------------------------------------------------- the dialect

CSV_OPTIONS = {
    'header': 'true',
    # With the default `true`, Spark ignores the header entirely and maps the
    # schema by position — a reordered file would be loaded with every column
    # silently mislabelled. `false` makes Spark compare the header to the schema
    # and raise. The preflight check is the primary gate; this is the backstop
    # for a file that lands between the check and the read.
    'enforceSchema': 'false',
    'sep': ',',
    'quote': '"',
    # RFC 4180 doubles a quote to escape it. Spark's default is a backslash, and
    # the mismatch shifts every column after the first embedded quote without
    # raising anything.
    'escape': '"',
    'encoding': 'UTF-8',
    # `true` switches the parser to whole-file mode, which is far slower and
    # holds a whole file in memory. Only turn it on if the vendor embeds real
    # newlines inside quoted fields.
    'multiLine': 'false',
    # FAILFAST would abandon a twenty-minute read on one bad row. PERMISSIVE
    # keeps going and routes the row to CORRUPT_RECORD_COLUMN, which the job
    # counts after the write and fails on.
    'mode': 'PERMISSIVE',
    'columnNameOfCorruptRecord': CORRUPT_RECORD_COLUMN,
}
"""One vendor, one dialect, so these apply to every table. If a table ever needs
different options, add a field to `Table` at that point rather than in advance."""

HEADER_BYTES = 65536
"""How much of each object the preflight check reads. Gzip is a stream, so the
leading DEFLATE block inflates without the rest of the file — this is one ranged
GET regardless of whether the object is 6 MB or 6 GB. It has to inflate to more
than one header line, which 64 KB does comfortably for any realistic width."""
