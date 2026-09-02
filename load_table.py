"""Load one table's gzipped CSV files into the lake.

Gzip is not splittable. Spark reads each `.gz` object with exactly one task on
one core, start to finish, so a table's useful cluster width is its file count
and its wall clock is bounded below by its largest single file. Nothing here
repartitions, sorts or joins — the read streams straight into the Parquet writer
and the job never shuffles.

Every column loads as a string. Type coercion belongs downstream, where a bad
value can be quarantined instead of failing a read that has already run for
twenty minutes. `inferSchema` is worse still: inference is a second pass over the
data, and against an unsplittable file that doubles the single-threaded time.

The catalog table is not created here. It is deployed by Terraform, and this job
fails if it is absent — see `check_catalog`.
"""

import logging
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as st

import schemas

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

MAX_RECORDS_PER_FILE = 10_000_000
"""Rolls a task's output into several Parquet files instead of one. A 22 GB CSV
otherwise becomes one multi-gigabyte object, because one input file is one task
and one task writes one file per partition. This rolls without a shuffle, which
`repartition` would not."""


def build_session(app: str) -> SparkSession:
    """Return the session, configured for a shuffle-free partition replace.

    Glue builds a session before this script runs, so this attaches settings to
    the existing one rather than creating a second.

    Args:
        app: Application name, surfaced in the Spark UI and the run listing.

    Returns:
        The active session.
    """
    spark = SparkSession.builder.appName(app).getOrCreate()
    # Static overwrite deletes the whole table path before writing, so a failed
    # run leaves an empty table and a reader mid-run sees a partial one. Dynamic
    # replaces only the partitions the frame actually contains.
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('spark.sql.files.maxRecordsPerFile', str(MAX_RECORDS_PER_FILE))
    return spark


def read_schema(table: schemas.Table) -> st.StructType:
    """Build the read schema for a table.

    Args:
        table: The table being loaded.

    Returns:
        Every vendor column as a nullable string, followed by the corrupt-record
        column. Spark requires that column to be present in the schema when
        `columnNameOfCorruptRecord` is set.
    """
    return st.StructType([st.StructField(name, st.StringType(), nullable=True) for name in table.catalog_columns])


def check_catalog(glue, database: str, table: schemas.Table) -> None:
    """Fail unless the catalog table exists and matches the contract.

    A missing table is an infrastructure fault, not a data fault, and creating it
    here would turn a wrong `--database` into a run that silently succeeds
    against a table nobody reads. Every run afterwards would also succeed, and
    the real table would go stale with nothing showing red.

    Args:
        glue: A Glue client.
        database: Catalog database holding the table.
        table: The table being loaded.

    Raises:
        RuntimeError: If the table does not exist.
        ValueError: If its columns or partition keys disagree with `schemas`.
    """
    try:
        live = glue.get_table(DatabaseName=database, Name=table.name)['Table']
    except glue.exceptions.EntityNotFoundException as err:
        raise RuntimeError(
            f'Catalog table {database}.{table.name} does not exist.\n'
            f'This job will not create it. Either --database is wrong, or the table '
            f'was dropped and that is an incident. Check the deployed infrastructure.'
        ) from err

    live_columns = tuple(column['Name'] for column in live['StorageDescriptor']['Columns'])
    if live_columns != table.catalog_columns:
        raise ValueError(
            f'Catalog table {database}.{table.name} does not match schemas.py.\n'
            f'  catalog:    {live_columns}\n'
            f'  schemas.py: {table.catalog_columns}\n'
            f'Something changed the table outside this pipeline.'
        )

    live_partitions = tuple(key['Name'] for key in live.get('PartitionKeys', ()))
    if live_partitions != (schemas.PARTITION_COLUMN,):
        raise ValueError(
            f'Catalog table {database}.{table.name} is partitioned by {live_partitions}, expected ({schemas.PARTITION_COLUMN!r},).'
        )


def read_csv(spark: SparkSession, table: schemas.Table, path: str) -> DataFrame:
    """Read one table's gzipped CSV objects.

    Args:
        spark: Active session.
        table: The table being loaded.
        path: Glob resolving to this table's objects.

    Returns:
        Every column as a string, plus the corrupt-record column.
    """
    log.info(f'reading {path}')
    return spark.read.schema(read_schema(table)).options(**schemas.CSV_OPTIONS).csv(path)


def write_partition(frame: DataFrame, location: str, load_date: str) -> None:
    """Write the frame into one date partition, replacing it if present.

    The partition column is a literal, so every task writes into the same
    directory and no shuffle is introduced.

    Args:
        frame: Rows to write.
        location: Table root in S3.
        load_date: Partition value for this run.
    """
    log.info(f'writing {location} partition {schemas.PARTITION_COLUMN}={load_date}')
    frame.withColumn(schemas.PARTITION_COLUMN, sf.lit(load_date)).write.mode('overwrite').partitionBy(schemas.PARTITION_COLUMN).parquet(
        location
    )


def register_partition(spark: SparkSession, database: str, table: schemas.Table, load_date: str) -> None:
    """Make the new partition visible to the catalog.

    `IF NOT EXISTS` is correct here and is not the pattern `check_catalog`
    rejects. A partition already existing is expected — it is what a retry of the
    same run looks like. A table not existing is not expected, which is why that
    case raises instead.

    Args:
        spark: Active session, wired to the Glue Catalog by `--enable-glue-datacatalog`.
        database: Catalog database holding the table.
        table: The table being loaded.
        load_date: Partition value for this run.
    """
    spark.sql(f"ALTER TABLE {database}.{table.name} ADD IF NOT EXISTS PARTITION ({schemas.PARTITION_COLUMN}='{load_date}')")


def count_corrupt(spark: SparkSession, location: str, load_date: str) -> int:
    """Count rows Spark could not parse, reading back what was just written.

    Counting before the write would mean a second pass over the CSV. Reading the
    Parquet back is one column of a columnar file, and the null count is in the
    footer.

    Args:
        spark: Active session.
        location: Table root in S3.
        load_date: Partition just written.

    Returns:
        Rows carrying raw text in the corrupt-record column.
    """
    written = spark.read.parquet(f'{location}/{schemas.PARTITION_COLUMN}={load_date}/')
    return written.where(sf.col(schemas.CORRUPT_RECORD_COLUMN).isNotNull()).count()


def main() -> None:
    """Load the table named by `--table`.

    Raises:
        ValueError: If `--table` is not in `schemas.TABLES`, or if any row failed
            to parse. A parse failure leaves the partition written and the run
            red — rerunning after a fix replaces the same partition.
    """
    args = getResolvedOptions(sys.argv, ['table', 'load_date', 'source_bucket', 'target_path', 'database'])

    if args['table'] not in schemas.TABLES:
        raise ValueError(f'Unknown table {args["table"]!r}. schemas.TABLES holds: {", ".join(schemas.TABLES)}')

    table = schemas.TABLES[args['table']]
    location = f'{args["target_path"].rstrip("/")}/{table.name}'

    # Before the session and before the read, so a misconfigured run costs
    # seconds rather than the length of the largest file.
    check_catalog(boto3.client('glue'), args['database'], table)

    spark = build_session(f'load-{table.name}')
    frame = read_csv(spark, table, f's3://{args["source_bucket"]}/{table.source_glob}')
    write_partition(frame, location, args['load_date'])
    register_partition(spark, args['database'], table, args['load_date'])

    corrupt = count_corrupt(spark, location, args['load_date'])
    if corrupt:
        raise ValueError(
            f'{corrupt} rows in {table.name} did not parse into {len(table.columns)} columns.\n'
            f'Their raw text is in {schemas.CORRUPT_RECORD_COLUMN} of partition '
            f'{schemas.PARTITION_COLUMN}={args["load_date"]}. Read a few, fix the cause, rerun.'
        )

    log.info(f'{table.name} loaded into {schemas.PARTITION_COLUMN}={args["load_date"]}')


if __name__ == '__main__':
    main()
