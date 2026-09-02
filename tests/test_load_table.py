"""The reader options and the catalog gate, proven locally.

The Spark tests read real gzipped files through the same `read_csv` the job uses,
because the options that decide whether this loads correctly — `escape`,
`enforceSchema`, `mode` — all fail silently when they are wrong. A deployed run
is a slow place to discover that.
"""

import pytest

import load_table
import schemas

TABLE = schemas.Table(
    name='widgets',
    prefix='vendor/widgets/',
    pattern='widgets_csv*.gz',
    columns=('widget_id', 'name', 'price'),
)

HEADER = 'widget_id,name,price'


class EntityNotFound(Exception):
    pass


class FakeGlue:
    """Stands in for a Glue client, carrying the one exception `check_catalog` catches."""

    class exceptions:
        EntityNotFoundException = EntityNotFound

    def __init__(self, table=None):
        self._table = table

    def get_table(self, DatabaseName, Name):  # noqa: N803 — boto3's own casing
        if self._table is None:
            raise EntityNotFound
        return {'Table': self._table}


def catalog_table(columns, partitions=('load_date',)):
    return {
        'StorageDescriptor': {'Columns': [{'Name': name} for name in columns]},
        'PartitionKeys': [{'Name': name} for name in partitions],
    }


# ---------------------------------------------------------------- the catalog gate


def test_missing_table_is_an_error_not_a_create():
    """Creating it here would turn a wrong --database into a run that silently succeeds."""
    with pytest.raises(RuntimeError, match='does not exist'):
        load_table.check_catalog(FakeGlue(), 'raw', TABLE)


def test_matching_catalog_passes():
    live = catalog_table(TABLE.catalog_columns)
    load_table.check_catalog(FakeGlue(live), 'raw', TABLE)


def test_drifted_columns_are_rejected():
    live = catalog_table((*TABLE.columns, 'surprise', schemas.CORRUPT_RECORD_COLUMN))
    with pytest.raises(ValueError, match='does not match schemas.py'):
        load_table.check_catalog(FakeGlue(live), 'raw', TABLE)


def test_reordered_columns_are_rejected():
    reordered = ('name', 'widget_id', 'price', schemas.CORRUPT_RECORD_COLUMN)
    with pytest.raises(ValueError, match='does not match schemas.py'):
        load_table.check_catalog(FakeGlue(catalog_table(reordered)), 'raw', TABLE)


def test_wrong_partition_key_is_rejected():
    live = catalog_table(TABLE.catalog_columns, partitions=('dt',))
    with pytest.raises(ValueError, match='partitioned by'):
        load_table.check_catalog(FakeGlue(live), 'raw', TABLE)


# ---------------------------------------------------------------- the schema


def test_every_column_is_a_nullable_string():
    schema = load_table.read_schema(TABLE)
    assert [field.name for field in schema.fields] == list(TABLE.catalog_columns)
    assert all(field.dataType.simpleString() == 'string' for field in schema.fields)
    assert all(field.nullable for field in schema.fields)


# ---------------------------------------------------------------- the read


def test_reads_several_gzipped_files_as_one_frame(spark, gz_csv, tmp_path):
    gz_csv('widgets_csv000.gz', [HEADER, 'w1,bolt,1.50'])
    gz_csv('widgets_csv001.gz', [HEADER, 'w2,nut,0.75'])

    frame = load_table.read_csv(spark, TABLE, str(tmp_path / TABLE.pattern))

    assert frame.columns == list(TABLE.catalog_columns)
    assert sorted(row.widget_id for row in frame.collect()) == ['w1', 'w2']


def test_values_stay_verbatim_as_strings(spark, gz_csv, tmp_path):
    """No inference means a leading zero, a currency string and a blank all survive."""
    gz_csv('widgets_csv000.gz', [HEADER, '007,"washer, large",N/A'])

    row = load_table.read_csv(spark, TABLE, str(tmp_path / TABLE.pattern)).collect()[0]

    assert row.widget_id == '007'
    assert row.name == 'washer, large'
    assert row.price == 'N/A'


def test_doubled_quote_is_an_escape(spark, gz_csv, tmp_path):
    """Spark's default escape is a backslash, which would shift every later column."""
    gz_csv('widgets_csv000.gz', [HEADER, 'w1,"3"" bolt",1.50'])

    row = load_table.read_csv(spark, TABLE, str(tmp_path / TABLE.pattern)).collect()[0]

    assert row.name == '3" bolt'
    assert row.price == '1.50'


def test_a_ragged_row_lands_in_the_corrupt_column(spark, gz_csv, tmp_path):
    """A row with the wrong field count would otherwise be written as all-null."""
    gz_csv('widgets_csv000.gz', [HEADER, 'w1,bolt,1.50', 'w2,nut,0.75,extra'])

    frame = load_table.read_csv(spark, TABLE, str(tmp_path / TABLE.pattern))
    corrupt = [row for row in frame.collect() if row[schemas.CORRUPT_RECORD_COLUMN] is not None]

    assert len(corrupt) == 1
    assert 'extra' in corrupt[0][schemas.CORRUPT_RECORD_COLUMN]


def test_a_wrong_header_raises_at_read_time(spark, gz_csv, tmp_path):
    """The backstop for a file landing between the preflight check and the read."""
    gz_csv('widgets_csv000.gz', ['widget_id,name,cost', 'w1,bolt,1.50'])

    with pytest.raises(Exception, match='(?i)header'):
        load_table.read_csv(spark, TABLE, str(tmp_path / TABLE.pattern)).collect()
