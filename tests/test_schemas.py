"""The contract has to be well-formed before it can be enforced.

None of this needs AWS or Spark. A duplicate column or a mistyped key is a
copy-paste error, and copy-paste is exactly how a table gets added.
"""

import pytest

import schemas

TABLES = list(schemas.TABLES.values())
IDS = [table.name for table in TABLES]


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_key_matches_table_name(table):
    assert schemas.TABLES[table.name] is table


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_columns_are_unique(table):
    assert len(set(table.columns)) == len(table.columns)


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_columns_are_not_empty(table):
    assert table.columns


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_columns_are_stripped(table):
    """Whitespace here would never match a header the check strips."""
    assert all(column == column.strip() for column in table.columns)


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_reserved_columns_are_not_declared(table):
    """The pipeline adds both of these, so a vendor column of either name would collide."""
    assert schemas.PARTITION_COLUMN not in table.columns
    assert schemas.CORRUPT_RECORD_COLUMN not in table.columns


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_prefix_ends_with_separator(table):
    """`source_glob` concatenates prefix and pattern, so a missing slash silently widens it."""
    assert table.prefix.endswith('/')


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_source_glob_joins_prefix_and_pattern(table):
    assert table.source_glob == table.prefix + table.pattern


@pytest.mark.parametrize('table', TABLES, ids=IDS)
def test_catalog_columns_end_with_corrupt_record(table):
    assert table.catalog_columns == (*table.columns, schemas.CORRUPT_RECORD_COLUMN)


def test_corrupt_record_option_names_the_declared_column():
    """Spark needs the option and the schema to agree, or the column is never populated."""
    assert schemas.CSV_OPTIONS['columnNameOfCorruptRecord'] == schemas.CORRUPT_RECORD_COLUMN


def test_header_is_not_enforced_positionally():
    """`true` makes Spark ignore the header and map by position, mislabelling a reordered file."""
    assert schemas.CSV_OPTIONS['enforceSchema'] == 'false'
