"""The header check is the only gate between a vendor change and the warehouse.

These tests cover inflating a header, parsing it by CSV's grammar, comparing it
to the contract and sizing the cluster. `handler` is then driven over a stub S3
client, to show one run checks every file and reports every bad table. Which
listed keys a table's check takes in is decided by `spark_reads`, and the Spark
tests in test_load_table.py compare that against what a real read takes in.
"""

import gzip
import io

import pytest

import check_headers
import schemas

DIALECT = {
    'encoding': schemas.CSV_OPTIONS['encoding'],
    'delimiter': schemas.CSV_OPTIONS['sep'],
    'quote': schemas.CSV_OPTIONS['quote'],
}


def gzipped(text: str) -> bytes:
    return gzip.compress(text.encode())


def test_first_line_reads_a_header_without_the_rest_of_the_file():
    head = gzipped('a,b,c\n1,2,3\n4,5,6\n')
    assert check_headers.first_line(head) == b'a,b,c'


def test_first_line_strips_a_carriage_return():
    """A vendor writing CRLF would otherwise leave \\r on the last column name."""
    assert check_headers.first_line(gzipped('a,b,c\r\n1,2,3\r\n')) == b'a,b,c'


def test_first_line_raises_when_no_newline_arrives():
    with pytest.raises(ValueError, match='HEADER_BYTES'):
        check_headers.first_line(gzipped('a,b,c'))


def test_parse_header_line_splits_on_the_delimiter():
    assert check_headers.parse_header_line(b'a,b,c', **DIALECT) == ('a', 'b', 'c')


def test_parse_header_line_strips_quotes_and_spaces():
    assert check_headers.parse_header_line(b'"a", "b" ,c', **DIALECT) == ('a', 'b', 'c')


def test_parse_header_line_keeps_a_quoted_delimiter_in_one_column():
    assert check_headers.parse_header_line(b'"a,x",b', **DIALECT) == ('a,x', 'b')


def test_parse_header_line_strips_a_byte_order_mark():
    """A BOM decodes into the first name and would read as a rename, not as an encoding fault."""
    assert check_headers.parse_header_line('﻿a,b,c'.encode(), **DIALECT) == ('a', 'b', 'c')


def test_matching_header_has_no_mismatch():
    assert check_headers.header_mismatch(('a', 'b'), ('a', 'b')) is None


def test_missing_column_is_named():
    assert check_headers.header_mismatch(('a', 'b', 'c'), ('a', 'c')) == check_headers.HeaderMismatch(missing=('b',))


def test_extra_column_is_named():
    assert check_headers.header_mismatch(('a', 'b'), ('a', 'b', 'z')) == check_headers.HeaderMismatch(extra=('z',))


def test_missing_and_extra_are_reported_together():
    """One pass should show everything wrong with a file, not the first thing."""
    mismatch = check_headers.header_mismatch(('a', 'b'), ('a', 'z'))
    assert mismatch == check_headers.HeaderMismatch(missing=('b',), extra=('z',))


def test_duplicated_column_is_named():
    assert check_headers.header_mismatch(('a', 'b'), ('a', 'b', 'b')) == check_headers.HeaderMismatch(duplicated=('b',))


def test_reordering_names_the_first_bad_position():
    mismatch = check_headers.header_mismatch(('a', 'b', 'c'), ('a', 'c', 'b'))
    assert mismatch == check_headers.HeaderMismatch(reordered=check_headers.Reorder(position=2, found='c', expected='b'))


def test_sizing_counts_idle_cores():
    sizing = check_headers.cluster_sizing(file_count=4, worker_type='G.2X', workers=3)
    assert (sizing.cores, sizing.idle, sizing.queued) == (16, 12, 0)


def test_sizing_counts_queued_files():
    sizing = check_headers.cluster_sizing(file_count=40, worker_type='G.1X', workers=3)
    assert (sizing.cores, sizing.idle, sizing.queued) == (8, 0, 32)


def test_sizing_survives_an_unknown_worker_type():
    sizing = check_headers.cluster_sizing(file_count=4, worker_type='G.99X', workers=3)
    assert (sizing.cores, sizing.idle, sizing.queued) == (0, 0, 0)


class StubS3:
    """The two calls `handler` makes on its S3 client: a listing and a ranged GET."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects
        self.read: list[str] = []

    def get_paginator(self, operation: str) -> 'StubS3':
        assert operation == 'list_objects_v2'
        return self

    def paginate(self, **params):
        yield {'Contents': [{'Key': key} for key in sorted(self.objects) if key.startswith(params['Prefix'])]}

    def get_object(self, **params) -> dict:
        self.read.append(params['Key'])
        return {'Body': io.BytesIO(self.objects[params['Key']])}


def test_handler_reports_every_bad_table_in_one_run(monkeypatch):
    """A bad fourth file and an empty table both fail the run, and the good files are checked too."""
    good_order = 'vendor/orders/orders_csv_001.gz'
    good_customer = 'vendor/customers/customers_csv_001.gz'
    bad_customer = 'vendor/customers/customers_csv_002.gz'
    customer_header = ','.join(schemas.CUSTOMERS.columns)
    s3 = StubS3(
        {
            good_order: gzipped(','.join(schemas.ORDERS.columns) + '\n1,2,3,4,5\n'),
            good_customer: gzipped(customer_header + '\n'),
            bad_customer: gzipped(customer_header + ',email\n'),
        }
    )
    monkeypatch.setattr(check_headers.boto3, 'client', lambda service: s3)
    monkeypatch.setenv('LANDING_BUCKET', 'landing')

    with pytest.raises(ValueError) as raised:
        check_headers.handler({}, None)

    report = str(raised.value)
    assert bad_customer in report
    assert schemas.SHIPMENTS.source_glob in report
    assert good_order not in report
    assert good_customer not in report
    assert sorted(s3.read) == sorted([good_order, good_customer, bad_customer])
